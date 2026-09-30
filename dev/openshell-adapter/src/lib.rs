//! One-shot command execution in an already-provisioned OpenShell sandbox.
//!
//! This backend consumes OpenShell's streamed RPC and bounds the amount of
//! stdout/stderr retained in memory. It stops reading when a collection limit
//! is exceeded; closing the stream does not prove the remote process stopped.
//!
//! OpenShell v0.1.2 reports a terminal exit code, but does not distinguish a
//! normal exit from its timeout exit code or expose a remote cancel operation.
//! Consequently, this adapter reports the exit code as observed and never
//! treats dropping this future or its gRPC stream as proof that the remote
//! process stopped. See `docs/openshell.md` for the backend contract.

use agent_supervisor::{
    Enforcement, EnforcementRequirement, ExecutableIdentity, ExecutionOutput, ExecutionRequest,
    NetworkMode, ProcessRunner, SandboxError, SandboxErrorCode, SandboxPhase, TerminationReason,
};
use futures::StreamExt;
use openshell_sdk::{raw, OpenShellClient};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Runs one-shot commands in an existing OpenShell sandbox.
///
/// The caller supplies a configured SDK client, workspace, and ready sandbox
/// name. This type does not create, delete, or provision the sandbox.
pub struct OpenShellRunner {
    client: OpenShellClient,
    workspace: String,
    sandbox: String,
}

impl OpenShellRunner {
    /// Connects request execution to an existing named sandbox.
    pub fn new(
        client: OpenShellClient,
        workspace: impl Into<String>,
        sandbox: impl Into<String>,
    ) -> Result<Self, SandboxError> {
        let workspace = workspace.into();
        let sandbox = sandbox.into();
        if workspace.is_empty() || workspace.contains('\0') {
            return Err(invalid_config(
                "OpenShell workspace must be non-empty and contain no NUL",
            ));
        }
        if sandbox.is_empty() || sandbox.contains('\0') {
            return Err(invalid_config(
                "OpenShell sandbox name must be non-empty and contain no NUL",
            ));
        }
        Ok(Self {
            client,
            workspace,
            sandbox,
        })
    }

    /// Workspace selected for every command.
    pub fn workspace(&self) -> &str {
        &self.workspace
    }

    /// Existing sandbox selected for every command.
    pub fn sandbox(&self) -> &str {
        &self.sandbox
    }
}

impl ProcessRunner for OpenShellRunner {
    fn execute<'a>(
        &'a self,
        request: &'a ExecutionRequest,
        input: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionOutput, SandboxError>> + 'a>> {
        Box::pin(async move { self.execute_command(request, input).await })
    }
}

impl OpenShellRunner {
    async fn execute_command(
        &self,
        request: &ExecutionRequest,
        input: &[u8],
    ) -> Result<ExecutionOutput, SandboxError> {
        validate_request(request, input)?;
        let wait_budget = Duration::from_millis(request.policy.limits.timeout_ms);
        match tokio::time::timeout(wait_budget, self.execute_command_until_exit(request, input))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(remote_error(
                SandboxPhase::Wait,
                "local wait deadline elapsed; remote command outcome is unknown".to_owned(),
                Some(ExecutableIdentity {
                    requested_path: request.executable.clone(),
                    canonical_path: None,
                }),
            )),
        }
    }

    async fn execute_command_until_exit(
        &self,
        request: &ExecutionRequest,
        input: &[u8],
    ) -> Result<ExecutionOutput, SandboxError> {
        let executable = request
            .executable
            .to_str()
            .ok_or_else(|| invalid_config("OpenShell executable path must be UTF-8"))?;
        let mut command = Vec::with_capacity(request.args.len() + 1);
        command.push(executable.to_owned());
        command.extend(request.args.iter().cloned());

        let workdir = request
            .working_directory
            .as_deref()
            .map(|path| {
                path.to_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid_config("OpenShell working directory must be UTF-8"))
            })
            .transpose()?
            .unwrap_or_default();

        let execution_timeout = proto_duration(request.policy.limits.timeout_ms)?;
        let proto_request = raw::ExecSandboxRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            workspace_scope: Some(raw::proto::workspace_selector(self.workspace.clone())),
            sandbox: self.sandbox.clone(),
            command,
            workdir,
            environment: request
                .policy
                .environment
                .variables
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            execution_timeout: Some(execution_timeout),
            stdin: input.to_vec(),
            tty: false,
            cols: 0,
            rows: 0,
            no_login_shell: true,
        };

        // Refresh proactively before admission. Never retry an exec after the
        // RPC starts: a transport failure can leave its remote outcome unknown.
        let mut grpc = self.client.raw_grpc_fresh().await.map_err(|error| {
            remote_error(
                SandboxPhase::Spawn,
                format!("OpenShell client setup failed: {error}"),
                None,
            )
        })?;
        let response = grpc.exec_sandbox(proto_request).await.map_err(|status| {
            remote_error(
                SandboxPhase::Spawn,
                format!("OpenShell rejected command execution: {status}"),
                None,
            )
        })?;
        let executable_identity = ExecutableIdentity {
            requested_path: request.executable.clone(),
            // OpenShell does not return the path resolved inside the sandbox.
            canonical_path: None,
        };
        let (stdout, stderr, exit_code) = collect_events(
            response.into_inner(),
            request.policy.limits.output_bytes,
            request.policy.limits.stderr_bytes,
            executable_identity.clone(),
        )
        .await?;

        let termination = TerminationReason::Exited {
            code: Some(exit_code),
            signal: None,
        };

        Ok(ExecutionOutput {
            success: exit_code == 0,
            stdout,
            stderr,
            // The runner uses the existing managed sandbox and cannot verify
            // the per-command enforcement state from ExecSandbox.
            enforcement: Enforcement::Degraded,
            executable: executable_identity,
            termination,
        })
    }
}

async fn collect_events<S, E>(
    mut stream: S,
    stdout_limit: usize,
    stderr_limit: usize,
    executable: ExecutableIdentity,
) -> Result<(Vec<u8>, Vec<u8>, i32), SandboxError>
where
    S: futures::Stream<Item = Result<raw::proto::ExecSandboxEvent, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut stdout = Vec::with_capacity(stdout_limit.min(16 * 1024));
    let mut stderr = Vec::with_capacity(stderr_limit.min(16 * 1024));
    loop {
        let event = match stream.next().await {
            Some(Ok(event)) => event,
            Some(Err(error)) => {
                return Err(remote_error(
                    SandboxPhase::Wait,
                    format!("OpenShell execution stream failed: {error}"),
                    Some(executable),
                ));
            }
            None => {
                return Err(remote_error(
                    SandboxPhase::Wait,
                    "OpenShell execution stream closed without an exit event".to_owned(),
                    Some(executable),
                ));
            }
        };
        match event.payload {
            Some(raw::proto::exec_sandbox_event::Payload::Stdout(chunk)) => {
                if append_bounded(&mut stdout, &chunk.data, stdout_limit) {
                    return Err(SandboxError::output_too_large().with_executable(executable));
                }
            }
            Some(raw::proto::exec_sandbox_event::Payload::Stderr(chunk)) => {
                if append_bounded(&mut stderr, &chunk.data, stderr_limit) {
                    return Err(SandboxError::stderr_too_large().with_executable(executable));
                }
            }
            Some(raw::proto::exec_sandbox_event::Payload::Exit(exit)) => {
                return Ok((stdout, stderr, exit.exit_code));
            }
            None => {}
        }
    }
}

fn validate_request(request: &ExecutionRequest, input: &[u8]) -> Result<(), SandboxError> {
    let executable = request
        .executable
        .to_str()
        .ok_or_else(|| invalid_config("OpenShell executable path must be UTF-8"))?;
    if !executable.starts_with('/') {
        return Err(SandboxError::relative_executable());
    }
    if executable.contains('\0') || request.args.iter().any(|arg| arg.contains('\0')) {
        return Err(invalid_config(
            "OpenShell command arguments must not contain NUL",
        ));
    }

    if let Some(working_directory) = &request.working_directory {
        let path = working_directory
            .to_str()
            .ok_or_else(|| invalid_config("OpenShell working directory must be UTF-8"))?;
        if !path.starts_with('/') {
            return Err(SandboxError::relative_working_directory());
        }
        if path.contains('\0') || path.contains('\n') || path.contains('\r') {
            return Err(invalid_config(
                "OpenShell working directory must not contain NUL or newline characters",
            ));
        }
    }

    if input.len() > request.policy.limits.input_bytes {
        return Err(SandboxError::input_too_large());
    }
    if request.policy.filesystem.is_some() {
        return Err(unsupported(
            "OpenShell ExecSandbox cannot enforce a per-command filesystem policy",
        ));
    }
    if request.policy.network == NetworkMode::Disabled {
        return Err(unsupported(
            "OpenShell ExecSandbox cannot enforce a per-command network-disabled policy",
        ));
    }
    if request.policy.enforcement == EnforcementRequirement::Required {
        return Err(unsupported(
            "OpenShell ExecSandbox cannot report that required per-command enforcement was applied",
        ));
    }
    if request.policy.limits.has_kernel_limits() {
        return Err(unsupported(
            "OpenShell ExecSandbox does not accept per-command memory, process-count, or CPU limits",
        ));
    }
    if !request.policy.environment.inherit.is_empty() {
        return Err(unsupported(
            "host environment inheritance cannot be applied to a remote OpenShell sandbox",
        ));
    }
    if !request
        .policy
        .environment
        .executable_search_paths
        .is_empty()
    {
        return Err(unsupported(
            "local executable search paths cannot be applied to a remote OpenShell sandbox",
        ));
    }
    agent_supervisor::validate_environment_policy(&request.policy.environment).map_err(
        |error| invalid_config(format!("invalid OpenShell environment policy: {error}")),
    )?;

    Ok(())
}

fn append_bounded(output: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
    let remaining = limit.saturating_sub(output.len());
    output.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    chunk.len() > remaining
}

fn proto_duration(milliseconds: u64) -> Result<prost_types::Duration, SandboxError> {
    let seconds = i64::try_from(milliseconds / 1_000)
        .map_err(|_| unsupported("OpenShell timeout exceeds protobuf duration range"))?;
    Ok(prost_types::Duration {
        seconds,
        nanos: ((milliseconds % 1_000) * 1_000_000) as i32,
    })
}

fn invalid_config(message: impl Into<String>) -> SandboxError {
    SandboxError::new(
        SandboxErrorCode::Unsupported,
        SandboxPhase::Validate,
        message,
    )
}

fn unsupported(message: impl Into<String>) -> SandboxError {
    SandboxError::new(
        SandboxErrorCode::Unsupported,
        SandboxPhase::Validate,
        message,
    )
}

fn remote_error(
    phase: SandboxPhase,
    message: String,
    executable: Option<ExecutableIdentity>,
) -> SandboxError {
    let mut error = SandboxError::new(SandboxErrorCode::ExecutionFailed, phase, message);
    if let Some(executable) = executable {
        error = error.with_executable(executable);
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_supervisor::{EnvironmentPolicy, FilesystemPolicy, ResourceLimits, SandboxPolicy};
    use std::path::PathBuf;

    fn request() -> ExecutionRequest {
        ExecutionRequest {
            executable: PathBuf::from("/bin/echo"),
            args: vec!["hello".into()],
            working_directory: Some(PathBuf::from("/tmp")),
            policy: SandboxPolicy::default(),
        }
    }

    #[test]
    fn bounded_collection_keeps_exact_limit_and_reports_overflow() {
        let mut output = Vec::new();
        assert!(!append_bounded(&mut output, b"abc", 3));
        assert_eq!(output, b"abc");
        assert!(append_bounded(&mut output, b"de", 3));
        assert_eq!(output, b"abc");
    }

    #[test]
    fn bounded_collection_handles_zero_limit() {
        let mut output = Vec::new();
        assert!(!append_bounded(&mut output, b"", 0));
        assert!(append_bounded(&mut output, b"x", 0));
        assert!(output.is_empty());
    }

    #[test]
    fn timeout_converts_to_protobuf_duration() {
        assert_eq!(
            proto_duration(0).unwrap(),
            prost_types::Duration {
                seconds: 0,
                nanos: 0
            }
        );
        assert_eq!(
            proto_duration(1_234).unwrap(),
            prost_types::Duration {
                seconds: 1,
                nanos: 234_000_000
            }
        );
    }

    #[test]
    fn accepts_absolute_command_and_working_directory() {
        assert!(validate_request(&request(), b"input").is_ok());
    }

    #[test]
    fn rejects_relative_command_and_working_directory() {
        let mut req = request();
        req.executable = PathBuf::from("echo");
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::RelativeExecutable
        );
        let mut req = request();
        req.working_directory = Some(PathBuf::from("tmp"));
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::RelativeWorkingDirectory
        );
    }

    #[test]
    fn rejects_oversized_input() {
        let mut req = request();
        req.policy.limits.input_bytes = 2;
        assert_eq!(
            validate_request(&req, b"123").unwrap_err().code_kind(),
            SandboxErrorCode::InputTooLarge
        );
    }

    #[test]
    fn rejects_requirements_open_shell_cannot_enforce() {
        let mut req = request();
        req.policy.filesystem = Some(FilesystemPolicy::default());
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::Unsupported
        );
        let mut req = request();
        req.policy.network = NetworkMode::Disabled;
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::Unsupported
        );
        let mut req = request();
        req.policy.enforcement = EnforcementRequirement::Required;
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::Unsupported
        );
    }

    #[test]
    fn rejects_kernel_limits_and_host_environment_assumptions() {
        let mut req = request();
        req.policy.limits = ResourceLimits {
            memory_bytes: Some(1),
            ..ResourceLimits::default()
        };
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::Unsupported
        );
        let mut req = request();
        req.policy.environment.inherit.insert("PATH".into());
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::Unsupported
        );
        let mut req = request();
        req.policy.environment = EnvironmentPolicy {
            executable_search_paths: vec![PathBuf::from("/bin")],
            ..EnvironmentPolicy::default()
        };
        assert_eq!(
            validate_request(&req, b"").unwrap_err().code_kind(),
            SandboxErrorCode::Unsupported
        );
    }
}
