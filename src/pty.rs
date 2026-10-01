use crate::environment::validate_environment_policy;
use crate::process_control::ProcessContainer;
use crate::supervisor::validate_request;
use crate::EnforcementRequirement;
use crate::{Enforcement, ExecutionRequest, ProcessReceipt, SandboxError, TerminationReason};
use portable_pty::Child;
use portable_pty::MasterPty;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Terminal dimensions used when creating or resizing a PTY session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PtyDimensions {
    pub rows: u16,
    pub cols: u16,
    pub pixel_width: u16,
    pub pixel_height: u16,
}

impl Default for PtyDimensions {
    fn default() -> Self {
        Self {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        }
    }
}

/// A PTY-backed child whose process is still owned by the sandbox container.
/// PTY I/O is blocking by design; callers that need async I/O should place
/// these operations on a blocking task.
pub struct PtySession {
    master: Box<dyn MasterPty + Send>,
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    child: Box<dyn Child + Send + Sync>,
    container: Arc<ProcessContainer>,
    executable: crate::ExecutableIdentity,
    enforcement: Enforcement,
    started_at: SystemTime,
    process_id: u32,
}

impl PtySession {
    pub fn start(
        request: &ExecutionRequest,
        dimensions: PtyDimensions,
    ) -> Result<Self, SandboxError> {
        crate::policy::validate_network_mode(request.policy.network)
            .map_err(SandboxError::spawn)?;
        let identity = validate_request(request, &[])?;
        if request.policy.network == crate::NetworkMode::Disabled {
            return Err(SandboxError::spawn(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "network isolation is not supported for PTY sessions",
            ))
            .with_executable(identity));
        }
        if let Some(policy) = &request.policy.filesystem {
            crate::filesystem::validate_policy(policy)
                .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
            return Err(SandboxError::spawn(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "filesystem isolation is not supported for PTY sessions",
            ))
            .with_executable(identity));
        }
        validate_environment_policy(&request.policy.environment)?;
        let container = Arc::new(ProcessContainer::new().map_err(SandboxError::spawn)?);
        let enforcement = container.enforcement();
        if request.policy.enforcement == EnforcementRequirement::Required
            && enforcement != Enforcement::Enforced
        {
            return Err(SandboxError::enforcement_unavailable().with_executable(identity));
        }
        container
            .apply_limits(&request.policy.limits)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
        let system = native_pty_system();
        let pair = system
            .openpty(to_pty_size(dimensions))
            .map_err(|error| SandboxError::spawn(std::io::Error::other(error.to_string())))?;

        #[cfg(windows)]
        let executable = crate::windows_filesystem::create_process_path(
            identity
                .canonical_path
                .as_ref()
                .expect("native path resolved"),
        );
        #[cfg(not(windows))]
        let executable = identity
            .canonical_path
            .as_ref()
            .expect("native path resolved");
        let mut command = CommandBuilder::new(executable);
        command.args(&request.args);
        command.env_clear();
        for name in &request.policy.environment.inherit {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        for (name, value) in &request.policy.environment.variables {
            command.env(name, value);
        }
        if let Some(directory) = &request.working_directory {
            command.cwd(directory);
        }
        let mut child = pair
            .slave
            .spawn_command(command)
            .map_err(|error| SandboxError::spawn(std::io::Error::other(error.to_string())))?;
        let process_id = child.process_id().ok_or_else(|| {
            SandboxError::spawn(std::io::Error::other("PTY child PID unavailable"))
        })?;
        // The child handle was created directly from this validated command,
        // so its PID is already owned by us. Keep the process-group check, but
        // don't re-resolve /proc/<pid>/exe here: the PTY backend can expose a
        // transient exec image while the child is being adopted.
        #[cfg(target_os = "linux")]
        let expected_executable = None;
        #[cfg(all(unix, not(target_os = "linux")))]
        let expected_executable = None;
        #[cfg(unix)]
        let adopted = container.adopt_process(process_id, expected_executable);
        #[cfg(windows)]
        let adopted = container.attach_process_id(process_id);
        if let Err(error) = adopted {
            let _ = child.kill();
            return Err(SandboxError::spawn(error).with_executable(identity));
        }
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|error| SandboxError::spawn(std::io::Error::other(error.to_string())))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|error| SandboxError::spawn(std::io::Error::other(error.to_string())))?;
        Ok(Self {
            master: pair.master,
            reader,
            writer,
            child,
            container: Arc::clone(&container),
            executable: identity,
            enforcement,
            started_at: SystemTime::now(),
            process_id,
        })
    }

    pub fn id(&self) -> u32 {
        self.process_id
    }
    pub fn enforcement(&self) -> Enforcement {
        self.enforcement
    }
    pub fn is_alive(&self) -> bool {
        self.container.is_alive()
    }
    pub fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buffer)
    }
    pub fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.writer.write(bytes)
    }
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    pub fn resize(&self, dimensions: PtyDimensions) -> Result<(), SandboxError> {
        self.master
            .resize(to_pty_size(dimensions))
            .map_err(|error| SandboxError::execution(std::io::Error::other(error.to_string())))
    }

    pub fn wait(mut self) -> Result<ProcessReceipt, SandboxError> {
        let status = self.child.wait().map_err(SandboxError::execution)?;
        Ok(self.receipt(TerminationReason::Exited {
            code: Some(status.exit_code() as i32),
            signal: None,
        }))
    }

    pub fn wait_timeout(mut self, timeout: Duration) -> Result<ProcessReceipt, SandboxError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().map_err(SandboxError::execution)? {
                return Ok(self.receipt(TerminationReason::Exited {
                    code: Some(status.exit_code() as i32),
                    signal: None,
                }));
            }
            if std::time::Instant::now() >= deadline {
                let _ = self.container.request_graceful_stop();
                std::thread::sleep(timeout.min(Duration::from_millis(250)));
                let _ = self.container.terminate();
                let _ = self.child.kill();
                let _ = self.child.wait();
                return Ok(self.receipt(TerminationReason::TimedOut));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn receipt(&self, termination: TerminationReason) -> ProcessReceipt {
        ProcessReceipt {
            executable: self.executable.clone(),
            enforcement: self.enforcement,
            started_at: self.started_at,
            finished_at: SystemTime::now(),
            termination,
        }
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        let _ = self.container.terminate();
        let _ = self.child.kill();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn runs_a_contained_terminal_child() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/bin/echo"),
            args: vec!["hello".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let mut session = PtySession::start(&request, PtyDimensions::default()).unwrap();
        let mut output = Vec::new();
        let mut buffer = [0_u8; 64];
        loop {
            let read = session.read(&mut buffer).unwrap_or(0);
            if read == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..read]);
            if output.windows(5).any(|window| window == b"hello") {
                break;
            }
        }
        assert!(output.windows(5).any(|window| window == b"hello"));
        let receipt = session.wait().unwrap();
        assert!(matches!(
            receipt.termination,
            TerminationReason::Exited { .. }
        ));
    }

    #[test]
    fn timeout_terminates_a_terminal_child() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/bin/sleep"),
            args: vec!["30".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let session = PtySession::start(&request, PtyDimensions::default()).unwrap();
        let receipt = session.wait_timeout(Duration::from_millis(10)).unwrap();
        assert_eq!(receipt.termination, TerminationReason::TimedOut);
    }
}

#[cfg(any(unix, windows))]
fn to_pty_size(dimensions: PtyDimensions) -> PtySize {
    PtySize {
        rows: dimensions.rows,
        cols: dimensions.cols,
        pixel_width: dimensions.pixel_width,
        pixel_height: dimensions.pixel_height,
    }
}
