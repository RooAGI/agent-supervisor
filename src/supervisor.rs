use crate::process_control::ProcessContainer;
use crate::{
    apply_environment_policy, Enforcement, EnforcementRequirement, ExecutableIdentity,
    ExecutionRequest, ProcessInfo, ProcessReceipt, RestartPolicy, SandboxError, SupervisionConfig,
    SupervisionOutcome, TerminationReason,
};
use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct ExecutionOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub enforcement: Enforcement,
    pub executable: ExecutableIdentity,
    pub termination: TerminationReason,
}

/// A long-lived child whose process-group lifecycle remains owned by the
/// supervisor. Protocol implementations may take each pipe once, but cannot
/// take ownership of the underlying process.
pub struct SupervisedChild {
    child: ChildProcess,
    container: Arc<ProcessContainer>,
    process_id: Option<u32>,
    stdin: Option<BoxAsyncWrite>,
    stdout: Option<BoxAsyncRead>,
    stderr: Option<BoxAsyncRead>,
    executable: ExecutableIdentity,
    enforcement: Enforcement,
    started_at: SystemTime,
    output_limit: usize,
    stderr_limit: usize,
    lease: Option<ProcessLease>,
    owns_container: bool,
}

type BoxAsyncRead = Box<dyn AsyncRead + Unpin + Send>;
type BoxAsyncWrite = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;

enum ChildProcess {
    Tokio(Child),
    #[cfg(windows)]
    Windows(crate::windows_filesystem::WindowsChild),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ChildStatus {
    pub(super) code: Option<i32>,
    pub(super) signal: Option<i32>,
}

impl ChildProcess {
    async fn wait(&mut self) -> std::io::Result<ChildStatus> {
        match self {
            Self::Tokio(child) => child.wait().await.map(|status| ChildStatus {
                code: status.code(),
                #[cfg(unix)]
                signal: std::os::unix::process::ExitStatusExt::signal(&status),
                #[cfg(not(unix))]
                signal: None,
            }),
            #[cfg(windows)]
            Self::Windows(child) => child.wait().await,
        }
    }

    fn try_wait_sync(&mut self) -> std::io::Result<Option<ChildStatus>> {
        match self {
            Self::Tokio(child) => child.try_wait().map(|status| {
                status.map(|status| ChildStatus {
                    code: status.code(),
                    #[cfg(unix)]
                    signal: std::os::unix::process::ExitStatusExt::signal(&status),
                    #[cfg(not(unix))]
                    signal: None,
                })
            }),
            #[cfg(windows)]
            Self::Windows(child) => child.try_wait(),
        }
    }

    fn start_kill(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tokio(child) => child.start_kill(),
            #[cfg(windows)]
            Self::Windows(child) => child.kill(),
        }
    }
}

/// Runtime-scoped registry for children that must be cleaned up together.
/// This is intentionally in-process; it is not a daemon or a cross-agent
/// process manager.
pub struct Supervisor {
    processes: Arc<Mutex<BTreeMap<u32, Arc<ProcessContainer>>>>,
    events: broadcast::Sender<crate::LifecycleEvent>,
}

impl Supervisor {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            processes: Arc::new(Mutex::new(BTreeMap::new())),
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<crate::LifecycleEvent> {
        self.events.subscribe()
    }

    pub fn active_processes(&self) -> usize {
        lock_processes(&self.processes).len()
    }

    pub fn processes(&self) -> Vec<ProcessInfo> {
        lock_processes(&self.processes)
            .iter()
            .map(|(process_id, container)| {
                let member = container.members().ok().and_then(|mut members| {
                    members
                        .drain(..)
                        .find(|member| member.process_id == *process_id)
                });
                ProcessInfo {
                    process_id: *process_id,
                    alive: container.is_alive(),
                    parent_process_id: member.as_ref().and_then(|m| m.parent_process_id),
                    start_time_ticks: member.as_ref().and_then(|m| m.start_time_ticks),
                    executable: member.and_then(|m| m.executable),
                }
            })
            .collect()
    }

    fn register(
        &self,
        process_id: Option<u32>,
        container: Arc<ProcessContainer>,
        executable: ExecutableIdentity,
    ) -> Option<ProcessLease> {
        let process_id = process_id?;
        lock_processes(&self.processes).insert(process_id, Arc::clone(&container));
        let _ = self.events.send(crate::LifecycleEvent::Started {
            process_id,
            executable,
        });
        Some(ProcessLease {
            process_id,
            container,
            processes: Arc::clone(&self.processes),
            armed: true,
            events: self.events.clone(),
        })
    }

    pub fn terminate_all(&self) {
        let containers = {
            let mut set = lock_processes(&self.processes);
            std::mem::take(&mut *set).into_values().collect::<Vec<_>>()
        };
        let _ = self.events.send(crate::LifecycleEvent::ShutdownRequested);
        for container in containers {
            let _ = container.terminate();
        }
    }

    pub async fn supervise(
        &self,
        request: &ExecutionRequest,
        config: SupervisionConfig,
        cancellation: CancellationToken,
    ) -> Result<SupervisionOutcome, SandboxError> {
        let mut restarts = 0;
        let mut backoff = Duration::from_millis(config.backoff_ms);
        loop {
            let child = spawn_with_supervisor(request, self)?;
            let receipt = child
                .wait_with_cancellation(
                    cancellation.clone(),
                    Duration::from_millis(config.shutdown_grace_ms),
                )
                .await?;
            let crashed = !matches!(
                receipt.termination,
                TerminationReason::Exited { code: Some(0), .. }
            );
            let should_restart = match config.policy {
                RestartPolicy::Never => false,
                RestartPolicy::OnCrash => crashed,
                RestartPolicy::Always => true,
            } && restarts < config.max_restarts
                && !matches!(receipt.termination, TerminationReason::Cancelled);
            if !should_restart {
                return Ok(SupervisionOutcome {
                    restarts,
                    last_receipt: receipt,
                });
            }
            restarts += 1;
            let _ = self.events.send(crate::LifecycleEvent::Restarting {
                attempt: restarts,
                delay_ms: backoff.as_millis().min(u128::from(u64::MAX)) as u64,
                reason: receipt.termination.clone(),
            });
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(SupervisionOutcome { restarts, last_receipt: receipt }),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_millis(config.max_backoff_ms));
        }
    }
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.terminate_all();
    }
}

struct ProcessLease {
    process_id: u32,
    container: Arc<ProcessContainer>,
    processes: Arc<Mutex<BTreeMap<u32, Arc<ProcessContainer>>>>,
    armed: bool,
    events: broadcast::Sender<crate::LifecycleEvent>,
}

impl ProcessLease {
    fn emit_exit(&self, termination: TerminationReason) {
        let _ = self.events.send(crate::LifecycleEvent::Exited {
            process_id: self.process_id,
            termination,
        });
    }

    fn disarm(mut self) {
        self.armed = false;
        lock_processes(&self.processes).remove(&self.process_id);
    }
}

impl Drop for ProcessLease {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.container.terminate();
            lock_processes(&self.processes).remove(&self.process_id);
        }
    }
}

fn lock_processes(
    processes: &Mutex<BTreeMap<u32, Arc<ProcessContainer>>>,
) -> MutexGuard<'_, BTreeMap<u32, Arc<ProcessContainer>>> {
    processes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SupervisedChild {
    pub fn id(&self) -> Option<u32> {
        self.process_id
    }

    pub fn executable(&self) -> &ExecutableIdentity {
        &self.executable
    }

    pub fn enforcement(&self) -> Enforcement {
        self.enforcement
    }

    pub fn take_stdin(&mut self) -> Option<BoxAsyncWrite> {
        self.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<BoxAsyncRead> {
        self.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<BoxAsyncRead> {
        self.stderr.take()
    }

    pub fn into_event_stream(self) -> Result<crate::ProcessEventStream, SandboxError> {
        crate::ProcessEventStream::new(self)
    }

    pub(crate) fn output_limits(&self) -> (usize, usize) {
        (self.output_limit, self.stderr_limit)
    }

    pub async fn try_wait(&mut self) -> Result<Option<ProcessReceipt>, SandboxError> {
        let Some(status) = self
            .child
            .try_wait_sync()
            .map_err(SandboxError::execution)?
        else {
            return Ok(None);
        };
        Ok(Some(self.complete(exit_reason(&status))))
    }

    pub async fn wait(mut self) -> Result<ProcessReceipt, SandboxError> {
        let status = self.child.wait().await.map_err(SandboxError::execution)?;
        Ok(self.complete(exit_reason(&status)))
    }

    /// Wait until the process exits or cancellation is requested. Cancellation
    /// owns the same containment cleanup path as an explicit shutdown.
    pub async fn wait_with_cancellation(
        mut self,
        cancellation: CancellationToken,
        grace: Duration,
    ) -> Result<ProcessReceipt, SandboxError> {
        tokio::select! {
            result = self.child.wait() => {
                let status = result.map_err(SandboxError::execution)?;
                Ok(self.complete(exit_reason(&status)))
            }
            _ = cancellation.cancelled() => {
                self.stdin.take();
                let _ = self.container.request_graceful_stop();
                let _ = tokio::time::timeout(grace, self.child.wait()).await;
                if self.child.try_wait_sync().ok().flatten().is_none() {
                    terminate_process_tree(&mut self.child, &self.container).await;
                    let _ = self.child.wait().await;
                }
                Ok(self.complete(TerminationReason::Cancelled))
            }
        }
    }

    /// Close stdin, allow a bounded natural shutdown, then kill the complete
    /// process group if the child remains alive.
    pub async fn shutdown(mut self, grace: Duration) -> Result<ProcessReceipt, SandboxError> {
        self.stdin.take();
        let initial_grace = grace / 2;
        match tokio::time::timeout(initial_grace, self.child.wait()).await {
            Ok(Ok(status)) => Ok(self.complete(exit_reason(&status))),
            Ok(Err(error)) => Err(SandboxError::execution(error)),
            Err(_) => {
                self.container
                    .request_graceful_stop()
                    .map_err(SandboxError::execution)?;
                let remaining_grace = grace.saturating_sub(initial_grace);
                match tokio::time::timeout(remaining_grace, self.child.wait()).await {
                    Ok(Ok(status)) => Ok(self.complete(exit_reason(&status))),
                    Ok(Err(error)) => Err(SandboxError::execution(error)),
                    Err(_) => {
                        terminate_process_tree(&mut self.child, &self.container).await;
                        let _ = self.child.wait().await;
                        Ok(self.complete(TerminationReason::TimedOut))
                    }
                }
            }
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

    fn complete(&mut self, termination: TerminationReason) -> ProcessReceipt {
        self.process_id = None;
        if let Some(lease) = self.lease.take() {
            lease.emit_exit(termination.clone());
            lease.disarm();
        }
        self.receipt(termination)
    }
}

/// Wait for the first child to finish while preserving the remaining handles.
/// This is intentionally polling-based so the API works with a dynamically
/// sized set of children and keeps ownership of every handle with the caller.
pub async fn wait_any(
    children: &mut [&mut SupervisedChild],
) -> Result<(usize, ProcessReceipt), SandboxError> {
    loop {
        for (index, child) in children.iter_mut().enumerate() {
            if let Some(receipt) = child.try_wait().await? {
                return Ok((index, receipt));
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
}

pub async fn wait_all(
    children: &mut [&mut SupervisedChild],
) -> Result<Vec<ProcessReceipt>, SandboxError> {
    let mut receipts = (0..children.len()).map(|_| None).collect::<Vec<_>>();
    while receipts.iter().any(Option::is_none) {
        for (index, child) in children.iter_mut().enumerate() {
            if receipts[index].is_none() {
                receipts[index] = child.try_wait().await?;
            }
        }
        if receipts.iter().any(Option::is_none) {
            sleep(Duration::from_millis(10)).await;
        }
    }
    Ok(receipts.into_iter().flatten().collect())
}

impl Drop for SupervisedChild {
    fn drop(&mut self) {
        if let Ok(Some(_)) = self.child.try_wait_sync() {
            self.process_id = None;
            if let Some(lease) = self.lease.take() {
                lease.disarm();
            }
            return;
        }
        if self.owns_container {
            let _ = self.container.terminate();
            let _ = self.child.start_kill();
        }
    }
}

pub fn spawn(request: &ExecutionRequest) -> Result<SupervisedChild, SandboxError> {
    spawn_internal(request, None)
}

pub fn spawn_with_supervisor(
    request: &ExecutionRequest,
    supervisor: &Supervisor,
) -> Result<SupervisedChild, SandboxError> {
    spawn_internal(request, Some(supervisor))
}

fn spawn_internal(
    request: &ExecutionRequest,
    supervisor: Option<&Supervisor>,
) -> Result<SupervisedChild, SandboxError> {
    let identity = validate_request(request, &[])?;
    if let Some(policy) = &request.policy.filesystem {
        crate::filesystem::validate_policy(policy)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    }
    crate::policy::validate_network_mode(request.policy.network)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    #[cfg(windows)]
    validate_windows_network_policy(request, &identity)?;
    #[cfg(windows)]
    if request.policy.filesystem.is_some() {
        return spawn_windows_internal(request, supervisor, identity);
    }
    let mut command = Command::new(
        identity
            .canonical_path
            .as_ref()
            .expect("native path resolved"),
    );
    command.args(&request.args);
    if let Some(policy) = &request.policy.filesystem {
        crate::filesystem::wrap_command(&mut command, policy, request.policy.network)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    } else if request.policy.network == crate::NetworkMode::Disabled {
        crate::filesystem::wrap_network_command(&mut command)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    }
    command
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let container = ProcessContainer::new()
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let enforcement = container.enforcement_for_filesystem(request.policy.filesystem.is_some());
    if request.policy.enforcement == EnforcementRequirement::Required
        && enforcement != Enforcement::Enforced
    {
        return Err(SandboxError::enforcement_unavailable().with_executable(identity.clone()));
    }
    container
        .apply_limits(&request.policy.limits)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    container.prepare_command_in_group(
        &mut command,
        None,
        &request.policy.limits,
        request.policy.filesystem.as_ref(),
    );
    crate::network::prepare_command(&mut command, request.policy.network)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    apply_environment_policy(&mut command, &request.policy.environment)
        .map_err(|error| error.with_executable(identity.clone()))?;
    if let Some(directory) = &request.working_directory {
        command.current_dir(directory);
    }
    let mut child = command
        .spawn()
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    container
        .attach(&mut child)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let process_id = child.id();
    container
        .set_process_id(process_id)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let container = Arc::new(container);
    Ok(SupervisedChild {
        process_id,
        container: Arc::clone(&container),
        stdin: child
            .stdin
            .take()
            .map(|value| Box::new(value) as BoxAsyncWrite),
        stdout: child
            .stdout
            .take()
            .map(|value| Box::new(value) as BoxAsyncRead),
        stderr: child
            .stderr
            .take()
            .map(|value| Box::new(value) as BoxAsyncRead),
        child: ChildProcess::Tokio(child),
        executable: identity.clone(),
        enforcement,
        started_at: SystemTime::now(),
        output_limit: request.policy.limits.output_bytes,
        stderr_limit: request.policy.limits.stderr_bytes,
        lease: supervisor.and_then(|value| value.register(process_id, container, identity.clone())),
        owns_container: true,
    })
}

#[cfg(windows)]
fn spawn_windows_internal(
    request: &ExecutionRequest,
    supervisor: Option<&Supervisor>,
    identity: ExecutableIdentity,
) -> Result<SupervisedChild, SandboxError> {
    let container = Arc::new(
        ProcessContainer::new()
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?,
    );
    let enforcement = container.enforcement_for_filesystem(true);
    if request.policy.enforcement == EnforcementRequirement::Required
        && enforcement != Enforcement::Enforced
    {
        return Err(SandboxError::enforcement_unavailable().with_executable(identity));
    }
    container
        .apply_limits(&request.policy.limits)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let mut child = crate::windows_filesystem::WindowsChild::spawn(
        identity
            .canonical_path
            .as_ref()
            .expect("native path resolved"),
        &request.args,
        request.working_directory.as_deref(),
        &request.policy.environment,
        request
            .policy
            .filesystem
            .as_ref()
            .expect("validated filesystem policy"),
        request.policy.network,
        &container,
    )
    .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let process_id = Some(child.pid);
    container
        .set_process_id(process_id)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let lease = supervisor
        .and_then(|value| value.register(process_id, Arc::clone(&container), identity.clone()));
    Ok(SupervisedChild {
        process_id,
        container,
        stdin: child.take_stdin(),
        stdout: child.take_stdout(),
        stderr: child.take_stderr(),
        child: ChildProcess::Windows(child),
        executable: identity,
        enforcement,
        started_at: SystemTime::now(),
        output_limit: request.policy.limits.output_bytes,
        stderr_limit: request.policy.limits.stderr_bytes,
        lease,
        owns_container: true,
    })
}

pub(crate) fn spawn_group_child(
    request: &ExecutionRequest,
    container: Arc<ProcessContainer>,
    group_id: Option<u32>,
) -> Result<SupervisedChild, SandboxError> {
    let identity = validate_request(request, &[])?;
    if let Some(policy) = &request.policy.filesystem {
        crate::filesystem::validate_policy(policy)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    }
    crate::policy::validate_network_mode(request.policy.network)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    #[cfg(windows)]
    validate_windows_network_policy(request, &identity)?;
    #[cfg(windows)]
    if request.policy.filesystem.is_some() {
        return spawn_windows_group_child(request, container, identity);
    }
    let enforcement = container.enforcement_for_filesystem(request.policy.filesystem.is_some());
    if request.policy.enforcement == EnforcementRequirement::Required
        && enforcement != Enforcement::Enforced
    {
        return Err(SandboxError::enforcement_unavailable().with_executable(identity));
    }
    container
        .apply_limits(&request.policy.limits)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;

    let mut command = Command::new(
        identity
            .canonical_path
            .as_ref()
            .expect("native path resolved"),
    );
    command.args(&request.args);
    if let Some(policy) = &request.policy.filesystem {
        crate::filesystem::wrap_command(&mut command, policy, request.policy.network)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    } else if request.policy.network == crate::NetworkMode::Disabled {
        crate::filesystem::wrap_network_command(&mut command)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    }
    command
        .kill_on_drop(false)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    container.prepare_command_in_group(
        &mut command,
        group_id,
        &request.policy.limits,
        request.policy.filesystem.as_ref(),
    );
    crate::network::prepare_command(&mut command, request.policy.network)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    apply_environment_policy(&mut command, &request.policy.environment)
        .map_err(|error| error.with_executable(identity.clone()))?;
    if let Some(directory) = &request.working_directory {
        command.current_dir(directory);
    }
    let mut child = command
        .spawn()
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    container
        .attach(&mut child)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let process_id = child.id();
    container
        .set_process_id(process_id)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    Ok(SupervisedChild {
        process_id,
        container: Arc::clone(&container),
        stdin: child
            .stdin
            .take()
            .map(|value| Box::new(value) as BoxAsyncWrite),
        stdout: child
            .stdout
            .take()
            .map(|value| Box::new(value) as BoxAsyncRead),
        stderr: child
            .stderr
            .take()
            .map(|value| Box::new(value) as BoxAsyncRead),
        child: ChildProcess::Tokio(child),
        executable: identity,
        enforcement,
        started_at: SystemTime::now(),
        output_limit: request.policy.limits.output_bytes,
        stderr_limit: request.policy.limits.stderr_bytes,
        lease: None,
        owns_container: false,
    })
}

#[cfg(windows)]
fn spawn_windows_group_child(
    request: &ExecutionRequest,
    container: Arc<ProcessContainer>,
    identity: ExecutableIdentity,
) -> Result<SupervisedChild, SandboxError> {
    let enforcement = container.enforcement_for_filesystem(true);
    if request.policy.enforcement == EnforcementRequirement::Required
        && enforcement != Enforcement::Enforced
    {
        return Err(SandboxError::enforcement_unavailable().with_executable(identity));
    }
    container
        .apply_limits(&request.policy.limits)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let mut child = crate::windows_filesystem::WindowsChild::spawn(
        identity
            .canonical_path
            .as_ref()
            .expect("native path resolved"),
        &request.args,
        request.working_directory.as_deref(),
        &request.policy.environment,
        request
            .policy
            .filesystem
            .as_ref()
            .expect("validated filesystem policy"),
        request.policy.network,
        &container,
    )
    .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let process_id = Some(child.pid);
    container
        .set_process_id(process_id)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    Ok(SupervisedChild {
        process_id,
        container,
        stdin: child.take_stdin(),
        stdout: child.take_stdout(),
        stderr: child.take_stderr(),
        child: ChildProcess::Windows(child),
        executable: identity,
        enforcement,
        started_at: SystemTime::now(),
        output_limit: request.policy.limits.output_bytes,
        stderr_limit: request.policy.limits.stderr_bytes,
        lease: None,
        owns_container: false,
    })
}

pub async fn execute(
    request: &ExecutionRequest,
    input: &[u8],
) -> Result<ExecutionOutput, SandboxError> {
    execute_with_cancellation(request, input, CancellationToken::new()).await
}

pub async fn execute_with_cancellation(
    request: &ExecutionRequest,
    input: &[u8],
    cancellation: CancellationToken,
) -> Result<ExecutionOutput, SandboxError> {
    let identity = validate_request(request, input)?;
    if let Some(policy) = &request.policy.filesystem {
        crate::filesystem::validate_policy(policy)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    }
    crate::policy::validate_network_mode(request.policy.network)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    #[cfg(windows)]
    validate_windows_network_policy(request, &identity)?;
    #[cfg(windows)]
    if request.policy.filesystem.is_some() {
        return execute_windows_with_cancellation(request, input, cancellation, identity).await;
    }
    let mut command = Command::new(
        identity
            .canonical_path
            .as_ref()
            .expect("native path resolved"),
    );
    command.args(&request.args);
    if let Some(policy) = &request.policy.filesystem {
        crate::filesystem::wrap_command(&mut command, policy, request.policy.network)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    } else if request.policy.network == crate::NetworkMode::Disabled {
        crate::filesystem::wrap_network_command(&mut command)
            .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    }
    command
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let container = ProcessContainer::new()
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let enforcement = container.enforcement_for_filesystem(request.policy.filesystem.is_some());
    if request.policy.enforcement == EnforcementRequirement::Required
        && enforcement != Enforcement::Enforced
    {
        return Err(SandboxError::enforcement_unavailable().with_executable(identity.clone()));
    }
    container
        .apply_limits(&request.policy.limits)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    container.prepare_command_in_group(
        &mut command,
        None,
        &request.policy.limits,
        request.policy.filesystem.as_ref(),
    );
    crate::network::prepare_command(&mut command, request.policy.network)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    apply_environment_policy(&mut command, &request.policy.environment)
        .map_err(|error| error.with_executable(identity.clone()))?;
    if let Some(directory) = &request.working_directory {
        command.current_dir(directory);
    }

    let mut child = command
        .spawn()
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    container
        .attach(&mut child)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let process_id = child.id();
    container
        .set_process_id(process_id)
        .map_err(|error| SandboxError::spawn(error).with_executable(identity.clone()))?;
    let container = Arc::new(container);
    let stdin = child
        .stdin
        .take()
        .map(|value| Box::new(value) as BoxAsyncWrite);
    let stdout = child
        .stdout
        .take()
        .map(|value| Box::new(value) as BoxAsyncRead)
        .expect("piped stdout");
    let stderr = child
        .stderr
        .take()
        .map(|value| Box::new(value) as BoxAsyncRead)
        .expect("piped stderr");
    let mut child = ChildProcess::Tokio(child);
    let input = input.to_vec();
    let stdin_task = tokio::spawn(async move {
        if let Some(mut stdin) = stdin {
            stdin.write_all(&input).await?;
            stdin.shutdown().await?;
        }
        Ok::<(), std::io::Error>(())
    });

    let (stream_tx, mut stream_rx) = mpsc::channel(2);
    spawn_bounded_reader(
        stdout,
        request.policy.limits.output_bytes,
        StreamKind::Stdout,
        stream_tx.clone(),
    );
    spawn_bounded_reader(
        stderr,
        request.policy.limits.stderr_bytes,
        StreamKind::Stderr,
        stream_tx,
    );

    let deadline = sleep(Duration::from_millis(request.policy.limits.timeout_ms));
    tokio::pin!(deadline);
    let mut stdout = None;
    let mut stderr = None;
    let mut status = None;
    let mut forced_termination = None;

    while status.is_none() || stdout.is_none() || stderr.is_none() {
        tokio::select! {
            _ = cancellation.cancelled(), if forced_termination.is_none() => {
                forced_termination = Some(TerminationReason::Cancelled);
                break;
            }
            _ = &mut deadline, if forced_termination.is_none() => {
                forced_termination = Some(TerminationReason::TimedOut);
                break;
            }
            result = child.wait(), if status.is_none() => {
                status = Some(result.map_err(|error| {
                    SandboxError::execution(error).with_executable(identity.clone())
                })?);
            }
            event = stream_rx.recv(), if stdout.is_none() || stderr.is_none() => {
                match event.expect("reader retains channel") {
                    StreamEvent::Complete(StreamKind::Stdout, bytes) => stdout = Some(bytes),
                    StreamEvent::Complete(StreamKind::Stderr, bytes) => stderr = Some(bytes),
                    StreamEvent::Limit(StreamKind::Stdout, bytes) => {
                        stdout = Some(bytes);
                        forced_termination = Some(TerminationReason::StdoutLimitExceeded);
                        break;
                    }
                    StreamEvent::Limit(StreamKind::Stderr, bytes) => {
                        stderr = Some(bytes);
                        forced_termination = Some(TerminationReason::StderrLimitExceeded);
                        break;
                    }
                    StreamEvent::ReadFailed(error) => {
                        return Err(SandboxError::execution(error).with_executable(identity.clone()))
                    }
                }
            }
        }
    }

    if forced_termination.is_some() {
        terminate_process_tree(&mut child, &container).await;
        status = child.wait().await.ok();
        while stdout.is_none() || stderr.is_none() {
            let Some(event) = stream_rx.recv().await else {
                break;
            };
            match event {
                StreamEvent::Complete(StreamKind::Stdout, bytes)
                | StreamEvent::Limit(StreamKind::Stdout, bytes) => stdout = Some(bytes),
                StreamEvent::Complete(StreamKind::Stderr, bytes)
                | StreamEvent::Limit(StreamKind::Stderr, bytes) => stderr = Some(bytes),
                StreamEvent::ReadFailed(_) => break,
            }
        }
    }

    if forced_termination.is_none() {
        stdin_task
            .await
            .map_err(|error| {
                SandboxError::execution(std::io::Error::other(error))
                    .with_executable(identity.clone())
            })?
            .map_err(|error| SandboxError::stdin(error).with_executable(identity.clone()))?;
    } else {
        stdin_task.abort();
    }

    let termination = forced_termination.unwrap_or_else(|| exit_reason(status.as_ref().unwrap()));
    let success = matches!(
        &termination,
        TerminationReason::Exited { code: Some(0), .. }
    );
    Ok(ExecutionOutput {
        success,
        stdout: stdout.unwrap_or_default(),
        stderr: stderr.unwrap_or_default(),
        enforcement,
        executable: identity,
        termination,
    })
}

#[cfg(windows)]
fn validate_windows_network_policy(
    request: &ExecutionRequest,
    identity: &ExecutableIdentity,
) -> Result<(), SandboxError> {
    if request.policy.network == crate::NetworkMode::Disabled && request.policy.filesystem.is_none()
    {
        return Err(SandboxError::spawn(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Windows network isolation requires a filesystem policy",
        ))
        .with_executable(identity.clone()));
    }
    Ok(())
}

#[cfg(windows)]
async fn execute_windows_with_cancellation(
    request: &ExecutionRequest,
    input: &[u8],
    cancellation: CancellationToken,
    identity: ExecutableIdentity,
) -> Result<ExecutionOutput, SandboxError> {
    let mut child = spawn_windows_internal(request, None, identity.clone())?;
    let mut stdin = child.take_stdin();
    let stdout = child
        .take_stdout()
        .ok_or_else(|| SandboxError::spawn(std::io::Error::other("piped stdout unavailable")))?;
    let stderr = child
        .take_stderr()
        .ok_or_else(|| SandboxError::spawn(std::io::Error::other("piped stderr unavailable")))?;
    let input = input.to_vec();
    let stdin_task = tokio::spawn(async move {
        if let Some(mut stdin) = stdin.take() {
            stdin.write_all(&input).await?;
            stdin.shutdown().await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let stdout_task = tokio::spawn(read_limited(stdout, request.policy.limits.output_bytes));
    let stderr_task = tokio::spawn(read_limited(stderr, request.policy.limits.stderr_bytes));
    let timeout = sleep(Duration::from_millis(request.policy.limits.timeout_ms));
    tokio::pin!(timeout);
    let mut termination = None;
    let (status, stdout, stderr) = tokio::select! {
        _ = cancellation.cancelled() => {
            termination = Some(TerminationReason::Cancelled);
            terminate_process_tree(&mut child.child, &child.container).await;
            let _ = child.child.wait().await;
            (None, Vec::new(), Vec::new())
        }
        _ = &mut timeout => {
            termination = Some(TerminationReason::TimedOut);
            terminate_process_tree(&mut child.child, &child.container).await;
            let _ = child.child.wait().await;
            (None, Vec::new(), Vec::new())
        }
        result = async {
            let status = child.child.wait().await;
            let stdout = stdout_task.await.map_err(std::io::Error::other)??;
            let stderr = stderr_task.await.map_err(std::io::Error::other)??;
            stdin_task.await.map_err(std::io::Error::other)??;
            Ok::<_, std::io::Error>((status?, stdout, stderr))
        } => {
            let (status, stdout, stderr) = result.map_err(|error| SandboxError::execution(error).with_executable(identity.clone()))?;
            (Some(status), stdout, stderr)
        }
    };
    if termination.is_none() {
        termination = Some(exit_reason(&status.expect("native child status")));
    }
    let termination = termination.expect("termination set");
    Ok(ExecutionOutput {
        success: matches!(termination, TerminationReason::Exited { code: Some(0), .. }),
        stdout,
        stderr,
        enforcement: child.enforcement(),
        executable: identity,
        termination,
    })
}

#[cfg(windows)]
async fn read_limited(mut reader: BoxAsyncRead, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(limit.min(8 * 1024));
    let mut chunk = [0u8; 8 * 1024];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "child output limit exceeded",
            ));
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

pub(crate) fn validate_request(
    request: &ExecutionRequest,
    input: &[u8],
) -> Result<ExecutableIdentity, SandboxError> {
    if !request.executable.is_absolute() {
        return Err(SandboxError::relative_executable());
    }
    if request
        .working_directory
        .as_ref()
        .is_some_and(|path| !path.is_absolute())
    {
        return Err(SandboxError::relative_working_directory());
    }
    if input.len() > request.policy.limits.input_bytes {
        return Err(SandboxError::input_too_large());
    }
    validate_working_directory(request)?;
    let canonical_path =
        std::fs::canonicalize(&request.executable).map_err(SandboxError::invalid_executable)?;
    if !canonical_path.is_file() {
        return Err(SandboxError::invalid_executable(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "executable is not a regular file",
        )));
    }
    Ok(ExecutableIdentity {
        requested_path: request.executable.clone(),
        canonical_path: Some(canonical_path),
    })
}

fn validate_working_directory(request: &ExecutionRequest) -> Result<(), SandboxError> {
    let Some(working_directory) = request.working_directory.as_ref() else {
        return Ok(());
    };
    let Some(filesystem) = request.policy.filesystem.as_ref() else {
        return Ok(());
    };
    let canonical_directory = std::fs::canonicalize(working_directory)
        .map_err(SandboxError::invalid_working_directory)?;
    if !canonical_directory.is_dir() {
        return Err(SandboxError::invalid_working_directory(
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "working directory is not a directory",
            ),
        ));
    }
    let granted = filesystem.grants.iter().any(|grant| {
        let readable = grant.access.iter().any(|access| {
            matches!(
                access,
                crate::FilesystemAccess::Read
                    | crate::FilesystemAccess::Write
                    | crate::FilesystemAccess::Append
            )
        });
        readable
            && std::fs::canonicalize(&grant.root)
                .map(|root| canonical_directory.starts_with(root))
                .unwrap_or(false)
    });
    if granted {
        Ok(())
    } else {
        Err(SandboxError::working_directory_not_granted())
    }
}

#[derive(Clone, Copy)]
enum StreamKind {
    Stdout,
    Stderr,
}

enum StreamEvent {
    Complete(StreamKind, Vec<u8>),
    Limit(StreamKind, Vec<u8>),
    ReadFailed(std::io::Error),
}

fn spawn_bounded_reader(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    limit: usize,
    kind: StreamKind,
    events: mpsc::Sender<StreamEvent>,
) {
    tokio::spawn(async move {
        let mut output = Vec::with_capacity(limit.min(8 * 1024));
        let mut chunk = [0_u8; 8 * 1024];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) => {
                    let _ = events.send(StreamEvent::Complete(kind, output)).await;
                    return;
                }
                Ok(read) if output.len().saturating_add(read) <= limit => {
                    output.extend_from_slice(&chunk[..read]);
                }
                Ok(read) => {
                    let remaining = limit.saturating_sub(output.len());
                    output.extend_from_slice(&chunk[..remaining.min(read)]);
                    let _ = events.send(StreamEvent::Limit(kind, output)).await;
                    return;
                }
                Err(error) => {
                    let _ = events.send(StreamEvent::ReadFailed(error)).await;
                    return;
                }
            }
        }
    });
}

async fn terminate_process_tree(child: &mut ChildProcess, container: &ProcessContainer) {
    let _ = container.terminate();
    let _ = child.start_kill();
}

fn exit_reason(status: &ChildStatus) -> TerminationReason {
    TerminationReason::Exited {
        code: status.code,
        signal: status.signal,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn request(executable: &str) -> ExecutionRequest {
        ExecutionRequest {
            executable: PathBuf::from(executable),
            args: Vec::new(),
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        }
    }

    #[tokio::test]
    async fn environment_is_empty_unless_explicitly_released() {
        let mut request = request("/usr/bin/env");
        request.policy.environment.variables =
            BTreeMap::from([("PLUGIN_VISIBLE".into(), "allowed".into())]);
        let output = execute(&request, b"").await.unwrap();
        let environment = String::from_utf8(output.stdout).unwrap();
        assert!(environment.contains("PLUGIN_VISIBLE=allowed"));
        assert!(!environment.contains("HOME="));
        assert!(!environment.contains("ROO_PAT="));
        assert!(!environment.contains("PATH="));
    }

    #[tokio::test]
    async fn output_limit_terminates_while_streaming() {
        let mut request = request("/usr/bin/yes");
        request.policy.limits.output_bytes = 128;
        let output = execute(&request, b"").await.unwrap();
        assert_eq!(output.stdout.len(), 128);
        assert_eq!(output.termination, TerminationReason::StdoutLimitExceeded);
    }

    #[tokio::test]
    async fn cancellation_terminates_the_process() {
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let output = execute_with_cancellation(&request, b"", cancellation)
            .await
            .unwrap();
        assert_eq!(output.termination, TerminationReason::Cancelled);
    }

    #[tokio::test]
    async fn required_os_enforcement_fails_closed() {
        let mut request = request("/usr/bin/true");
        request.policy.enforcement = EnforcementRequirement::Required;
        assert!(matches!(
            execute(&request, b"").await,
            Err(error) if error.code() == "sandbox_enforcement_unavailable"
        ));
    }

    #[tokio::test]
    async fn disabled_network_blocks_ip_socket_creation() {
        let mut request = request("/usr/bin/python3");
        request.policy.network = crate::NetworkMode::Disabled;
        request.args = vec![
            "-c".into(),
            "import socket; socket.socket(socket.AF_INET, socket.SOCK_STREAM)".into(),
        ];
        let output = execute(&request, b"").await.unwrap();
        assert!(!output.success, "IP socket creation unexpectedly succeeded");
    }

    #[tokio::test]
    async fn rejects_relative_working_directory() {
        let mut request = request("/usr/bin/true");
        request.working_directory = Some(PathBuf::from("."));
        let error = execute(&request, b"").await.unwrap_err();
        assert_eq!(error.code(), "relative_working_directory");
    }

    #[tokio::test]
    async fn rejects_ungranted_working_directory() {
        let mut request = request("/usr/bin/true");
        request.working_directory = Some(std::env::current_dir().unwrap());
        request.policy.filesystem = Some(crate::FilesystemPolicy::deny_all());
        let error = execute(&request, b"").await.unwrap_err();
        assert_eq!(error.code(), "working_directory_not_granted");
    }

    #[tokio::test]
    async fn supervised_child_closes_stdin_and_reports_exit() {
        let request = request("/bin/cat");
        let child = spawn(&request).unwrap();
        let receipt = child.shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(
            receipt.termination,
            TerminationReason::Exited { code: Some(0), .. }
        ));
        assert!(receipt
            .executable
            .canonical_path
            .as_ref()
            .is_some_and(|path| path.is_absolute()));
    }

    #[tokio::test]
    async fn supervisor_emits_started_and_exited_events() {
        let supervisor = Supervisor::new();
        let mut events = supervisor.subscribe();
        let child = spawn_with_supervisor(&request("/usr/bin/true"), &supervisor).unwrap();
        let started = events.recv().await.unwrap();
        assert!(matches!(started, crate::LifecycleEvent::Started { .. }));
        child.wait().await.unwrap();
        let exited = events.recv().await.unwrap();
        assert!(matches!(exited, crate::LifecycleEvent::Exited { .. }));
    }

    #[tokio::test]
    async fn supervision_is_bounded_by_restart_budget() {
        let supervisor = Supervisor::new();
        let mut events = supervisor.subscribe();
        let config = SupervisionConfig {
            policy: RestartPolicy::Always,
            max_restarts: 2,
            backoff_ms: 1,
            max_backoff_ms: 2,
            ..SupervisionConfig::default()
        };
        let outcome = supervisor
            .supervise(&request("/usr/bin/false"), config, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(outcome.restarts, 2);
        assert!(matches!(
            outcome.last_receipt.termination,
            TerminationReason::Exited { code: Some(1), .. }
        ));
        let mut restarting = 0;
        while let Ok(event) = events.try_recv() {
            if matches!(event, crate::LifecycleEvent::Restarting { .. }) {
                restarting += 1;
            }
        }
        assert_eq!(restarting, 2);
    }

    #[tokio::test]
    async fn supervised_child_wait_honors_cancellation() {
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];
        let child = spawn(&request).unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let receipt = child
            .wait_with_cancellation(cancellation, Duration::from_millis(100))
            .await
            .unwrap();
        assert_eq!(receipt.termination, TerminationReason::Cancelled);
    }

    #[tokio::test]
    async fn dropping_runtime_supervisor_terminates_registered_children() {
        let supervisor = Supervisor::new();
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];
        let child = spawn_with_supervisor(&request, &supervisor).unwrap();
        assert_eq!(supervisor.active_processes(), 1);
        drop(supervisor);

        let receipt = child.wait().await.unwrap();
        assert!(matches!(
            receipt.termination,
            TerminationReason::Exited {
                signal: Some(_),
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shared_group_child_drop_does_not_terminate_siblings() {
        let group = crate::ProcessGroup::new().unwrap();
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];

        let first = group.start(&request).unwrap();
        let second = group.start(&request).unwrap();
        assert!(group.is_alive());
        drop(first);
        assert!(group.is_alive());

        group.terminate();
        let receipt = second.wait().await.unwrap();
        assert!(matches!(
            receipt.termination,
            TerminationReason::Exited {
                signal: Some(_),
                ..
            }
        ));
        #[cfg(target_os = "linux")]
        tokio::time::timeout(Duration::from_secs(1), async {
            while group.is_alive() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("terminated process group should become inactive");
    }

    #[tokio::test]
    async fn explicit_termination_clears_the_registry() {
        let supervisor = Supervisor::new();
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];
        let child = spawn_with_supervisor(&request, &supervisor).unwrap();
        assert_eq!(supervisor.active_processes(), 1);

        supervisor.terminate_all();

        assert_eq!(supervisor.active_processes(), 0);
        let receipt = child.wait().await.unwrap();
        assert!(matches!(
            receipt.termination,
            TerminationReason::Exited {
                signal: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn concurrent_registration_and_termination_are_serialized() {
        let supervisor = Arc::new(Supervisor::new());
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let supervisor = Arc::clone(&supervisor);
                scope.spawn(move || {
                    let container = Arc::new(ProcessContainer::new().unwrap());
                    let _lease = supervisor.register(
                        Some(u32::MAX),
                        container,
                        ExecutableIdentity {
                            requested_path: "test".into(),
                            canonical_path: Some("test".into()),
                        },
                    );
                });
            }
            let supervisor_for_termination = Arc::clone(&supervisor);
            scope.spawn(move || supervisor_for_termination.terminate_all());
        });
        assert!(supervisor.active_processes() <= 1);
        supervisor.terminate_all();
        assert_eq!(supervisor.active_processes(), 0);
    }
}
