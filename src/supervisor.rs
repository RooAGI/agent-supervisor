use crate::agent_run::{AgentRun, AgentRunPolicy, AgentRunRegistry};
use crate::event_log::{EventLog, EventStream, ProcessTap, StreamScope};
use crate::process_control::ProcessContainer;
use crate::{
    apply_environment_policy, Enforcement, EnforcementRequirement, ExecutableIdentity,
    ExecutionRequest, ProcessEvent, ProcessInfo, ProcessReceipt, RestartPolicy, SandboxError,
    SupervisionConfig, SupervisionOutcome, TerminationReason,
};
use futures::future::join_all;
use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{oneshot, watch};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
    supervisor_id: Option<SupervisorProcessId>,
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
    tap: Option<ProcessTap>,
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
    processes: Arc<Mutex<BTreeMap<SupervisorProcessId, RegisteredProcess>>>,
    jobs: Arc<Mutex<BTreeMap<SupervisorProcessId, ManagedJobControl>>>,
    supervisor_id: Uuid,
    next_process_id: Arc<AtomicU64>,
    events: broadcast::Sender<crate::LifecycleEvent>,
    agent_runs: Arc<AgentRunRegistry>,
    owned_process_ids: Option<Arc<Mutex<BTreeSet<SupervisorProcessId>>>>,
    shutdown_on_drop: bool,
    log: Arc<EventLog>,
    /// Set on the per-run view, so its processes are attributed to that run.
    run_id: Option<u64>,
}

/// Stable identity for a process registered with one [`Supervisor`]. The ID
/// includes a random supervisor-instance identity, so a saved ID from an old
/// application lifetime cannot accidentally refer to a new process whose
/// local sequence has been reused.
///
/// The string representation is versioned (`v1:<supervisor-uuid>:<sequence>`)
/// and can be stored with application or framework checkpoint data. It does
/// not restore a process after its owning supervisor exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SupervisorProcessId {
    supervisor_id: Uuid,
    sequence: u64,
}

impl SupervisorProcessId {
    /// Return this process's sequence number within its supervisor instance.
    /// Use the full ID, not this number alone, for persistence or comparisons.
    pub fn get(self) -> u64 {
        self.sequence
    }
}

impl std::fmt::Display for SupervisorProcessId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "v1:{}:{}", self.supervisor_id, self.sequence)
    }
}

impl std::str::FromStr for SupervisorProcessId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split(':');
        let version = parts
            .next()
            .ok_or_else(|| "missing process ID version".to_string())?;
        let supervisor_id = parts
            .next()
            .ok_or_else(|| "missing supervisor ID".to_string())?;
        let sequence = parts
            .next()
            .ok_or_else(|| "missing process sequence".to_string())?;
        if version != "v1" || parts.next().is_some() {
            return Err("unsupported or malformed supervisor process ID".into());
        }
        let supervisor_id = Uuid::parse_str(supervisor_id)
            .map_err(|error| format!("invalid supervisor ID: {error}"))?;
        let sequence = sequence
            .parse::<u64>()
            .map_err(|error| format!("invalid process sequence: {error}"))?;
        Ok(Self {
            supervisor_id,
            sequence,
        })
    }
}

impl serde::Serialize for SupervisorProcessId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for SupervisorProcessId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let token = <String as serde::Deserialize>::deserialize(deserializer)?;
        token.parse().map_err(serde::de::Error::custom)
    }
}

/// A snapshot of one process registered with a [`Supervisor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedProcessInfo {
    pub supervisor_id: SupervisorProcessId,
    /// Operating-system process ID. This can be reused after the process exits.
    pub process_id: u32,
    pub alive: bool,
    pub parent_process_id: Option<u32>,
    pub start_time_ticks: Option<u64>,
    pub executable: Option<std::path::PathBuf>,
}

/// Lifecycle status for a job started through [`Supervisor::start_background`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedProcessStatus {
    Running,
    Completed,
    Failed,
}

/// Bounded output and final process receipt from a background-managed job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedProcessResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// True when the job wrote more than its output limits, so `stdout` or
    /// `stderr` holds only the newest bytes.
    pub output_truncated: bool,
    pub receipt: ProcessReceipt,
    /// Set when reading the job's output failed.
    pub error: Option<String>,
}

/// Result of asking a supervisor to stop one registered process tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStopResult {
    /// A graceful stop request was sent and the process container reported no
    /// live processes before the grace period ended. The child handle still
    /// must be waited to collect its final receipt.
    GracefulStopRequested,
    /// The grace period ended while the process container still reported live
    /// processes, so a force-stop request was sent. Reaping is still owned by
    /// the child handle.
    ForceStopRequested,
    /// A stop request has already been issued for this registration.
    AlreadyStopping,
    /// The process was already no longer alive when the request was handled.
    AlreadyExited,
    /// The ID is not registered with this supervisor.
    NotFound,
}

#[derive(Clone)]
struct RegisteredProcess {
    process_id: u32,
    container: Arc<ProcessContainer>,
    stop_requested: Arc<AtomicBool>,
    shutdown_lock: Arc<AsyncMutex<()>>,
}

#[derive(Clone)]
struct ManagedJobControl {
    stdin: Option<mpsc::Sender<ManagedJobCommand>>,
    state: watch::Receiver<ManagedJobState>,
}

#[derive(Debug, Clone)]
enum ManagedJobState {
    Running,
    Completed(ManagedProcessResult),
    Failed(String),
}

enum ManagedJobCommand {
    WriteStdin {
        bytes: Vec<u8>,
        reply: oneshot::Sender<Result<(), SandboxError>>,
    },
    CloseStdin {
        reply: oneshot::Sender<Result<(), SandboxError>>,
    },
}

impl Supervisor {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            processes: Arc::new(Mutex::new(BTreeMap::new())),
            jobs: Arc::new(Mutex::new(BTreeMap::new())),
            supervisor_id: Uuid::new_v4(),
            next_process_id: Arc::new(AtomicU64::new(1)),
            events,
            agent_runs: Arc::new(AgentRunRegistry::default()),
            owned_process_ids: None,
            shutdown_on_drop: true,
            log: EventLog::new(),
            run_id: None,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<crate::LifecycleEvent> {
        self.events.subscribe()
    }

    /// Create and register one agent run owned by this supervisor.
    pub fn start_agent_run(&self, policy: AgentRunPolicy) -> AgentRun {
        AgentRun::new(policy, self)
    }

    /// Return the number of agent runs currently owned by this supervisor.
    pub fn active_agent_runs(&self) -> usize {
        self.agent_runs.active_count()
    }

    pub(crate) fn agent_run_view(&self, run_id: u64) -> Supervisor {
        Supervisor {
            processes: Arc::clone(&self.processes),
            jobs: Arc::clone(&self.jobs),
            supervisor_id: self.supervisor_id,
            next_process_id: Arc::clone(&self.next_process_id),
            events: self.events.clone(),
            agent_runs: Arc::new(AgentRunRegistry::default()),
            owned_process_ids: Some(Arc::new(Mutex::new(BTreeSet::new()))),
            shutdown_on_drop: false,
            log: Arc::clone(&self.log),
            run_id: Some(run_id),
        }
    }

    pub(crate) fn agent_run_registry(&self) -> &Arc<AgentRunRegistry> {
        &self.agent_runs
    }

    pub(crate) fn owned_process_ids(&self) -> Option<Vec<SupervisorProcessId>> {
        self.owned_process_ids
            .as_ref()
            .map(|ids| lock_process_ids(ids).iter().copied().collect())
    }

    pub fn active_processes(&self) -> usize {
        self.owned_process_ids
            .as_ref()
            .map(|ids| lock_process_ids(ids).len())
            .unwrap_or_else(|| lock_processes(&self.processes).len())
    }

    /// Start a caller-owned child and register it with this supervisor for
    /// shared cleanup. The returned [`SupervisedChild`] remains responsible
    /// for reading output and calling `wait`; use [`Self::start_background`]
    /// when the supervisor should own output draining and reaping.
    pub fn start(
        &self,
        command: &crate::Command,
        policy: &crate::Policy,
    ) -> Result<SupervisedChild, SandboxError> {
        let request = command.request(policy);
        spawn_with_supervisor(&request, self)
    }

    /// Watch this supervisor's work. This only observes: use [`Self::start`],
    /// [`Self::start_background`], or [`AgentRun`] to launch work. Dropping the
    /// stream stops nothing, and a slow watcher never slows a process.
    ///
    /// A run from another supervisor yields an empty stream, as does a process
    /// ID that is unknown or has already exited and left the log.
    pub fn stream(&self, scope: StreamScope<'_>) -> EventStream {
        let own_run = match scope {
            StreamScope::Run(run) => run.supervisor_uuid() == self.supervisor_id,
            _ => true,
        };
        EventStream::new(Arc::clone(&self.log), scope, own_run)
    }

    pub(crate) fn event_log(&self) -> &Arc<EventLog> {
        &self.log
    }

    pub(crate) fn uuid(&self) -> Uuid {
        self.supervisor_id
    }

    fn allocate_process_id(&self) -> SupervisorProcessId {
        SupervisorProcessId {
            supervisor_id: self.supervisor_id,
            sequence: self.next_process_id.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn process_tap(&self, id: SupervisorProcessId) -> ProcessTap {
        ProcessTap::new(Arc::clone(&self.log), self.run_id, id)
    }

    /// A tap for a one-shot command, which is not in the process registry.
    pub(crate) fn one_shot_tap(&self) -> ProcessTap {
        self.process_tap(self.allocate_process_id())
    }

    /// Start a background job whose output draining, process wait, and reaping
    /// are owned by this supervisor. An active Tokio runtime is required for
    /// the monitor task. The job may write any amount of output: the newest
    /// bytes are retained up to the configured stdout and stderr limits, and
    /// older bytes are discarded rather than stopping the job. The returned ID can be used to write or close
    /// stdin, request a stop, and retrieve the final result.
    pub fn start_background(
        &self,
        command: &crate::Command,
        policy: &crate::Policy,
    ) -> Result<SupervisorProcessId, SandboxError> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            SandboxError::new(
                crate::SandboxErrorCode::Unsupported,
                crate::SandboxPhase::Spawn,
                "start_background requires an active Tokio runtime",
            )
        })?;

        let mut child = self.start(command, policy)?;
        let retained_limits = child.output_limits();
        child.remove_output_limits();
        let id = child.supervisor_id().ok_or_else(|| {
            SandboxError::execution(std::io::Error::other(
                "background child was not registered with its supervisor",
            ))
        })?;
        let mut stream = child.into_event_stream()?;
        let stdin = stream.take_stdin_writer();
        let (stdin_sender, stdin_receiver) = mpsc::channel(16);
        let (state_sender, state) = watch::channel(ManagedJobState::Running);
        lock_jobs(&self.jobs).insert(
            id,
            ManagedJobControl {
                stdin: Some(stdin_sender),
                state,
            },
        );
        tokio::spawn(run_managed_stdin(stdin, stdin_receiver));
        tokio::spawn(run_managed_job(
            id,
            stream,
            retained_limits,
            Arc::clone(&self.jobs),
            state_sender,
        ));
        Ok(id)
    }

    /// Return the lifecycle state for a background job, or `None` for an
    /// unknown or forgotten ID.
    pub fn process_status(&self, id: SupervisorProcessId) -> Option<ManagedProcessStatus> {
        let jobs = lock_jobs(&self.jobs);
        let job = jobs.get(&id)?;
        let state = job.state.borrow().clone();
        Some(match state {
            ManagedJobState::Running => ManagedProcessStatus::Running,
            ManagedJobState::Completed(_) => ManagedProcessStatus::Completed,
            ManagedJobState::Failed(_) => ManagedProcessStatus::Failed,
        })
    }

    /// Return a completed result snapshot, or `None` while the job is running
    /// or if its result is unknown/forgotten. Use [`Self::process_status`] to
    /// distinguish those cases.
    pub fn process_result(&self, id: SupervisorProcessId) -> Option<ManagedProcessResult> {
        let jobs = lock_jobs(&self.jobs);
        let job = jobs.get(&id)?;
        let state = job.state.borrow().clone();
        match state {
            ManagedJobState::Completed(result) => Some(result),
            ManagedJobState::Running | ManagedJobState::Failed(_) => None,
        }
    }

    /// Wait for one background job and return its result. `Ok(None)` means the
    /// ID is unknown or its result was consumed; an active job waits until its
    /// monitor has drained output and reaped the child.
    pub async fn wait_process(
        &self,
        id: SupervisorProcessId,
    ) -> Result<Option<ManagedProcessResult>, SandboxError> {
        let Some(mut state) = lock_jobs(&self.jobs).get(&id).map(|job| job.state.clone()) else {
            return Ok(None);
        };
        loop {
            match state.borrow_and_update().clone() {
                ManagedJobState::Running => {}
                ManagedJobState::Completed(result) => return Ok(Some(result)),
                ManagedJobState::Failed(message) => {
                    return Err(SandboxError::execution(std::io::Error::other(message)));
                }
            }
            state.changed().await.map_err(|_| {
                SandboxError::execution(std::io::Error::other(
                    "background process monitor ended without a result",
                ))
            })?;
        }
    }

    /// Remove and return a completed result. Returns `None` if the job is
    /// unknown, still running, or ended with a monitor error. Use
    /// [`Self::forget_process`] to clear a failed result entry.
    pub fn take_process_result(&self, id: SupervisorProcessId) -> Option<ManagedProcessResult> {
        let mut jobs = lock_jobs(&self.jobs);
        let result = match &*jobs.get(&id)?.state.borrow() {
            ManagedJobState::Completed(result) => result.clone(),
            ManagedJobState::Running | ManagedJobState::Failed(_) => return None,
        };
        jobs.remove(&id);
        Some(result)
    }

    /// Forget a completed or failed background job result. Active jobs are
    /// retained and return `false`.
    pub fn forget_process(&self, id: SupervisorProcessId) -> bool {
        let mut jobs = lock_jobs(&self.jobs);
        let Some(job) = jobs.get(&id) else {
            return false;
        };
        if matches!(&*job.state.borrow(), ManagedJobState::Running) {
            return false;
        }
        jobs.remove(&id).is_some()
    }

    /// Send bytes to a background job's stdin. Returns a broken-pipe error if
    /// the process has completed or stdin was closed.
    pub async fn write_process_stdin(
        &self,
        id: SupervisorProcessId,
        bytes: &[u8],
    ) -> Result<(), SandboxError> {
        let job = lock_jobs(&self.jobs).get(&id).cloned().ok_or_else(|| {
            SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "unknown background process ID",
            ))
        })?;
        if !matches!(&*job.state.borrow(), ManagedJobState::Running) {
            return Err(SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "background process has completed",
            )));
        }
        let stdin = job.stdin.ok_or_else(|| {
            SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "background process stdin is closed",
            ))
        })?;
        let (reply, response) = oneshot::channel();
        stdin
            .send(ManagedJobCommand::WriteStdin {
                bytes: bytes.to_vec(),
                reply,
            })
            .await
            .map_err(|_| {
                SandboxError::stdin(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "background process monitor has stopped",
                ))
            })?;
        response.await.map_err(|_| {
            SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "background process monitor has stopped",
            ))
        })?
    }

    /// Close a background job's stdin and deliver EOF.
    pub async fn close_process_stdin(&self, id: SupervisorProcessId) -> Result<(), SandboxError> {
        let job = lock_jobs(&self.jobs).get(&id).cloned().ok_or_else(|| {
            SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "unknown background process ID",
            ))
        })?;
        let stdin = job.stdin.ok_or_else(|| {
            SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "background process stdin is closed",
            ))
        })?;
        let (reply, response) = oneshot::channel();
        stdin
            .send(ManagedJobCommand::CloseStdin { reply })
            .await
            .map_err(|_| {
                SandboxError::stdin(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "background process monitor has stopped",
                ))
            })?;
        response.await.map_err(|_| {
            SandboxError::stdin(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "background process monitor has stopped",
            ))
        })?
    }

    /// Gracefully stop every registered process, escalating as needed, then
    /// wait for background monitors to drain output and reap their children.
    /// Results for background jobs are returned and removed from the result
    /// registry. Legacy `start` children are signaled but remain caller-owned;
    /// their `SupervisedChild` handles must still be waited by the caller.
    pub async fn shutdown_all(
        &self,
        grace: Duration,
    ) -> Result<Vec<(SupervisorProcessId, ManagedProcessResult)>, SandboxError> {
        self.agent_runs.cancel_all().await;
        let process_ids = lock_processes(&self.processes)
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let stop_results = join_all(
            process_ids
                .iter()
                .map(|id| self.shutdown_process(*id, grace)),
        )
        .await;
        let mut first_error = stop_results.into_iter().find_map(Result::err);
        if first_error.is_some() {
            // Preserve cleanup even if one backend signal failed; background
            // monitors still need their children terminated before waiting.
            self.terminate_all();
        }

        let job_ids = lock_jobs(&self.jobs).keys().copied().collect::<Vec<_>>();
        let wait_results = join_all(job_ids.iter().map(|id| self.wait_process(*id))).await;
        let mut completed = Vec::new();
        for (id, result) in job_ids.into_iter().zip(wait_results) {
            match result {
                Ok(Some(result)) => completed.push((id, result)),
                Ok(None) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        for (id, _) in &completed {
            self.take_process_result(*id);
        }
        Ok(completed)
    }

    pub fn processes(&self) -> Vec<ProcessInfo> {
        lock_processes(&self.processes)
            .values()
            .map(|process| {
                let process_id = process.process_id;
                let container = &process.container;
                let member = container.members().ok().and_then(|mut members| {
                    members
                        .drain(..)
                        .find(|member| member.process_id == process_id)
                });
                ProcessInfo {
                    process_id,
                    alive: container.is_alive(),
                    parent_process_id: member.as_ref().and_then(|m| m.parent_process_id),
                    start_time_ticks: member.as_ref().and_then(|m| m.start_time_ticks),
                    executable: member.and_then(|m| m.executable),
                }
            })
            .collect()
    }

    /// Look up one process by its stable supervisor-scoped identity.
    pub fn process(&self, id: SupervisorProcessId) -> Option<ManagedProcessInfo> {
        let process = lock_processes(&self.processes).get(&id).cloned()?;
        let member = process.container.members().ok().and_then(|mut members| {
            members
                .drain(..)
                .find(|member| member.process_id == process.process_id)
        });
        Some(ManagedProcessInfo {
            supervisor_id: id,
            process_id: process.process_id,
            alive: process.container.is_alive(),
            parent_process_id: member.as_ref().and_then(|member| member.parent_process_id),
            start_time_ticks: member.as_ref().and_then(|member| member.start_time_ticks),
            executable: member.and_then(|member| member.executable),
        })
    }

    /// Return snapshots of every process currently registered with this
    /// supervisor. Entries remain registered until their child handles reap
    /// the corresponding processes.
    pub fn managed_processes(&self) -> Vec<ManagedProcessInfo> {
        let ids = lock_processes(&self.processes)
            .keys()
            .copied()
            .collect::<Vec<_>>();
        ids.into_iter().filter_map(|id| self.process(id)).collect()
    }

    /// Request graceful shutdown of one registered process tree, then force
    /// stop it if the container still reports it alive after `grace`. This
    /// does not stop other processes managed by this supervisor. A stop result
    /// reports which request was sent; the child handle remains responsible
    /// for observing exit, reaping the process, and collecting its receipt.
    /// Windows has no graceful signal in this backend, so it sends a force-stop
    /// request immediately and does not wait for `grace`.
    pub async fn shutdown_process(
        &self,
        id: SupervisorProcessId,
        grace: Duration,
    ) -> Result<ProcessStopResult, SandboxError> {
        let Some(process) = lock_processes(&self.processes).get(&id).cloned() else {
            return Ok(ProcessStopResult::NotFound);
        };
        let _shutdown_guard = process.shutdown_lock.lock().await;
        if process.stop_requested.load(Ordering::Acquire) {
            return Ok(ProcessStopResult::AlreadyStopping);
        }
        if !process.container.is_alive() {
            return Ok(ProcessStopResult::AlreadyExited);
        }

        #[cfg(windows)]
        {
            let _ = grace;
            process
                .container
                .terminate()
                .map_err(SandboxError::execution)?;
            process.stop_requested.store(true, Ordering::Release);
            Ok(ProcessStopResult::ForceStopRequested)
        }

        #[cfg(not(windows))]
        {
            process
                .container
                .request_graceful_stop()
                .map_err(SandboxError::execution)?;
            let deadline = tokio::time::Instant::now() + grace;
            while process.container.is_alive() {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10).min(deadline - now)).await;
            }
            if !process.container.is_alive() {
                process.stop_requested.store(true, Ordering::Release);
                Ok(ProcessStopResult::GracefulStopRequested)
            } else {
                process
                    .container
                    .terminate()
                    .map_err(SandboxError::execution)?;
                process.stop_requested.store(true, Ordering::Release);
                Ok(ProcessStopResult::ForceStopRequested)
            }
        }
    }

    fn register(
        &self,
        process_id: Option<u32>,
        container: Arc<ProcessContainer>,
        executable: ExecutableIdentity,
    ) -> Option<ProcessLease> {
        let process_id = process_id?;
        let supervisor_id = self.allocate_process_id();
        if let Some(ids) = &self.owned_process_ids {
            lock_process_ids(ids).insert(supervisor_id);
        }
        lock_processes(&self.processes).insert(
            supervisor_id,
            RegisteredProcess {
                process_id,
                container: Arc::clone(&container),
                stop_requested: Arc::new(AtomicBool::new(false)),
                shutdown_lock: Arc::new(AsyncMutex::new(())),
            },
        );
        let _ = self.events.send(crate::LifecycleEvent::Started {
            process_id,
            executable,
        });
        Some(ProcessLease {
            supervisor_id,
            process_id,
            container,
            processes: Arc::clone(&self.processes),
            armed: true,
            events: self.events.clone(),
        })
    }

    pub fn terminate_all(&self) {
        if let Some(ids) = self.owned_process_ids() {
            self.terminate_owned_now(&ids);
            return;
        }
        self.agent_runs.cancel_all_now();
        let containers = {
            let mut set = lock_processes(&self.processes);
            std::mem::take(&mut *set)
                .into_values()
                .map(|process| process.container)
                .collect::<Vec<_>>()
        };
        let _ = self.events.send(crate::LifecycleEvent::ShutdownRequested);
        for container in containers {
            let _ = container.terminate();
        }
    }

    /// Ask registered process groups to exit, wait up to `grace`, then kill
    /// any groups that remain alive.
    pub async fn terminate_all_with_grace(&self, grace: Duration) {
        if let Some(ids) = self.owned_process_ids() {
            self.terminate_owned_with_grace(&ids, grace).await;
            return;
        }
        self.agent_runs.cancel_all().await;
        let containers = {
            let mut set = lock_processes(&self.processes);
            std::mem::take(&mut *set)
                .into_values()
                .map(|process| process.container)
                .collect::<Vec<_>>()
        };
        let _ = self.events.send(crate::LifecycleEvent::ShutdownRequested);
        for container in &containers {
            let _ = container.request_graceful_stop();
        }
        let deadline = tokio::time::Instant::now() + grace;
        while tokio::time::Instant::now() < deadline
            && containers.iter().any(|container| container.is_alive())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for container in containers {
            if container.is_alive() {
                let _ = container.terminate();
            }
        }
    }

    pub(crate) fn terminate_owned_now(&self, ids: &[SupervisorProcessId]) {
        for id in ids {
            if let Some(process) = lock_processes(&self.processes).get(id).cloned() {
                let _ = process.container.terminate();
            }
        }
        if let Some(owned_ids) = &self.owned_process_ids {
            let mut owned_ids = lock_process_ids(owned_ids);
            for id in ids {
                owned_ids.remove(id);
            }
        }
    }

    pub(crate) async fn terminate_owned_with_grace(
        &self,
        ids: &[SupervisorProcessId],
        grace: Duration,
    ) {
        let stop_results = join_all(ids.iter().map(|id| self.shutdown_process(*id, grace))).await;
        if stop_results.iter().any(Result::is_err) {
            self.terminate_owned_now(ids);
        }
        let job_ids = ids
            .iter()
            .copied()
            .filter(|id| lock_jobs(&self.jobs).contains_key(id))
            .collect::<Vec<_>>();
        let results = join_all(job_ids.iter().map(|id| self.wait_process(*id))).await;
        for (id, result) in job_ids.into_iter().zip(results) {
            if result.is_ok() {
                self.take_process_result(id);
            }
        }
        if let Some(owned_ids) = &self.owned_process_ids {
            let mut owned_ids = lock_process_ids(owned_ids);
            for id in ids {
                owned_ids.remove(id);
            }
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
        if self.shutdown_on_drop {
            self.terminate_all();
            self.log.close();
        }
    }
}

struct ProcessLease {
    supervisor_id: SupervisorProcessId,
    process_id: u32,
    container: Arc<ProcessContainer>,
    processes: Arc<Mutex<BTreeMap<SupervisorProcessId, RegisteredProcess>>>,
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
        lock_processes(&self.processes).remove(&self.supervisor_id);
    }
}

impl Drop for ProcessLease {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.container.terminate();
            lock_processes(&self.processes).remove(&self.supervisor_id);
        }
    }
}

fn lock_processes(
    processes: &Mutex<BTreeMap<SupervisorProcessId, RegisteredProcess>>,
) -> MutexGuard<'_, BTreeMap<SupervisorProcessId, RegisteredProcess>> {
    processes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_jobs(
    jobs: &Mutex<BTreeMap<SupervisorProcessId, ManagedJobControl>>,
) -> MutexGuard<'_, BTreeMap<SupervisorProcessId, ManagedJobControl>> {
    jobs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_process_ids(
    ids: &Mutex<BTreeSet<SupervisorProcessId>>,
) -> MutexGuard<'_, BTreeSet<SupervisorProcessId>> {
    ids.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Append `bytes`, keeping only the newest `limit` bytes. Returns whether any
/// output was discarded.
fn retain_newest(output: &mut Vec<u8>, bytes: &[u8], limit: usize) -> bool {
    output.extend_from_slice(bytes);
    let excess = output.len().saturating_sub(limit);
    output.drain(..excess);
    excess > 0
}

async fn run_managed_job(
    id: SupervisorProcessId,
    mut stream: crate::ProcessEventStream,
    (stdout_limit, stderr_limit): (usize, usize),
    jobs: Arc<Mutex<BTreeMap<SupervisorProcessId, ManagedJobControl>>>,
    state: watch::Sender<ManagedJobState>,
) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut output_truncated = false;
    loop {
        tokio::select! {
            event = stream.next() => match event {
                Ok(Some(ProcessEvent::Stdout(bytes))) => {
                    output_truncated |= retain_newest(&mut stdout, &bytes, stdout_limit);
                }
                Ok(Some(ProcessEvent::Stderr(bytes))) => {
                    output_truncated |= retain_newest(&mut stderr, &bytes, stderr_limit);
                }
                Ok(Some(ProcessEvent::Exited(receipt))) => {
                    finish_managed_job(&jobs, id, &state, ManagedJobState::Completed(ManagedProcessResult {
                        stdout,
                        stderr,
                        output_truncated,
                        receipt,
                        error: None,
                    }));
                    return;
                }
                Ok(Some(ProcessEvent::Started { .. })) => {}
                Ok(None) => {
                    finish_managed_job(&jobs, id, &state, ManagedJobState::Failed(
                        "background process event stream ended without an exit receipt".into(),
                    ));
                    return;
                }
                Err(error) => {
                    let message = error.to_string();
                    match stream.wait().await {
                        Ok(receipt) => {
                            finish_managed_job(&jobs, id, &state, ManagedJobState::Completed(ManagedProcessResult {
                                stdout,
                                stderr,
                                output_truncated,
                                receipt,
                                error: Some(message),
                            }));
                        }
                        Err(wait_error) => {
                            finish_managed_job(&jobs, id, &state, ManagedJobState::Failed(format!(
                                "{message}; waiting for child failed: {wait_error}"
                            )));
                        }
                    }
                    return;
                }
            }
        }
    }
}

fn finish_managed_job(
    jobs: &Mutex<BTreeMap<SupervisorProcessId, ManagedJobControl>>,
    id: SupervisorProcessId,
    state: &watch::Sender<ManagedJobState>,
    finished: ManagedJobState,
) {
    if let Some(job) = lock_jobs(jobs).get_mut(&id) {
        job.stdin = None;
    }
    state.send_replace(finished);
}

async fn run_managed_stdin(
    mut stdin: Option<BoxAsyncWrite>,
    mut commands: mpsc::Receiver<ManagedJobCommand>,
) {
    while let Some(command) = commands.recv().await {
        match command {
            ManagedJobCommand::WriteStdin { bytes, reply } => {
                let result = match stdin.as_mut() {
                    Some(writer) => writer.write_all(&bytes).await.map_err(SandboxError::stdin),
                    None => Err(SandboxError::stdin(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "background process stdin is closed",
                    ))),
                };
                if result.is_err() {
                    stdin = None;
                }
                let _ = reply.send(result);
            }
            ManagedJobCommand::CloseStdin { reply } => {
                stdin = None;
                let _ = reply.send(Ok(()));
            }
        }
    }
}

impl SupervisedChild {
    pub fn id(&self) -> Option<u32> {
        self.process_id
    }

    /// Return the stable ID assigned when this child was registered with a
    /// [`Supervisor`]. It remains available after the child exits so callers
    /// can retain the job identity; the supervisor no longer looks it up once
    /// the child handle has reaped it.
    pub fn supervisor_id(&self) -> Option<SupervisorProcessId> {
        self.supervisor_id
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

    /// Let the process write any amount of output. The caller bounds what it
    /// keeps.
    fn remove_output_limits(&mut self) {
        self.output_limit = usize::MAX;
        self.stderr_limit = usize::MAX;
    }

    /// Hand the process's event publishing to its event stream, which reports
    /// the exit only after its output has been read.
    pub(crate) fn take_tap(&mut self) -> Option<ProcessTap> {
        self.tap.take()
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
        let receipt = self.receipt(termination);
        if let Some(tap) = self.tap.take() {
            tap.finish(Some(receipt.clone()));
        }
        receipt
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
    let mut child = spawn_internal(request, Some(supervisor))?;
    if let Some(id) = child.supervisor_id {
        let tap = supervisor.process_tap(id);
        tap.started(child.executable.clone());
        child.tap = Some(tap);
    }
    Ok(child)
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
    let mut command = Command::new(executable);
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
    let lease = supervisor
        .and_then(|value| value.register(process_id, Arc::clone(&container), identity.clone()));
    let supervisor_id = lease.as_ref().map(|lease| lease.supervisor_id);
    Ok(SupervisedChild {
        process_id,
        supervisor_id,
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
        lease,
        owns_container: true,
        tap: None,
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
    let supervisor_id = lease.as_ref().map(|lease| lease.supervisor_id);
    Ok(SupervisedChild {
        process_id,
        supervisor_id,
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
        tap: None,
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
        supervisor_id: None,
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
        tap: None,
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
        supervisor_id: None,
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
        tap: None,
    })
}

pub async fn execute(
    request: &ExecutionRequest,
    input: &[u8],
) -> Result<ExecutionOutput, SandboxError> {
    execute_with_cancellation(request, input, CancellationToken::new()).await
}

/// Run a request with empty stdin and collect bounded output.
pub async fn run(
    command: &crate::Command,
    policy: &crate::Policy,
) -> Result<ExecutionOutput, SandboxError> {
    execute(&command.request(policy), &[]).await
}

pub async fn execute_with_cancellation(
    request: &ExecutionRequest,
    input: &[u8],
    cancellation: CancellationToken,
) -> Result<ExecutionOutput, SandboxError> {
    execute_internal(request, input, cancellation, None).await
}

/// Run a one-shot command and publish its events through `tap`.
pub(crate) async fn execute_with_tap(
    request: &ExecutionRequest,
    input: &[u8],
    cancellation: CancellationToken,
    tap: ProcessTap,
) -> Result<ExecutionOutput, SandboxError> {
    tap.started(validate_request(request, input)?);
    let started_at = SystemTime::now();
    let output = execute_internal(request, input, cancellation, Some(tap.clone())).await;
    tap.finish(output.as_ref().ok().map(|output| ProcessReceipt {
        executable: output.executable.clone(),
        enforcement: output.enforcement,
        started_at,
        finished_at: SystemTime::now(),
        termination: output.termination.clone(),
    }));
    output
}

async fn execute_internal(
    request: &ExecutionRequest,
    input: &[u8],
    cancellation: CancellationToken,
    tap: Option<ProcessTap>,
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
        return execute_windows_with_cancellation(request, input, cancellation, identity, tap)
            .await;
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
        tap.clone(),
    );
    spawn_bounded_reader(
        stderr,
        request.policy.limits.stderr_bytes,
        StreamKind::Stderr,
        stream_tx,
        tap,
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
                    StreamEvent::ReadFailed(StreamKind::Stdout, error) => {
                        return Err(SandboxError::stdout_read(error).with_executable(identity.clone()))
                    }
                    StreamEvent::ReadFailed(StreamKind::Stderr, error) => {
                        return Err(SandboxError::stderr_read(error).with_executable(identity.clone()))
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
                StreamEvent::ReadFailed(_, _) => break,
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
    tap: Option<ProcessTap>,
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
    let stdout_tap = tap.clone();
    let stdout_task = tokio::spawn(read_limited(
        stdout,
        request.policy.limits.output_bytes,
        move |bytes| {
            if let Some(tap) = &stdout_tap {
                tap.stdout(bytes);
            }
        },
    ));
    let stderr_task = tokio::spawn(read_limited(
        stderr,
        request.policy.limits.stderr_bytes,
        move |bytes| {
            if let Some(tap) = &tap {
                tap.stderr(bytes);
            }
        },
    ));
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
async fn read_limited(
    mut reader: BoxAsyncRead,
    limit: usize,
    publish: impl Fn(&[u8]),
) -> std::io::Result<Vec<u8>> {
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
        publish(&chunk[..read]);
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
    let Some(filesystem) = request.policy.filesystem.as_ref() else {
        return Ok(());
    };
    #[cfg(windows)]
    let inherited_directory;
    #[cfg(windows)]
    let working_directory = match request.working_directory.as_ref() {
        Some(directory) => directory,
        None => {
            inherited_directory =
                std::env::current_dir().map_err(SandboxError::invalid_working_directory)?;
            &inherited_directory
        }
    };
    #[cfg(not(windows))]
    let Some(working_directory) = request.working_directory.as_ref() else {
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
    ReadFailed(StreamKind, std::io::Error),
}

fn spawn_bounded_reader(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    limit: usize,
    kind: StreamKind,
    events: mpsc::Sender<StreamEvent>,
    tap: Option<ProcessTap>,
) {
    let publish = move |bytes: &[u8]| match (&tap, kind) {
        (Some(tap), StreamKind::Stdout) => tap.stdout(bytes),
        (Some(tap), StreamKind::Stderr) => tap.stderr(bytes),
        (None, _) => {}
    };
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
                    publish(&chunk[..read]);
                    output.extend_from_slice(&chunk[..read]);
                }
                Ok(read) => {
                    let remaining = limit.saturating_sub(output.len());
                    publish(&chunk[..remaining.min(read)]);
                    output.extend_from_slice(&chunk[..remaining.min(read)]);
                    let _ = events.send(StreamEvent::Limit(kind, output)).await;
                    return;
                }
                Err(error) => {
                    let _ = events.send(StreamEvent::ReadFailed(kind, error)).await;
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

    #[cfg(windows)]
    #[tokio::test]
    async fn rejects_ungranted_inherited_working_directory() {
        let mut request = request("C:\\Windows\\System32\\cmd.exe");
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
    async fn supervisor_ids_are_stable_and_processes_can_be_stopped_individually() {
        let supervisor = Supervisor::new();
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];

        let first = spawn_with_supervisor(&request, &supervisor).unwrap();
        let first_id = first.supervisor_id().expect("registered child has an ID");
        let mut second = spawn_with_supervisor(&request, &supervisor).unwrap();
        let second_id = second.supervisor_id().expect("registered child has an ID");

        assert_ne!(first_id, second_id);
        assert_ne!(first_id.get(), u64::from(first.id().unwrap()));
        let first_info = supervisor
            .process(first_id)
            .expect("first child is registered");
        assert!(first_info.alive);
        assert_eq!(first_info.process_id, first.id().unwrap());
        assert_eq!(supervisor.managed_processes().len(), 2);

        assert_eq!(
            supervisor
                .shutdown_process(first_id, Duration::from_millis(50))
                .await
                .unwrap(),
            ProcessStopResult::GracefulStopRequested
        );
        assert!(!supervisor.process(first_id).unwrap().alive);
        assert!(supervisor.process(second_id).unwrap().alive);
        assert!(second.try_wait().await.unwrap().is_none());

        assert_eq!(
            supervisor
                .shutdown_process(first_id, Duration::ZERO)
                .await
                .unwrap(),
            ProcessStopResult::AlreadyStopping
        );
        assert_eq!(
            supervisor
                .shutdown_process(
                    SupervisorProcessId {
                        supervisor_id: Uuid::new_v4(),
                        sequence: u64::MAX,
                    },
                    Duration::ZERO,
                )
                .await
                .unwrap(),
            ProcessStopResult::NotFound
        );

        let first_receipt = first.wait().await.unwrap();
        assert!(matches!(
            first_receipt.termination,
            TerminationReason::Exited {
                signal: Some(_),
                ..
            }
        ));
        assert!(supervisor.process(first_id).is_none());

        let second_receipt = supervisor
            .shutdown_process(second_id, Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(second_receipt, ProcessStopResult::GracefulStopRequested);
        second.wait().await.unwrap();
        assert_eq!(supervisor.active_processes(), 0);
    }

    #[tokio::test]
    async fn supervisor_process_id_round_trips_and_resolves_in_its_supervisor() {
        let supervisor = Supervisor::new();
        let mut request = request("/bin/sleep");
        request.args = vec!["30".into()];
        let child = spawn_with_supervisor(&request, &supervisor).unwrap();
        let id = child.supervisor_id().expect("registered child has an ID");

        let token = id.to_string();
        assert!(token.starts_with("v1:"));
        let parsed: SupervisorProcessId = token.parse().unwrap();
        assert_eq!(parsed, id);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{token}\""));
        let from_json: SupervisorProcessId = serde_json::from_str(&json).unwrap();
        assert_eq!(from_json, id);
        assert!(supervisor.process(parsed).is_some());

        supervisor
            .shutdown_process(id, Duration::from_millis(50))
            .await
            .unwrap();
        child.wait().await.unwrap();
    }

    #[tokio::test]
    async fn process_id_from_another_supervisor_cannot_resolve_or_stop_child() {
        let old_command = crate::Command {
            argv: vec!["/bin/sh".into(), "-c".into(), "printf old".into()],
            cwd: None,
        };
        let new_command = crate::Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };

        let old_supervisor = Supervisor::new();
        let old_id = old_supervisor
            .start_background(&old_command, &crate::Policy::default())
            .unwrap();
        let old_result = old_supervisor.wait_process(old_id).await.unwrap().unwrap();
        assert_eq!(old_result.stdout, b"old");
        assert!(old_supervisor.process_result(old_id).is_some());

        let new_supervisor = Supervisor::new();
        let new_id = new_supervisor
            .start_background(&new_command, &crate::Policy::default())
            .unwrap();
        assert_eq!(
            old_id.get(),
            new_id.get(),
            "local counters should be reused"
        );
        assert_ne!(old_id, new_id, "instance identity must distinguish IDs");
        assert!(new_supervisor.process(old_id).is_none());
        assert!(new_supervisor.process_result(old_id).is_none());
        assert_eq!(
            new_supervisor
                .shutdown_process(old_id, Duration::ZERO)
                .await
                .unwrap(),
            ProcessStopResult::NotFound
        );
        assert_eq!(
            new_supervisor.process_status(new_id),
            Some(ManagedProcessStatus::Running)
        );

        new_supervisor
            .shutdown_process(new_id, Duration::from_millis(50))
            .await
            .unwrap();
        new_supervisor.wait_process(new_id).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn shutdown_process_serializes_concurrent_requests_and_force_stops_child() {
        let supervisor = Supervisor::new();
        let mut request = request("/bin/sh");
        request.args = vec![
            "-c".into(),
            "trap '' TERM; printf 'ready\\n'; while :; do /bin/sleep 1; done".into(),
        ];
        let mut child = spawn_with_supervisor(&request, &supervisor).unwrap();
        let id = child.supervisor_id().unwrap();
        let mut stdout = child.take_stdout().unwrap();
        let mut ready = [0; 6];
        stdout.read_exact(&mut ready).await.unwrap();
        assert_eq!(&ready, b"ready\n");

        let (first_stop, concurrent_stop) = tokio::join!(
            supervisor.shutdown_process(id, Duration::from_millis(50)),
            supervisor.shutdown_process(id, Duration::from_millis(50)),
        );
        let outcomes = [first_stop.unwrap(), concurrent_stop.unwrap()];
        assert!(outcomes.contains(&ProcessStopResult::ForceStopRequested));
        assert!(outcomes.contains(&ProcessStopResult::AlreadyStopping));
        let receipt = child.wait().await.unwrap();
        assert!(matches!(
            receipt.termination,
            TerminationReason::Exited {
                signal: Some(9),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn background_job_captures_output_and_retains_consumable_result() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf 'out'; printf 'err' >&2".into(),
            ],
            cwd: None,
        };
        let id = supervisor
            .start_background(&command, &crate::Policy::default())
            .unwrap();

        let result = supervisor.wait_process(id).await.unwrap().unwrap();
        assert_eq!(result.stdout, b"out");
        assert_eq!(result.stderr, b"err");
        assert_eq!(
            result.receipt.termination,
            TerminationReason::Exited {
                code: Some(0),
                signal: None
            }
        );
        assert_eq!(result.error, None);
        assert_eq!(
            supervisor.process_status(id),
            Some(ManagedProcessStatus::Completed)
        );
        assert_eq!(supervisor.process_result(id), Some(result.clone()));
        assert_eq!(supervisor.take_process_result(id), Some(result));
        assert_eq!(supervisor.process_status(id), None);
        assert_eq!(supervisor.wait_process(id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn background_stdin_write_drains_child_output_concurrently() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec!["/bin/cat".into()],
            cwd: None,
        };
        let mut policy = crate::Policy::default();
        policy.limits.input_bytes = 1024 * 1024;
        policy.limits.output_bytes = 1024 * 1024;
        let id = supervisor.start_background(&command, &policy).unwrap();
        let input = vec![b'x'; 512 * 1024];

        tokio::time::timeout(
            Duration::from_secs(5),
            supervisor.write_process_stdin(id, &input),
        )
        .await
        .expect("stdin write should progress while stdout is drained")
        .unwrap();
        supervisor.close_process_stdin(id).await.unwrap();
        let result = supervisor.wait_process(id).await.unwrap().unwrap();
        assert_eq!(result.stdout, input);
        assert_eq!(
            result.receipt.termination,
            TerminationReason::Exited {
                code: Some(0),
                signal: None
            }
        );
    }

    #[tokio::test]
    async fn background_output_keeps_the_newest_bytes_without_stopping_the_job() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec!["/usr/bin/yes".into()],
            cwd: None,
        };
        let mut policy = crate::Policy::default();
        policy.limits.output_bytes = 1024;
        policy.limits.stderr_bytes = 128;
        let id = supervisor.start_background(&command, &policy).unwrap();

        // `yes` passes its limit almost immediately and must keep running.
        sleep(Duration::from_millis(200)).await;
        assert_eq!(
            supervisor.process_status(id),
            Some(ManagedProcessStatus::Running)
        );
        supervisor
            .shutdown_process(id, Duration::from_millis(100))
            .await
            .unwrap();
        let result = supervisor.wait_process(id).await.unwrap().unwrap();
        assert_eq!(result.stdout.len(), 1024);
        assert!(result.output_truncated);
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn background_shutdown_is_individual_and_shutdown_all_waits_for_reaping() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let policy = crate::Policy::default();
        let first = supervisor.start_background(&command, &policy).unwrap();
        let second = supervisor.start_background(&command, &policy).unwrap();

        supervisor
            .shutdown_process(first, Duration::from_millis(50))
            .await
            .unwrap();
        let first_result = supervisor.wait_process(first).await.unwrap().unwrap();
        assert!(matches!(
            first_result.receipt.termination,
            TerminationReason::Exited {
                signal: Some(_),
                ..
            }
        ));
        assert_eq!(
            supervisor.process_status(second),
            Some(ManagedProcessStatus::Running)
        );
        assert!(supervisor.process(second).unwrap().alive);

        let all_results = supervisor
            .shutdown_all(Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(all_results.len(), 2);
        assert!(all_results.iter().any(|(id, _)| *id == first));
        assert!(all_results.iter().any(|(id, _)| *id == second));
        let second_result = all_results.iter().find(|(id, _)| *id == second).unwrap();
        assert!(matches!(
            second_result.1.receipt.termination,
            TerminationReason::Exited {
                signal: Some(_),
                ..
            }
        ));
        assert_eq!(supervisor.process_status(first), None);
        assert_eq!(supervisor.process_status(second), None);
        assert_eq!(supervisor.active_processes(), 0);
    }

    #[tokio::test]
    async fn dropping_supervisor_kills_and_reaps_background_jobs() {
        let pid = {
            let supervisor = Supervisor::new();
            let command = crate::Command {
                argv: vec!["/bin/sleep".into(), "30".into()],
                cwd: None,
            };
            let id = supervisor
                .start_background(&command, &crate::Policy::default())
                .unwrap();
            supervisor.process(id).unwrap().process_id
        };

        tokio::time::timeout(Duration::from_secs(2), async {
            while unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background child should be reaped after its supervisor is dropped");
    }

    #[tokio::test]
    async fn dropping_supervisor_kills_background_descendants() {
        let directory = tempfile::tempdir().unwrap();
        let ready = directory.path().join("descendant-ready");
        let marker = directory.path().join("descendant-survived");
        let command = crate::Command {
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                format!(
                    "(/bin/sh -c 'sleep 0.4; printf alive > \"{}\"') & printf ready > \"{}\"; wait",
                    marker.display(),
                    ready.display(),
                ),
            ],
            cwd: None,
        };
        {
            let supervisor = Supervisor::new();
            supervisor
                .start_background(&command, &crate::Policy::default())
                .unwrap();

            tokio::time::timeout(Duration::from_secs(2), async {
                while !ready.exists() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("child process should have started before owner drop");
        }

        sleep(Duration::from_millis(700)).await;
        assert!(
            !marker.exists(),
            "a descendant must not outlive its dropped Supervisor"
        );
    }

    #[test]
    fn background_start_requires_a_tokio_runtime() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec!["/bin/true".into()],
            cwd: None,
        };
        let error = supervisor
            .start_background(&command, &crate::Policy::default())
            .unwrap_err();
        assert_eq!(error.code(), "unsupported");
        assert!(error.message().contains("Tokio runtime"));
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

#[cfg(all(test, windows))]
mod windows_supervisor_tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn shutdown_process_uses_immediate_force_stop_without_graceful_signal() {
        let supervisor = Supervisor::new();
        let request = crate::ExecutionRequest {
            executable: PathBuf::from(r"C:\Windows\System32\timeout.exe"),
            args: vec!["/t".into(), "30".into(), "/nobreak".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let child = spawn_with_supervisor(&request, &supervisor).unwrap();
        let id = child.supervisor_id().unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            supervisor.shutdown_process(id, Duration::from_secs(30)),
        )
        .await
        .expect("Windows shutdown should not wait for the unsupported grace period")
        .unwrap();
        assert_eq!(result, ProcessStopResult::ForceStopRequested);
        child.wait().await.unwrap();
    }

    #[tokio::test]
    async fn background_job_drains_output_and_returns_a_reaped_result() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec![
                r"C:\Windows\System32\cmd.exe".into(),
                "/D".into(),
                "/C".into(),
                "echo managed".into(),
            ],
            cwd: None,
        };
        let id = supervisor
            .start_background(&command, &crate::Policy::default())
            .unwrap();
        let result = supervisor.wait_process(id).await.unwrap().unwrap();
        assert!(String::from_utf8_lossy(&result.stdout).contains("managed"));
        assert!(matches!(
            result.receipt.termination,
            TerminationReason::Exited { code: Some(0), .. }
        ));
    }
}
