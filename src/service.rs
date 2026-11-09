//! Authenticated per-user Unix socket service for a long-lived [`Supervisor`].
//!
//! The service owns process event streams and continues draining/reaping when
//! clients disconnect. Messages are UTF-8 JSON objects delimited by newlines.
//! Every connection must begin with `{"type":"auth","token":"..."}`.

use crate::coordinator::{CoordinatorError, CoordinatorEvent, CoordinatorHandle, ExecutionStatus};
use crate::coordinator_store::{
    CommandReservation, CoordinatorStore, RecoveryState, ReservationOutcome,
};
use crate::workflow::{AttemptOutcome, FlowRunProjection, WorkflowDispatcher};
use crate::{Command, ManagedProcessMetadata, ProcessEvent, Supervisor, SupervisorProcessId};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, watch};

const MAX_FRAME: usize = 1024 * 1024;
const EVENT_HISTORY: usize = 256;
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const MAX_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_OUTPUT_CAPABILITIES: usize = 4096;
const OUTPUT_CAPABILITY_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientRole {
    View,
    Control,
    Tui,
    Reporter,
    Canceller,
    OutputReader,
}

impl ClientRole {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "view" => Some(Self::View),
            "control" => Some(Self::Control),
            "tui" => Some(Self::Tui),
            "reporter" => Some(Self::Reporter),
            "canceller" => Some(Self::Canceller),
            "output" => Some(Self::OutputReader),
            _ => None,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Control => "control",
            Self::Tui => "tui",
            Self::Reporter => "reporter",
            Self::Canceller => "canceller",
            Self::OutputReader => "output",
        }
    }
}

#[derive(Clone)]
struct RoleRevocations {
    view: watch::Sender<bool>,
    tui: watch::Sender<bool>,
}

impl RoleRevocations {
    fn new() -> Self {
        let (view, _) = watch::channel(false);
        let (tui, _) = watch::channel(false);
        Self { view, tui }
    }
    fn subscribe(&self, role: ClientRole) -> Option<watch::Receiver<bool>> {
        match role {
            ClientRole::View => Some(self.view.subscribe()),
            ClientRole::Tui => Some(self.tui.subscribe()),
            ClientRole::Reporter => None,
            ClientRole::Canceller => None,
            ClientRole::OutputReader => None,
            ClientRole::Control => None,
        }
    }
    fn revoked(&self, role: ClientRole) -> bool {
        match role {
            ClientRole::View => *self.view.borrow(),
            ClientRole::Tui => *self.tui.borrow(),
            ClientRole::Control => false,
            ClientRole::Reporter => false,
            ClientRole::Canceller => false,
            ClientRole::OutputReader => false,
        }
    }
    fn revoke(&self, role: ClientRole) -> bool {
        match role {
            ClientRole::View => {
                self.view.send_replace(true);
                true
            }
            ClientRole::Tui => {
                self.tui.send_replace(true);
                true
            }
            ClientRole::Control => false,
            ClientRole::Reporter => false,
            ClientRole::Canceller => false,
            ClientRole::OutputReader => false,
        }
    }
}

struct Managed {
    events: broadcast::Sender<Value>,
    history: Arc<Mutex<VecDeque<Value>>>,
    input: mpsc::Sender<InputCommand>,
}

#[derive(Clone)]
struct AttemptCapability {
    attempt_id: String,
    generation: u64,
}

#[derive(Default)]
struct CapabilityRegistry {
    reporter_tokens: HashMap<Vec<u8>, AttemptCapability>,
    cancel_tokens: HashMap<Vec<u8>, AttemptCapability>,
    output_tokens: HashMap<Vec<u8>, OutputCapability>,
}

struct OutputCapability {
    process_id: String,
    issued_at: Instant,
    expires_at: Instant,
}

enum InputCommand {
    Write(Vec<u8>),
    Close,
}

#[derive(Clone)]
pub struct SupervisorService {
    supervisor: Arc<Supervisor>,
    view_token: Arc<Vec<u8>>,
    control_token: Arc<Vec<u8>>,
    tui_token: Arc<Vec<u8>>,
    processes: Arc<Mutex<HashMap<SupervisorProcessId, Managed>>>,
    session_ttl: Duration,
    revocations: RoleRevocations,
    coordinator: Arc<OnceLock<CoordinatorHandle>>,
    state_dir: Option<PathBuf>,
    instance_lock: Arc<OnceLock<Arc<ServiceInstanceLock>>>,
    recovery: Arc<Mutex<Option<RecoveryState>>>,
    capabilities: Arc<Mutex<CapabilityRegistry>>,
    shutdown: watch::Sender<bool>,
    manager_id: String,
    started_at: std::time::Instant,
    flowboard_mcp: Option<crate::flowboard_mcp::FlowboardMcpConfig>,
    workflow_dispatcher: Option<Arc<dyn WorkflowDispatcher>>,
    workflow_worker: Option<crate::workflow_dispatcher::RuntimeWorkerConfig>,
    workflow_invocations: Arc<Mutex<HashMap<(String, String), Value>>>,
}

/// Hold the single-owner lock for one configured service state directory.
/// The file is intentionally retained after exit; the advisory lock, not file
/// existence, identifies a live owner.
pub struct ServiceInstanceLock {
    _file: std::fs::File,
}

/// Acquire the one-service-per-state-directory lock. The directory must be
/// owned by the current user, or be a sticky shared system directory such as
/// `/tmp`; group-writable non-sticky directories are rejected.
pub fn acquire_instance_lock(state_dir: impl AsRef<Path>) -> io::Result<ServiceInstanceLock> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let state_dir = state_dir.as_ref();
    std::fs::create_dir_all(state_dir)?;
    let dir_metadata = std::fs::symlink_metadata(state_dir)?;
    let current_uid = unsafe { libc::geteuid() };
    let dir_mode = dir_metadata.permissions().mode();
    let sticky_shared = dir_metadata.uid() == 0 && dir_mode & 0o1000 != 0;
    if !dir_metadata.file_type().is_dir()
        || (dir_metadata.uid() != current_uid && !sticky_shared)
        || (dir_mode & 0o022 != 0 && !sticky_shared)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "service state directory must be a current-user directory or sticky shared directory",
        ));
    }
    let lock_path = state_dir.join(".agent-supervisor-service.lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&lock_path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.uid() != current_uid
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "service lock must be a current-user mode-0600 regular file",
        ));
    }
    // SAFETY: flock operates on the valid open file descriptor and has no
    // memory-safety preconditions. LOCK_NB fails instead of waiting for owner.
    if unsafe {
        libc::flock(
            std::os::fd::AsRawFd::as_raw_fd(&file),
            libc::LOCK_EX | libc::LOCK_NB,
        )
    } != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another Supervisor service owns this state directory",
        ));
    }
    Ok(ServiceInstanceLock { _file: file })
}

impl SupervisorService {
    pub fn new(
        supervisor: Arc<Supervisor>,
        view_token: impl AsRef<[u8]>,
        control_token: impl AsRef<[u8]>,
        tui_token: impl AsRef<[u8]>,
    ) -> io::Result<Self> {
        let view_token = view_token.as_ref();
        let control_token = control_token.as_ref();
        let tui_token = tui_token.as_ref();
        if view_token.len() < 32
            || control_token.len() < 32
            || tui_token.len() < 32
            || constant_time_eq(view_token, control_token)
            || constant_time_eq(view_token, tui_token)
            || constant_time_eq(control_token, tui_token)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "view, control, and TUI tokens must be distinct and each contain at least 32 bytes",
            ));
        }
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            supervisor,
            view_token: Arc::new(view_token.to_vec()),
            control_token: Arc::new(control_token.to_vec()),
            tui_token: Arc::new(tui_token.to_vec()),
            processes: Arc::new(Mutex::new(HashMap::new())),
            session_ttl: DEFAULT_SESSION_TTL,
            revocations: RoleRevocations::new(),
            coordinator: Arc::new(OnceLock::new()),
            state_dir: None,
            instance_lock: Arc::new(OnceLock::new()),
            recovery: Arc::new(Mutex::new(None)),
            capabilities: Arc::new(Mutex::new(CapabilityRegistry::default())),
            shutdown,
            manager_id: new_id(),
            started_at: std::time::Instant::now(),
            flowboard_mcp: None,
            workflow_dispatcher: None,
            workflow_worker: None,
            workflow_invocations: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Configure a service-owned Flowboard MCP stdio connection. The worker
    /// is started by the Supervisor service and drains its durable outbox; no
    /// Desktop callback or connection is involved.
    pub fn with_flowboard_mcp(
        mut self,
        command: impl AsRef<Path>,
        args: Vec<String>,
        env: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.flowboard_mcp = Some(crate::flowboard_mcp::FlowboardMcpConfig {
            command: command.as_ref().to_path_buf(),
            args,
            env,
            cwd: None,
        });
        self
    }

    /// Install a service-owned FlowRun worker dispatcher. The scheduler and
    /// its worker attachments live with the Supervisor service process.
    pub(crate) fn with_workflow_dispatcher(
        mut self,
        dispatcher: Arc<dyn WorkflowDispatcher>,
    ) -> Self {
        self.workflow_dispatcher = Some(dispatcher);
        self
    }

    /// Configure the service-owned Roo Runtime worker executable. Task
    /// invocation bodies arrive through authenticated FlowRun registration and
    /// remain memory-only; they are never copied into the coordinator journal.
    pub fn with_workflow_worker(
        mut self,
        command: impl AsRef<Path>,
        args: Vec<String>,
        cwd: Option<PathBuf>,
        env: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.workflow_worker = Some(crate::workflow_dispatcher::RuntimeWorkerConfig {
            executable: command.as_ref().to_path_buf(),
            args,
            cwd,
            environment: crate::EnvironmentPolicy {
                variables: env,
                ..crate::EnvironmentPolicy::default()
            },
        });
        self
    }

    /// Set an injectable, bounded authenticated-connection lifetime.
    pub fn with_session_ttl(mut self, ttl: Duration) -> io::Result<Self> {
        if ttl.is_zero() || ttl > MAX_SESSION_TTL {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session TTL must be between 1ns and 24h",
            ));
        }
        self.session_ttl = ttl;
        Ok(self)
    }

    /// Configure durable coordinator state beneath a service-owned directory.
    /// The instance lock is acquired and the journal opened before the socket
    /// becomes ready; embedded callers may omit this to use in-memory state.
    pub fn with_state_dir(mut self, state_dir: impl AsRef<Path>) -> Self {
        self.state_dir = Some(state_dir.as_ref().to_path_buf());
        self
    }

    fn coordinator(&self) -> CoordinatorHandle {
        self.coordinator
            .get_or_init(|| CoordinatorHandle::spawn(self.manager_id.clone()))
            .clone()
    }

    fn recovery_summary(&self) -> Value {
        self.recovery
            .lock()
            .unwrap()
            .as_ref()
            .map(|recovery| {
                json!({
                    "previousEpoch": recovery.previous_epoch,
                    "attempts": recovery.recovered_attempts,
                    "diagnostics": recovery.diagnostics,
                    "pendingCommandReservations": recovery.reservations.iter().map(|reservation| json!({
                        "requestId": reservation.request_id,
                        "command": reservation.command,
                        "reservedSequence": reservation.reserved_sequence,
                        "outcome": "unknown",
                    })).collect::<Vec<_>>(),
                })
            })
            .unwrap_or(Value::Null)
    }

    fn workflow_dispatch_summary(&self) -> Value {
        let enabled = self.workflow_dispatcher.is_some() || self.workflow_worker.is_some();
        let snapshot = self.coordinator().snapshot();
        let invocations = self.workflow_invocations.lock().unwrap();
        let missing = snapshot
            .flow_runs
            .iter()
            .flat_map(|flow| {
                flow.ready_tasks().into_iter().filter_map(|task_id| {
                    (!invocations.contains_key(&(flow.flow_id.clone(), task_id.clone())))
                        .then(|| json!({"flowId":flow.flow_id,"taskId":task_id}))
                })
            })
            .collect::<Vec<_>>();
        let pending_reason = if !enabled {
            Some("No service-owned Roo Runtime worker adapter is configured")
        } else if !missing.is_empty() {
            Some("Ready tasks need their transient invocations re-registered after reconnect")
        } else {
            None
        };
        json!({
            "enabled":enabled,
            "pendingReason":pending_reason,
            "invocationRefreshRequired":!missing.is_empty(),
            "missingInvocations":missing,
        })
    }

    fn note_reservation(&self, request_id: &str, command: Value, sequence: u64) {
        if let Some(recovery) = self.recovery.lock().unwrap().as_mut() {
            recovery
                .reservations
                .retain(|reservation| reservation.request_id != request_id);
            recovery.reservations.push_back(CommandReservation {
                request_id: request_id.to_owned(),
                command,
                reserved_sequence: sequence,
            });
            recovery.diagnostics.pending_command_reservations = recovery.reservations.len();
        }
    }

    fn complete_reservation(&self, request_id: &str) {
        if let Some(recovery) = self.recovery.lock().unwrap().as_mut() {
            recovery
                .reservations
                .retain(|reservation| reservation.request_id != request_id);
            recovery.diagnostics.pending_command_reservations = recovery.reservations.len();
        }
    }

    /// Bind a private socket and serve authenticated client connections.
    pub async fn serve_unix(self, socket_path: impl AsRef<Path>) -> io::Result<()> {
        self.serve_unix_with_ready(socket_path, None).await
    }

    /// Serve the Unix socket and optionally publish a private readiness file
    /// only after bind and socket permission setup complete.
    pub async fn serve_unix_with_ready(
        self,
        socket_path: impl AsRef<Path>,
        ready_file: Option<&Path>,
    ) -> io::Result<()> {
        if let Some(state_dir) = self.state_dir.as_ref() {
            let lock = Arc::new(acquire_instance_lock(state_dir)?);
            self.instance_lock.set(lock).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "service state directory is already initialized",
                )
            })?;
            let journal_path = state_dir.join("coordinator-v1.jsonl");
            let (store, recovery) =
                tokio::task::spawn_blocking(move || CoordinatorStore::open(journal_path))
                    .await
                    .map_err(io::Error::other)?
                    .map_err(io::Error::other)?;
            let coordinator = CoordinatorHandle::spawn_persistent(
                self.manager_id.clone(),
                store,
                recovery.clone(),
            );
            coordinator.wait_ready().await.map_err(|error| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("coordinator recovery could not be durably committed: {error}"),
                )
            })?;
            self.coordinator.set(coordinator).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "service coordinator was initialized before startup",
                )
            })?;
            *self.recovery.lock().unwrap() = Some(recovery);
        }
        if self.flowboard_mcp.is_some() && self.state_dir.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Flowboard MCP outbox requires a persistent --state-dir",
            ));
        }
        if let Some(worker) = &self.workflow_worker {
            if !worker.executable.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "workflow worker executable must be an absolute path",
                ));
            }
            if worker.cwd.as_ref().is_some_and(|cwd| !cwd.is_absolute()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "workflow worker cwd must be an absolute path",
                ));
            }
            crate::validate_environment_policy(&worker.environment).map_err(io::Error::other)?;
        }
        let socket_path = socket_path.as_ref();
        if socket_path.exists() {
            use std::os::unix::fs::{FileTypeExt, MetadataExt};
            let metadata = std::fs::symlink_metadata(socket_path)?;
            // SAFETY: geteuid has no preconditions and reads the effective UID.
            let current_uid = unsafe { libc::geteuid() };
            if !metadata.file_type().is_socket() || metadata.uid() != current_uid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to remove a non-socket or another user's path",
                ));
            }
            if UnixStream::connect(socket_path).await.is_ok() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another Supervisor service is already accepting this socket",
                ));
            }
            std::fs::remove_file(socket_path)?;
        }
        let listener = UnixListener::bind(socket_path)?;
        std::fs::set_permissions(
            socket_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )?;
        let ready_identity = if let Some(path) = ready_file {
            match write_ready_file(path, socket_path, &self.manager_id) {
                Ok(identity) => Some(identity),
                Err(error) => {
                    drop(listener);
                    let _ = std::fs::remove_file(socket_path);
                    return Err(error);
                }
            }
        } else {
            None
        };
        let outbox_task = self.flowboard_mcp.clone().map(|config| {
            let sink = crate::flowboard_mcp::McpStdioFlowboardSink::new(config);
            let task = tokio::spawn(crate::workflow::run_flowboard_outbox(
                self.coordinator(),
                sink.clone(),
                self.shutdown.subscribe(),
            ));
            (sink, task)
        });
        let configured_dispatcher = self.workflow_worker.clone().map(|config| {
            Arc::new(crate::workflow_dispatcher::RuntimeWorkerDispatcher::new(
                socket_path.to_owned(),
                String::from_utf8((*self.control_token).clone())
                    .expect("Supervisor service tokens are ASCII"),
                config,
                Arc::clone(&self.workflow_invocations),
            )) as Arc<dyn WorkflowDispatcher>
        });
        let dispatcher = self.workflow_dispatcher.clone().or(configured_dispatcher);
        let dispatcher_task = dispatcher.map(|dispatcher| {
            tokio::spawn(crate::workflow::run_workflow_dispatcher(
                self.coordinator(),
                dispatcher,
                self.shutdown.subscribe(),
            ))
        });
        let mut shutdown_rx = self.shutdown.subscribe();
        loop {
            let accepted = tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() { break; }
                    continue;
                }
                accepted = listener.accept() => accepted?,
            };
            let (stream, _) = accepted;
            let service = self.clone();
            tokio::spawn(async move {
                let _ = service.connection(stream).await;
            });
        }
        let _ = std::fs::remove_file(socket_path);
        if let Some(identity) = ready_identity {
            remove_ready_file(identity);
        }
        if let Some((sink, task)) = outbox_task {
            sink.shutdown().await;
            let _ = task.await;
        }
        if let Some(task) = dispatcher_task {
            let _ = task.await;
        }
        Ok(())
    }

    async fn connection(&self, stream: UnixStream) -> io::Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        read_frame(&mut reader, &mut line).await?;
        let auth: Value = serde_json::from_slice(&line).map_err(invalid_data)?;
        let supplied = auth
            .get("token")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .as_bytes();
        let role = auth
            .get("role")
            .and_then(Value::as_str)
            .and_then(ClientRole::parse);
        let reporter_attempt = if role == Some(ClientRole::Reporter) {
            self.reporter_attempt(supplied)
        } else {
            None
        };
        let canceller_attempt = if role == Some(ClientRole::Canceller) {
            self.canceller_attempt(supplied)
        } else {
            None
        };
        let output_capability = if role == Some(ClientRole::OutputReader) {
            self.output_capability(supplied)
        } else {
            None
        };
        let authorized = match role {
            Some(ClientRole::View) => constant_time_eq(&self.view_token, supplied),
            Some(ClientRole::Control) => constant_time_eq(&self.control_token, supplied),
            Some(ClientRole::Tui) => constant_time_eq(&self.tui_token, supplied),
            Some(ClientRole::Reporter) => reporter_attempt.is_some(),
            Some(ClientRole::Canceller) => canceller_attempt.is_some(),
            Some(ClientRole::OutputReader) => output_capability.is_some(),
            None => false,
        };
        if auth.get("type").and_then(Value::as_str) != Some("auth") || !authorized {
            write_json(
                &mut writer,
                &json!({"type":"error","code":"unauthorized","message":"authentication failed"}),
            )
            .await?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authentication failed",
            ));
        }
        let role = role.expect("authorized role was parsed");
        let authenticated_attempt = reporter_attempt;
        let authenticated_cancellation = canceller_attempt;
        let authenticated_output_process = output_capability
            .as_ref()
            .map(|(process_id, _)| process_id.clone());
        let output_expires_at = output_capability.map(|(_, expires_at)| expires_at);
        let mut revocation_rx = self.revocations.subscribe(role);
        if self.revocations.revoked(role) {
            write_json(
                &mut writer,
                &json!({"type":"error","code":"unauthorized","message":"authentication failed"}),
            )
            .await?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "authentication failed",
            ));
        }
        write_json(
            &mut writer,
            &json!({"type":"ready","version":1,"role":role.as_str()}),
        )
        .await?;
        let (out_tx, mut out_rx) = mpsc::channel::<Value>(64);
        let writer_task = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if write_json(&mut writer, &msg).await.is_err() {
                    break;
                }
            }
        });
        let mut subscriptions: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let session_lifetime = if role == ClientRole::OutputReader {
            self.session_ttl.min(OUTPUT_CAPABILITY_TTL)
        } else {
            self.session_ttl
        };
        let configured_deadline = tokio::time::Instant::now() + session_lifetime;
        let session_deadline = output_expires_at
            .map(|expiry| configured_deadline.min(tokio::time::Instant::from_std(expiry)))
            .unwrap_or(configured_deadline);
        let mut service_shutdown_rx = self.shutdown.subscribe();
        loop {
            line.clear();
            match read_session_frame(
                &mut reader,
                &mut line,
                session_deadline,
                &mut revocation_rx,
                &mut service_shutdown_rx,
            )
            .await
            {
                Ok(()) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::PermissionDenied
                            | io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    break
                }
                Err(e) => return Err(e),
            }
            let request: Value = match serde_json::from_slice(&line) {
                Ok(v) => v,
                Err(e) => {
                    let _ = out_tx
                        .send(json!({"type":"error","code":"invalid_json","message":e.to_string()}))
                        .await;
                    continue;
                }
            };
            let request_id = request.get("requestId").cloned().unwrap_or(Value::Null);
            let action = request.get("type").and_then(Value::as_str).unwrap_or("");
            let permitted = role_allows(role, action);
            if !permitted {
                let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"view credentials cannot control processes"})).await;
                continue;
            }
            if role == ClientRole::Reporter
                && !self.capability_active("report", authenticated_attempt.as_ref())
            {
                let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"reporting capability has expired"})).await;
                continue;
            }
            if role == ClientRole::Canceller
                && !self.capability_active("cancel", authenticated_cancellation.as_ref())
            {
                let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"cancellation capability has expired"})).await;
                continue;
            }
            if role == ClientRole::OutputReader
                && process_id(&request)
                    .ok()
                    .map(|id| id.to_string())
                    .as_deref()
                    != authenticated_output_process.as_deref()
            {
                let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"output credential is scoped to a different process"})).await;
                continue;
            }
            match action {
                "registerRun" => {
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let context = request.get("context").cloned().unwrap_or_else(|| json!({}));
                    match validate_context(&context) {
                        Ok(()) => {
                            let response = match self
                                .coordinator()
                                .register_run_with_id(command_id, context)
                                .await
                            {
                                Ok(run) => {
                                    json!({"type":"runRegistered","requestId":request_id,"run":run})
                                }
                                Err(error) => coordinator_error(request_id, error),
                            };
                            let _ = out_tx.send(response).await;
                        }
                        Err(message) => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_context","message":message})).await;
                        }
                    }
                }
                "registerActivity" => {
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let run_id = request.get("runId").and_then(Value::as_str).unwrap_or("");
                    let kind = request
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or("activity");
                    let response = if kind.len() > 128 || kind.chars().any(char::is_control) {
                        json!({"type":"error","requestId":request_id,"code":"invalid_activity","message":"invalid activity kind"})
                    } else {
                        match self
                            .coordinator()
                            .register_activity_with_id(
                                command_id,
                                run_id.to_owned(),
                                kind.to_owned(),
                            )
                            .await
                        {
                            Ok(activity) => {
                                json!({"type":"activityRegistered","requestId":request_id,"activity":activity})
                            }
                            Err(error) => coordinator_error(request_id, error),
                        }
                    };
                    let _ = out_tx.send(response).await;
                }
                "registerAttempt" => {
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let activity_id = request
                        .get("activityId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let response = match self
                        .coordinator()
                        .register_attempt_with_id(command_id, activity_id.to_owned())
                        .await
                    {
                        Ok(attempt) => {
                            let reporter = new_token()?;
                            let canceller = new_token()?;
                            let reporter_text =
                                String::from_utf8(reporter.clone()).expect("token is ASCII hex");
                            let canceller_text =
                                String::from_utf8(canceller.clone()).expect("token is ASCII hex");
                            let capability = AttemptCapability {
                                attempt_id: attempt.attempt_id.clone(),
                                generation: attempt.generation,
                            };
                            let mut capabilities = self.capabilities.lock().unwrap();
                            capabilities
                                .reporter_tokens
                                .insert(reporter, capability.clone());
                            capabilities.cancel_tokens.insert(canceller, capability);
                            json!({"type":"attemptRegistered","requestId":request_id,"attempt":attempt,"reporterToken":reporter_text,"cancelToken":canceller_text})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "setActivityStatus" => {
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let activity_id = request
                        .get("activityId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let status: Result<crate::coordinator::ActivityStatus, _> = request
                        .get("status")
                        .cloned()
                        .ok_or(())
                        .and_then(|value| serde_json::from_value(value).map_err(|_| ()));
                    let reason_value = request.get("reason");
                    let invalid_reason_type =
                        reason_value.is_some_and(|value| !value.is_null() && !value.is_string());
                    let reason = reason_value.and_then(Value::as_str).map(str::to_owned);
                    let invalid_reason = invalid_reason_type
                        || reason.as_ref().is_some_and(|value| {
                            value.len() > 2048 || value.chars().any(char::is_control)
                        });
                    let response = if invalid_reason {
                        json!({"type":"error","requestId":request_id,"code":"invalid_activity","message":"waiting reason must be at most 2048 bytes and contain no control characters"})
                    } else if let Ok(status) = status {
                        match self
                            .coordinator()
                            .set_waiting(command_id, activity_id.to_owned(), reason, status)
                            .await
                        {
                            Ok(activity) => {
                                json!({"type":"activityStatus","requestId":request_id,"activity":activity})
                            }
                            Err(error) => coordinator_error(request_id, error),
                        }
                    } else {
                        json!({"type":"error","requestId":request_id,"code":"invalid_activity_status","message":"unknown activity status"})
                    };
                    let _ = out_tx.send(response).await;
                }
                "setRunStatus" => {
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let run_id = request.get("runId").and_then(Value::as_str).unwrap_or("");
                    let status: Result<ExecutionStatus, _> = request
                        .get("status")
                        .cloned()
                        .ok_or(())
                        .and_then(|value| serde_json::from_value(value).map_err(|_| ()));
                    let response = if let Ok(status) = status {
                        match self
                            .coordinator()
                            .set_run_status(command_id, run_id.to_owned(), status)
                            .await
                        {
                            Ok(run) => json!({"type":"runStatus","requestId":request_id,"run":run}),
                            Err(error) => coordinator_error(request_id, error),
                        }
                    } else {
                        json!({"type":"error","requestId":request_id,"code":"invalid_run_status","message":"unknown run status"})
                    };
                    let _ = out_tx.send(response).await;
                }
                "setActivityTimeout" => {
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let activity_id = request
                        .get("activityId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let timeout_ms = request
                        .get("timeoutMs")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if timeout_ms == 0 || timeout_ms > 86_400_000 {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_timeout","message":"timeoutMs must be between 1 and 86400000"})).await;
                        continue;
                    }
                    let response = match self
                        .coordinator()
                        .set_activity_timeout(
                            command_id,
                            activity_id.to_owned(),
                            Duration::from_millis(timeout_ms),
                        )
                        .await
                    {
                        Ok(()) => {
                            json!({"type":"activityTimeoutSet","requestId":request_id,"activityId":activity_id,"timeoutMs":timeout_ms})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "report" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let status = request.get("status").and_then(Value::as_str).unwrap_or("");
                    let detail = match sanitize_report_detail(
                        request.get("detail").cloned().unwrap_or(Value::Null),
                    ) {
                        Ok(detail) => detail,
                        Err(message) => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_report","message":message})).await;
                            continue;
                        }
                    };
                    let generation = request.get("generation").and_then(Value::as_u64);
                    if authenticated_attempt
                        .as_ref()
                        .map(|capability| capability.attempt_id.as_str())
                        != Some(attempt_id)
                        || generation
                            != authenticated_attempt
                                .as_ref()
                                .map(|capability| capability.generation)
                        || status.len() > 128
                        || !matches!(
                            status,
                            "queued"
                                | "running"
                                | "succeeded"
                                | "failed"
                                | "cancelled"
                                | "blocked"
                                | "unknown"
                        )
                    {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"report credential is not scoped to this attempt"})).await;
                        continue;
                    }
                    let status =
                        serde_json::from_value(json!(status)).unwrap_or(ExecutionStatus::Unknown);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .report_attempt_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation.unwrap_or(0),
                            status,
                            detail,
                        )
                        .await
                    {
                        Ok(attempt) => {
                            json!({"type":"reported","requestId":request_id,"attempt":attempt})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                    if matches!(
                        status,
                        ExecutionStatus::Succeeded
                            | ExecutionStatus::Failed
                            | ExecutionStatus::Cancelled
                    ) {
                        let mut capabilities = self.capabilities.lock().unwrap();
                        capabilities
                            .reporter_tokens
                            .retain(|_, owner| owner.attempt_id != attempt_id);
                        capabilities
                            .cancel_tokens
                            .retain(|_, owner| owner.attempt_id != attempt_id);
                    }
                }
                "pollAttempt" => {
                    let Some(capability) = authenticated_attempt.as_ref() else {
                        continue;
                    };
                    let attempt_id = capability.attempt_id.as_str();
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if generation != capability.generation {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"stale_generation","message":"attempt generation is stale"})).await;
                        continue;
                    }
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .mark_cancel_delivered_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                        )
                        .await
                    {
                        Ok(attempt) => {
                            json!({"type":"attemptState","requestId":request_id,"attempt":attempt})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "cancelAttempt" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    if role == ClientRole::Canceller
                        && (authenticated_cancellation
                            .as_ref()
                            .map(|capability| capability.attempt_id.as_str())
                            != Some(attempt_id)
                            || generation
                                != authenticated_cancellation
                                    .as_ref()
                                    .map(|capability| capability.generation)
                                    .unwrap_or(0))
                    {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"cancellation credential is not scoped to this attempt"})).await;
                        continue;
                    }
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .request_cancel_with_id(command_id, attempt_id.to_owned(), generation)
                        .await
                    {
                        Ok(attempt) => {
                            json!({"type":"cancelRequested","requestId":request_id,"attempt":attempt})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "registerFlowRun" => {
                    let projection = match request.get("projection") {
                        Some(value) => serde_json::from_value::<FlowRunProjection>(value.clone()),
                        None => Err(serde_json::from_str::<FlowRunProjection>("null").unwrap_err()),
                    };
                    let projection = match projection {
                        Ok(projection) => projection,
                        Err(_) => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_flowrun","message":"projection must be a valid FlowRun projection"})).await;
                            continue;
                        }
                    };
                    let flow_id = projection.flow_id.clone();
                    if request
                        .get("invocations")
                        .is_some_and(|value| !value.is_object())
                    {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_workflow_invocations","message":"invocations must be an object keyed by FlowRun task ID"})).await;
                        continue;
                    }
                    let valid_task_ids = projection
                        .tasks
                        .iter()
                        .map(|task| task.task_id.as_str())
                        .collect::<HashSet<_>>();
                    let invocations = request
                        .get("invocations")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default();
                    let invocation_bytes = serde_json::to_vec(&invocations)
                        .map(|bytes| bytes.len())
                        .unwrap_or(usize::MAX);
                    if invocation_bytes > 900 * 1024
                        || invocations.iter().any(|(task_id, invocation)| {
                            !valid_task_ids.contains(task_id.as_str()) || !invocation.is_object()
                        })
                    {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_workflow_invocations","message":"invocations must be bounded JSON objects keyed by FlowRun task ID"})).await;
                        continue;
                    }
                    let coordinator_id = request
                        .get("coordinatorId")
                        .and_then(Value::as_str)
                        .unwrap_or("supervisor")
                        .to_owned();
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(&flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .register_flowrun_with_id(command_id, projection, coordinator_id)
                        .await
                    {
                        Ok(flow) => {
                            if self.workflow_worker.is_some() || self.workflow_dispatcher.is_some()
                            {
                                let mut registered = self.workflow_invocations.lock().unwrap();
                                for (task_id, invocation) in invocations {
                                    registered.insert((flow_id.clone(), task_id), invocation);
                                }
                            }
                            json!({"type":"flowRunRegistered","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "updateFlowInvocations" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(invocations) = request.get("invocations").and_then(Value::as_object)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_workflow_invocations","message":"invocations must be an object keyed by FlowRun task ID"})).await;
                        continue;
                    };
                    let snapshot = self.coordinator().snapshot();
                    let Some(flow) = snapshot
                        .flow_runs
                        .iter()
                        .find(|flow| flow.flow_id == flow_id)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"not_found","message":"unknown FlowRun"})).await;
                        continue;
                    };
                    if flow.coordinator_fence != fence {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"stale_coordinator_fence","message":"FlowRun invocation update used a stale coordinator fence"})).await;
                        continue;
                    }
                    let invocation_bytes = serde_json::to_vec(invocations)
                        .map(|bytes| bytes.len())
                        .unwrap_or(usize::MAX);
                    if invocation_bytes > 900 * 1024
                        || invocations.iter().any(|(task_id, invocation)| {
                            !flow.tasks.contains_key(task_id) || !invocation.is_object()
                        })
                    {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_workflow_invocations","message":"invocations must be bounded JSON objects keyed by FlowRun task ID"})).await;
                        continue;
                    }
                    let count = invocations.len();
                    {
                        let mut registered = self.workflow_invocations.lock().unwrap();
                        registered
                            .retain(|(registered_flow_id, _), _| registered_flow_id != flow_id);
                        for (task_id, invocation) in invocations {
                            registered
                                .insert((flow_id.to_owned(), task_id.clone()), invocation.clone());
                        }
                    }
                    let _ = out_tx.send(json!({"type":"flowInvocationsUpdated","requestId":request_id,"flowId":flow_id,"count":count})).await;
                }
                "claimFlowRun" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let coordinator_id = request
                        .get("coordinatorId")
                        .and_then(Value::as_str)
                        .unwrap_or("supervisor")
                        .to_owned();
                    let expected_fence = request
                        .get("expectedFence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .claim_flowrun_with_id(
                            command_id,
                            flow_id.to_owned(),
                            coordinator_id,
                            expected_fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            json!({"type":"flowRunClaimed","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "advanceFlowStage" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let expected_stage = request
                        .get("expectedStage")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let next_stage = request
                        .get("nextStage")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let expected_flow_revision = request
                        .get("expectedFlowRevision")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    if expected_stage.is_empty() || next_stage.is_empty() {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_stage_transition","message":"expectedStage and nextStage are required"})).await;
                        continue;
                    }
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .advance_flow_stage_with_id(
                            command_id,
                            flow_id.to_owned(),
                            expected_stage.to_owned(),
                            next_stage.to_owned(),
                            expected_flow_revision,
                            fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            json!({"type":"flowStageAdvanced","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "completeFlowRun" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let expected_stage = request
                        .get("expectedStage")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let expected_flow_revision = request
                        .get("expectedFlowRevision")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let evidence = request.get("evidence").cloned().unwrap_or(Value::Null);
                    if serde_json::to_vec(&evidence).map_or(usize::MAX, |bytes| bytes.len())
                        > 16 * 1024
                    {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_completion_evidence","message":"completion evidence exceeds 16 KiB"})).await;
                        continue;
                    }
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .complete_flowrun_with_id(
                            command_id,
                            flow_id.to_owned(),
                            expected_stage.to_owned(),
                            expected_flow_revision,
                            evidence,
                            fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            json!({"type":"flowRunCompleted","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "requestWorkerApproval" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let approval_id = request
                        .get("approvalRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let kind = request.get("kind").and_then(Value::as_str).unwrap_or("");
                    let reason = request.get("reason").and_then(Value::as_str).unwrap_or("");
                    let payload = request.get("payload").cloned().unwrap_or(Value::Null);
                    let timeout_ms = request
                        .get("timeoutMs")
                        .and_then(Value::as_u64)
                        .unwrap_or(120_000)
                        .clamp(1, 30 * 60 * 1000);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .request_worker_approval_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                            fence,
                            approval_id.to_owned(),
                            kind.to_owned(),
                            reason.to_owned(),
                            payload,
                            now_unix_ms().saturating_add(timeout_ms),
                        )
                        .await
                    {
                        Ok(approval) => {
                            json!({"type":"approvalRequested","requestId":request_id,"approval":approval})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "requestWorkerTool" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let tool_request_id = request
                        .get("toolRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let tool_name = request
                        .get("toolName")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let arguments = request.get("arguments").cloned().unwrap_or(Value::Null);
                    let timeout_ms = request
                        .get("timeoutMs")
                        .and_then(Value::as_u64)
                        .unwrap_or(120_000)
                        .clamp(1, 30 * 60 * 1000);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .request_worker_tool_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                            fence,
                            tool_request_id.to_owned(),
                            tool_name.to_owned(),
                            arguments,
                            now_unix_ms().saturating_add(timeout_ms),
                        )
                        .await
                    {
                        Ok(tool_request) => {
                            json!({"type":"toolRequestCreated","requestId":request_id,"toolRequest":tool_request})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "listPendingToolRequests" => {
                    let requests = self
                        .coordinator()
                        .snapshot()
                        .tool_requests
                        .into_iter()
                        .filter(|item| {
                            matches!(
                                item.state,
                                crate::coordinator::WorkerToolRequestState::Pending
                                    | crate::coordinator::WorkerToolRequestState::Claimed
                            )
                        })
                        .collect::<Vec<_>>();
                    let _ = out_tx.send(json!({"type":"pendingToolRequests","requestId":request_id,"toolRequests":requests})).await;
                }
                "claimToolRequest" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let tool_request_id = request
                        .get("toolRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let claim_id = request.get("claimId").and_then(Value::as_str).unwrap_or("");
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .claim_worker_tool_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                            tool_request_id.to_owned(),
                            claim_id.to_owned(),
                            fence,
                        )
                        .await
                    {
                        Ok(tool_request) => {
                            json!({"type":"toolRequestClaimed","requestId":request_id,"toolRequest":tool_request})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "waitToolRequest" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let tool_request_id = request
                        .get("toolRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let response = match self
                        .coordinator()
                        .wait_worker_tool(attempt_id, generation, tool_request_id)
                        .await
                    {
                        Ok(tool_request) => {
                            json!({"type":"toolRequestWait","requestId":request_id,"toolRequest":tool_request})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "resolveToolRequest" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let tool_request_id = request
                        .get("toolRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let claim_id = request.get("claimId").and_then(Value::as_str).unwrap_or("");
                    let resolution_id = request
                        .get("resolutionId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let result = request
                        .get("result")
                        .filter(|value| !value.is_null())
                        .cloned();
                    let error = request
                        .get("error")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .resolve_worker_tool_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                            tool_request_id.to_owned(),
                            claim_id.to_owned(),
                            resolution_id.to_owned(),
                            result,
                            error,
                            fence,
                        )
                        .await
                    {
                        Ok(tool_request) => {
                            json!({"type":"toolRequestResolved","requestId":request_id,"toolRequest":tool_request})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "finishToolRequest" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let tool_request_id = request
                        .get("toolRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let state = match request.get("state").and_then(Value::as_str) {
                        Some("cancelled") => crate::coordinator::WorkerToolRequestState::Cancelled,
                        Some("interrupted") => {
                            crate::coordinator::WorkerToolRequestState::Interrupted
                        }
                        Some("unknown") => crate::coordinator::WorkerToolRequestState::Unknown,
                        _ => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_tool_state","message":"state must be cancelled, interrupted, or unknown"})).await;
                            continue;
                        }
                    };
                    let error = request
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("worker tool request ended");
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .finish_worker_tool_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                            tool_request_id.to_owned(),
                            state,
                            error.to_owned(),
                            fence,
                        )
                        .await
                    {
                        Ok(tool_request) => {
                            json!({"type":"toolRequestFinished","requestId":request_id,"toolRequest":tool_request})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "listPendingApprovals" => {
                    let approvals = self
                        .coordinator()
                        .snapshot()
                        .approvals
                        .into_iter()
                        .filter(|approval| {
                            approval.state == crate::coordinator::ApprovalRequestState::Pending
                        })
                        .collect::<Vec<_>>();
                    let _ = out_tx.send(json!({"type":"pendingApprovals","requestId":request_id,"approvals":approvals})).await;
                }
                "waitApproval" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let approval_id = request
                        .get("approvalRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let response = match self
                        .coordinator()
                        .wait_worker_approval(attempt_id, generation, approval_id)
                        .await
                    {
                        Ok(approval) => {
                            json!({"type":"approvalWait","requestId":request_id,"approval":approval})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "resolveApproval" => {
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let approval_id = request
                        .get("approvalRequestId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let resolution_id = request
                        .get("resolutionId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let decision = match request.get("decision").and_then(Value::as_str) {
                        Some("approved") => crate::coordinator::ApprovalRequestState::Approved,
                        Some("denied") => crate::coordinator::ApprovalRequestState::Denied,
                        Some("cancelled") => crate::coordinator::ApprovalRequestState::Cancelled,
                        Some("interrupted") => {
                            crate::coordinator::ApprovalRequestState::Interrupted
                        }
                        _ => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_approval_decision","message":"decision must be approved, denied, cancelled, or interrupted"})).await;
                            continue;
                        }
                    };
                    let grant = request
                        .get("grant")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(attempt_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .resolve_worker_approval_with_id(
                            command_id,
                            attempt_id.to_owned(),
                            generation,
                            approval_id.to_owned(),
                            resolution_id.to_owned(),
                            decision,
                            grant,
                            fence,
                        )
                        .await
                    {
                        Ok(approval) => {
                            json!({"type":"approvalResolved","requestId":request_id,"approval":approval})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "bindFlowAttempt" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let task_id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let source_cursor = request
                        .get("sourceCursor")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .bind_flow_attempt_with_id(
                            command_id,
                            flow_id.to_owned(),
                            task_id.to_owned(),
                            attempt_id.to_owned(),
                            generation,
                            source_cursor,
                            fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            json!({"type":"flowAttemptBound","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "reconcileFlowAttempt" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let task_id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
                    let outcome = match request.get("outcome") {
                        Some(value) => serde_json::from_value::<AttemptOutcome>(value.clone()),
                        None => Err(serde_json::from_str::<AttemptOutcome>("null").unwrap_err()),
                    };
                    let outcome = match outcome {
                        Ok(outcome) => outcome,
                        Err(_) => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_attempt_outcome","message":"outcome must be a valid attempt outcome"})).await;
                            continue;
                        }
                    };
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .reconcile_flow_attempt_with_id(
                            command_id,
                            flow_id.to_owned(),
                            task_id.to_owned(),
                            outcome,
                            fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            json!({"type":"flowAttemptReconciled","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "applyFlowReview" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let task_id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
                    let flow_revision = request
                        .get("flowRevision")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let evidence = request.get("evidence").cloned().unwrap_or(Value::Null);
                    let evidence_size = serde_json::to_vec(&evidence)
                        .map(|bytes| bytes.len())
                        .unwrap_or(usize::MAX);
                    if evidence_size > 16 * 1024 {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_review_evidence","message":"review evidence exceeds 16 KiB"})).await;
                        continue;
                    }
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .apply_flow_review_with_id(
                            command_id,
                            flow_id.to_owned(),
                            task_id.to_owned(),
                            flow_revision,
                            evidence,
                            fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            json!({"type":"flowReviewApplied","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "cancelFlowTask" => {
                    let flow_id = request.get("flowId").and_then(Value::as_str).unwrap_or("");
                    let task_id = request.get("taskId").and_then(Value::as_str).unwrap_or("");
                    let attempt_id = request
                        .get("attemptId")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let generation = request
                        .get("generation")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let fence = request
                        .get("fence")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    let Some(command_id) =
                        coordinator_request_id(&request_id, role, action, Some(flow_id))
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    let response = match self
                        .coordinator()
                        .request_flow_task_cancel_with_id(
                            command_id,
                            flow_id.to_owned(),
                            task_id.to_owned(),
                            attempt_id.to_owned(),
                            generation,
                            fence,
                        )
                        .await
                    {
                        Ok(flow) => {
                            let _ = self
                                .coordinator()
                                .request_cancel_with_id(
                                    format!(
                                        "flow-cancel-generic:{flow_id}:{attempt_id}:{generation}"
                                    ),
                                    attempt_id.to_owned(),
                                    generation,
                                )
                                .await;
                            let pending_approvals = self
                                .coordinator()
                                .snapshot()
                                .approvals
                                .into_iter()
                                .filter(|approval| {
                                    approval.attempt_id == attempt_id
                                        && approval.generation == generation
                                        && approval.state
                                            == crate::coordinator::ApprovalRequestState::Pending
                                })
                                .collect::<Vec<_>>();
                            for approval in pending_approvals {
                                let digest =
                                    format!("{:x}", Sha256::digest(approval.request_id.as_bytes()));
                                let _ = self
                                    .coordinator()
                                    .resolve_worker_approval_with_id(
                                        format!(
                                            "flow-cancel-approval:{attempt_id}:{generation}:{}",
                                            &digest[..16]
                                        ),
                                        attempt_id.to_owned(),
                                        generation,
                                        approval.request_id,
                                        format!("cancel:{flow_id}:{task_id}:{generation}"),
                                        crate::coordinator::ApprovalRequestState::Cancelled,
                                        None,
                                        fence,
                                    )
                                    .await;
                            }
                            let pending_tools = self.coordinator().snapshot().tool_requests.into_iter()
                                .filter(|tool| tool.attempt_id == attempt_id && tool.generation == generation
                                    && matches!(tool.state, crate::coordinator::WorkerToolRequestState::Pending | crate::coordinator::WorkerToolRequestState::Claimed))
                                .collect::<Vec<_>>();
                            for tool in pending_tools {
                                let digest =
                                    format!("{:x}", Sha256::digest(tool.request_id.as_bytes()));
                                let _ = self
                                    .coordinator()
                                    .finish_worker_tool_with_id(
                                        format!(
                                            "flow-cancel-tool:{attempt_id}:{generation}:{}",
                                            &digest[..16]
                                        ),
                                        attempt_id.to_owned(),
                                        generation,
                                        tool.request_id,
                                        crate::coordinator::WorkerToolRequestState::Cancelled,
                                        "worker attempt cancellation was requested".into(),
                                        fence,
                                    )
                                    .await;
                            }
                            json!({"type":"flowTaskCancellationRequested","requestId":request_id,"flowRun":flow})
                        }
                        Err(error) => coordinator_error(request_id, error),
                    };
                    let _ = out_tx.send(response).await;
                }
                "status" => {
                    let snapshot = self.coordinator().snapshot();
                    let recovery = self.recovery_summary();
                    let recovery_diagnostics =
                        recovery.get("diagnostics").cloned().unwrap_or(Value::Null);
                    let _ = out_tx.send(json!({"type":"status","requestId":request_id,"epoch":snapshot.epoch,"cursor":snapshot.cursor,"managerId":snapshot.manager_id,"pid":std::process::id(),"uptimeMs":self.started_at.elapsed().as_millis() as u64,"counts":snapshot.counts,"recovery":recovery,"recoveryDiagnostics":recovery_diagnostics,"workflowDispatch":self.workflow_dispatch_summary()})).await;
                }
                "subscribe" => {
                    let coordinator = self.coordinator();
                    let initial = coordinator.snapshot();
                    let epoch = request
                        .get("epoch")
                        .and_then(Value::as_str)
                        .unwrap_or(&initial.epoch)
                        .to_owned();
                    let cursor = request
                        .get("cursor")
                        .and_then(Value::as_u64)
                        .unwrap_or(initial.cursor);
                    let (mut receiver, replay) = coordinator.subscribe_from(epoch, cursor).await;
                    let snapshot = coordinator.snapshot();
                    let resync_required = replay.resync_required;
                    let replay_cursor = replay.cursor;
                    let filter = request.clone();
                    let filtered_events = replay
                        .events
                        .iter()
                        .filter(|event| event_matches_filter(event, &filter))
                        .cloned()
                        .collect::<Vec<_>>();
                    let snapshot_value = serde_json::to_value(&snapshot).unwrap_or(Value::Null);
                    let snapshot_value = redact_snapshot_for_role(snapshot_value, role);
                    let response = json!({"type":if resync_required {"resyncRequired"} else {"subscribed"},"requestId":request_id,"epoch":replay.epoch,"cursor":replay.cursor,"resyncRequired":resync_required,"events":filtered_events,"snapshot":snapshot_value});
                    let _ = out_tx.send(response).await;
                    if !resync_required {
                        let tx = out_tx.clone();
                        subscriptions.push(tokio::spawn(async move {
                            loop {
                                match receiver.recv().await {
                                    Ok(event) if event.cursor > replay_cursor && event_matches_filter(&event, &filter) => {
                                        if tx.send(json!({"type":"coordinatorEvent","event":event})).await.is_err() { break; }
                                    }
                                    Ok(_) => {}
                                    Err(broadcast::error::RecvError::Lagged(_)) => {
                                        let current = coordinator.snapshot();
                                        let snapshot_value = serde_json::to_value(&current).unwrap_or(Value::Null);
                                        let snapshot_value = redact_snapshot_for_role(snapshot_value, role);
                                        let _ = tx.send(json!({"type":"resyncRequired","epoch":current.epoch,"cursor":current.cursor,"snapshot":snapshot_value})).await;
                                        break;
                                    }
                                    Err(broadcast::error::RecvError::Closed) => break,
                                }
                            }
                        }));
                    }
                }
                "shutdown" => {
                    let grace = Duration::from_millis(
                        request
                            .get("graceMs")
                            .and_then(Value::as_u64)
                            .unwrap_or(1500)
                            .min(30_000),
                    );
                    let results = self.supervisor.shutdown_all(grace).await;
                    let _ = out_tx.send(match results {
                        Ok(results) => json!({"type":"shutdownComplete","requestId":request_id,"reaped":results.len()}),
                        Err(error) => json!({"type":"error","requestId":request_id,"code":"shutdown_failed","message":error.to_string()}),
                    }).await;
                    self.shutdown.send_replace(true);
                }
                "start" => {
                    let output_token = new_token()?;
                    let Some(command_id) = coordinator_request_id(&request_id, role, action, None)
                    else {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_request_id","message":"coordinator mutations require a requestId of at most 128 bytes"})).await;
                        continue;
                    };
                    match self.start(&request, command_id).await {
                        Ok((id, managed)) => {
                            let output_token_text = String::from_utf8(output_token.clone())
                                .expect("token is ASCII hex");
                            register_output_capability(
                                &mut self.capabilities.lock().unwrap(),
                                output_token,
                                id.to_string(),
                                Instant::now(),
                                OUTPUT_CAPABILITY_TTL,
                            );
                            let _ = out_tx.send(json!({"type":"started","requestId":request_id,"processId":id.to_string(),"outputToken":output_token_text})).await;
                            subscriptions.push(subscribe(managed, id, out_tx.clone()));
                        }
                        Err(e) => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"start_failed","message":e})).await;
                        }
                    }
                }
                "attach" | "wait" => match process_id(&request) {
                    Ok(id) => match self.managed(id) {
                        Some(managed) => {
                            let _ = out_tx.send(json!({"type":"attached","requestId":request_id,"processId":id.to_string()})).await;
                            subscriptions.push(subscribe(managed, id, out_tx.clone()));
                        }
                        None => {
                            let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"not_found","message":"unknown process"})).await;
                        }
                    },
                    Err(e) => {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"invalid_process_id","message":e})).await;
                    }
                },
                "snapshot" => {
                    let processes = self.supervisor.managed_processes().into_iter().map(|process| json!({
                        "processId":process.supervisor_id.to_string(),"pid":process.process_id,"alive":process.alive,
                        "executable":process.executable,"framework":process.framework,"runLabel":process.run_label,
                        "projectId":process.project_id,"flowRunId":process.flow_run_id,"taskId":process.task_id
                        ,"postId":process.post_id,"workerId":process.worker_id,"runtimeRunId":process.runtime_run_id
                    })).collect::<Vec<_>>();
                    let snapshot = self.coordinator().snapshot();
                    let mut seen_processes = HashSet::new();
                    let mut coordinated_processes = snapshot.processes.iter().map(|coordinated| {
                        seen_processes.insert(coordinated.process_id.clone());
                        let observed = processes.iter().find(|p| p["processId"] == coordinated.process_id);
                        let metadata = observed.map(|p|json!({"framework":p["framework"],"runLabel":p["runLabel"],"projectId":p["projectId"],"flowRunId":p["flowRunId"],"taskId":p["taskId"],"postId":p["postId"],"workerId":p["workerId"],"runtimeRunId":p["runtimeRunId"]})).unwrap_or(Value::Null);
                        let mut row = json!({"processId":coordinated.process_id,"runId":coordinated.run_id,"attemptId":coordinated.attempt_id,"alive":coordinated.alive,"exitStatus":coordinated.exit_status,"revision":coordinated.revision,"updatedAtMs":coordinated.updated_at_ms,
                            "pid":observed.map(|p|p["pid"].clone()),"executable":observed.map(|p|p["executable"].clone()),
                            "metadata":metadata});
                        if let Some(metadata) = row.get("metadata").and_then(Value::as_object).cloned() {
                            for (key, value) in metadata {
                                row[key] = value;
                            }
                        }
                        row
                    }).collect::<Vec<_>>();
                    for observed in &processes {
                        let id = observed["processId"].as_str().unwrap_or_default();
                        if seen_processes.insert(id.to_owned()) {
                            let metadata = json!({"framework":observed["framework"],"runLabel":observed["runLabel"],"projectId":observed["projectId"],"flowRunId":observed["flowRunId"],"taskId":observed["taskId"],"postId":observed["postId"],"workerId":observed["workerId"],"runtimeRunId":observed["runtimeRunId"]});
                            let mut row = json!({"processId":id,"runId":null,"attemptId":null,"alive":observed["alive"],"exitStatus":null,"revision":0,"updatedAtMs":0,"pid":observed["pid"],"executable":observed["executable"],"metadata":metadata});
                            if let Some(metadata) =
                                row.get("metadata").and_then(Value::as_object).cloned()
                            {
                                for (key, value) in metadata {
                                    row[key] = value;
                                }
                            }
                            coordinated_processes.push(row);
                        }
                    }
                    let mut snapshot_value =
                        serde_json::to_value(&snapshot).unwrap_or_else(|_| json!({}));
                    snapshot_value = redact_snapshot_for_role(snapshot_value, role);
                    snapshot_value["processes"] = json!(coordinated_processes);
                    snapshot_value["type"] = json!("snapshot");
                    snapshot_value["requestId"] = request_id;
                    snapshot_value["cursor"] = json!(snapshot.cursor);
                    snapshot_value["recovery"] = self.recovery_summary();
                    snapshot_value["workflowDispatch"] = self.workflow_dispatch_summary();
                    snapshot_value["workflowDispatchConfigured"] =
                        json!(self.workflow_dispatcher.is_some() || self.workflow_worker.is_some());
                    let _ = out_tx.send(snapshot_value).await;
                }
                "revokeRole" => {
                    let target = request
                        .get("role")
                        .and_then(Value::as_str)
                        .and_then(ClientRole::parse);
                    let revoked = match target {
                        Some(ClientRole::View | ClientRole::Tui) => {
                            self.revocations.revoke(target.unwrap())
                        }
                        _ => false,
                    };
                    let _ = out_tx.send(if revoked {
                        json!({"type":"roleRevoked","requestId":request_id,"role":target.unwrap().as_str()})
                    } else {
                        json!({"type":"error","requestId":request_id,"code":"forbidden","message":"only view and TUI roles can be revoked"})
                    }).await;
                }
                "stdin" | "closeStdin" => {
                    let id = process_id(&request);
                    let result = id.ok().and_then(|id| self.managed(id)).map(|managed| {
                        if request["type"] == "closeStdin" {
                            managed.input.try_send(InputCommand::Close)
                        } else {
                            decode_base64(request.get("data").and_then(Value::as_str).unwrap_or(""))
                                .map(|bytes| managed.input.try_send(InputCommand::Write(bytes)))
                                .unwrap_or_else(|_| {
                                    Err(mpsc::error::TrySendError::Closed(InputCommand::Close))
                                })
                        }
                    });
                    let ok = matches!(result, Some(Ok(())));
                    let _ = out_tx
                        .send(json!({"type":"ack","requestId":request_id,"ok":ok}))
                        .await;
                }
                "stop" => {
                    let actor = if role == ClientRole::Tui {
                        "tui"
                    } else {
                        "client"
                    };
                    if let Ok(id) = process_id(&request) {
                        if let Some(managed) = self.managed(id) {
                            let event = json!({"type":"stop_requested","processId":id.to_string(),"actor":actor});
                            let mut history = managed.history.lock().unwrap();
                            history.push_back(event.clone());
                            while history.len() > EVENT_HISTORY {
                                history.pop_front();
                            }
                            let _ = managed.events.send(event);
                        }
                    }
                    let result = match process_id(&request) {
                        Ok(id) => self
                            .supervisor
                            .shutdown_process(
                                id,
                                Duration::from_millis(
                                    request
                                        .get("graceMs")
                                        .and_then(Value::as_u64)
                                        .unwrap_or(1500)
                                        .min(30_000),
                                ),
                            )
                            .await
                            .map(|r| (id, format!("{r:?}"))),
                        Err(e) => Err(crate::SandboxError::execution(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            e,
                        ))),
                    };
                    let _ = out_tx.send(match result { Ok((id,status)) => json!({"type":"stopped","requestId":request_id,"processId":id.to_string(),"status":status}), Err(e) => json!({"type":"error","requestId":request_id,"code":"stop_failed","message":e.to_string()}) }).await;
                }
                _ => {
                    let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"unknown_command","message":"unsupported request"})).await;
                }
            }
        }
        for task in subscriptions {
            task.abort();
        }
        drop(out_tx);
        let _ = writer_task.await;
        Ok(())
    }

    async fn start(
        &self,
        request: &Value,
        coordinator_request_id: String,
    ) -> Result<(SupervisorProcessId, Managed), String> {
        let attempt_id = request
            .get("attemptId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let attempt_generation = request.get("generation").and_then(Value::as_u64);
        if let Some(attempt_id) = attempt_id.as_deref() {
            let matching = self
                .coordinator()
                .snapshot()
                .attempts
                .iter()
                .any(|attempt| {
                    attempt.attempt_id == attempt_id
                        && Some(attempt.generation) == attempt_generation
                });
            if !matching {
                return Err("unknown attemptId".to_owned());
            }
        } else if attempt_generation.is_some() {
            return Err("generation requires attemptId".to_owned());
        }
        let command_value = request.get("command").ok_or("missing command")?;
        let executable = command_value
            .get("executable")
            .and_then(Value::as_str)
            .ok_or("missing executable")?;
        let args = command_value
            .get("args")
            .and_then(Value::as_array)
            .ok_or("args must be an array")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or("args must contain strings")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let cwd = command_value
            .get("cwd")
            .and_then(Value::as_str)
            .map(PathBuf::from);
        let policy: crate::Policy = serde_json::from_value(
            request
                .get("policy")
                .cloned()
                .ok_or("explicit policy is required")?,
        )
        .map_err(|e| format!("invalid policy: {e}"))?;
        let metadata_value = request
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let framework = metadata_value
            .get("framework")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let label = metadata_value
            .get("runLabel")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let metadata = ManagedProcessMetadata::new(framework, label)
            .map_err(|e| e.to_string())?
            .with_flow_context(
                metadata_value
                    .get("projectId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                metadata_value
                    .get("flowRunId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                metadata_value
                    .get("taskId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            )
            .map_err(|e| e.to_string())?
            .with_work_identity(
                metadata_value
                    .get("postId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                metadata_value
                    .get("workerId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                metadata_value
                    .get("runtimeRunId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            )
            .map_err(|e| e.to_string())?;

        let mut fingerprint_input = request.clone();
        if let Some(fields) = fingerprint_input.as_object_mut() {
            fields.remove("requestId");
        }
        let fingerprint_bytes = serde_json::to_vec(&fingerprint_input)
            .map_err(|error| format!("could not fingerprint launch command: {error}"))?;
        let command_fingerprint = json!({
            "action": "start",
            "algorithm": "sha256",
            "digest": format!("{:x}", Sha256::digest(fingerprint_bytes)),
        });
        let persistent = self.state_dir.is_some();
        if persistent {
            match self
                .coordinator()
                .reserve_launch_with_id(coordinator_request_id.clone(), command_fingerprint.clone())
                .await
                .map_err(|error| error.to_string())?
            {
                ReservationOutcome::New { sequence } => {
                    self.note_reservation(
                        &coordinator_request_id,
                        command_fingerprint.clone(),
                        sequence,
                    );
                }
                ReservationOutcome::ReservedUnknown { .. } => {
                    return Err(
                        "launch request is reserved with an unknown outcome; refusing to dispatch it again"
                            .to_owned(),
                    );
                }
                ReservationOutcome::Completed { result, .. } => {
                    let process_id = result
                        .get("processId")
                        .and_then(Value::as_str)
                        .ok_or("completed launch receipt is missing processId")?
                        .parse::<SupervisorProcessId>()
                        .map_err(|_| "completed launch process ID is invalid")?;
                    return self
                        .managed(process_id)
                        .map(|managed| (process_id, managed))
                        .ok_or_else(|| {
                            "launch was already dispatched, but its process is no longer owned by this service; refusing to dispatch it again".to_owned()
                        });
                }
            }
        }
        let event_metadata = metadata.clone();
        let child = self
            .supervisor
            .start_with_metadata(
                &Command {
                    argv: std::iter::once(executable.to_owned()).chain(args).collect(),
                    cwd: cwd.clone(),
                },
                &policy,
                metadata,
            )
            .map_err(|e| e.to_string())?;
        let id = child.supervisor_id().ok_or("process was not registered")?;
        if let Err(error) = self
            .coordinator()
            .associate_process_with_id(
                if persistent {
                    format!("association:{coordinator_request_id}")
                } else {
                    coordinator_request_id.clone()
                },
                id.to_string(),
                attempt_id.clone(),
                attempt_generation,
            )
            .await
        {
            let _ = self.supervisor.shutdown_process(id, Duration::ZERO).await;
            return Err(error.to_string());
        }
        if persistent {
            if let Err(error) = self
                .coordinator()
                .complete_launch_reservation_with_id(
                    coordinator_request_id.clone(),
                    json!({"processId":id.to_string()}),
                )
                .await
            {
                let _ = self.supervisor.shutdown_process(id, Duration::ZERO).await;
                return Err(error.to_string());
            }
            self.complete_reservation(&coordinator_request_id);
        }
        let stream = child.into_event_stream().map_err(|e| e.to_string())?;
        let (events, _) = broadcast::channel(128);
        let history = Arc::new(Mutex::new(VecDeque::new()));
        let (input, mut input_rx) = mpsc::channel(64);
        let managed = Managed {
            events: events.clone(),
            history: history.clone(),
            input,
        };
        self.processes.lock().unwrap().insert(id, managed.clone());
        let coordinator = self.coordinator();
        tokio::spawn(async move {
            let mut stream = stream;
            loop {
                tokio::select! {
                    cmd = input_rx.recv() => match cmd {
                        Some(InputCommand::Write(bytes)) => { let _ = stream.write(&bytes).await; },
                        Some(InputCommand::Close) => { let _ = stream.close_stdin().await; },
                        None => {},
                    },
                    next = stream.next() => match next {
                        Ok(Some(event)) => {
                            let value = event_json(id, event, &event_metadata);
                            if let Some(value) = value {
                                {
                                    let mut h = history.lock().unwrap(); h.push_back(value.clone()); while h.len() > EVENT_HISTORY { h.pop_front(); }
                                }
                                let _ = events.send(value.clone());
                                if value["type"] == "exited" {
                                    let exit = value.get("receipt").map(Value::to_string).unwrap_or_else(|| "unknown".to_owned());
                                    let _ = coordinator.process_exited_with_id(
                                        format!("process-exit:{id}"),
                                        id.to_string(),
                                        exit,
                                    ).await;
                                    break;
                                }
                            }
                        }
                        _ => break,
                    }
                }
            }
        });
        Ok((id, managed))
    }

    fn managed(&self, id: SupervisorProcessId) -> Option<Managed> {
        self.processes.lock().unwrap().get(&id).cloned()
    }

    fn reporter_attempt(&self, token: &[u8]) -> Option<AttemptCapability> {
        self.capabilities
            .lock()
            .unwrap()
            .reporter_tokens
            .iter()
            .find_map(|(expected, attempt)| {
                constant_time_eq(expected, token).then_some(attempt.clone())
            })
    }

    fn canceller_attempt(&self, token: &[u8]) -> Option<AttemptCapability> {
        self.capabilities
            .lock()
            .unwrap()
            .cancel_tokens
            .iter()
            .find_map(|(expected, attempt)| {
                constant_time_eq(expected, token).then_some(attempt.clone())
            })
    }

    fn output_capability(&self, token: &[u8]) -> Option<(String, Instant)> {
        output_capability(
            &mut self.capabilities.lock().unwrap(),
            token,
            Instant::now(),
        )
    }

    fn capability_active(&self, scope: &str, attempt: Option<&AttemptCapability>) -> bool {
        let Some(attempt) = attempt else {
            return false;
        };
        let capabilities = self.capabilities.lock().unwrap();
        match scope {
            "report" => capabilities.reporter_tokens.values().any(|owner| {
                owner.attempt_id == attempt.attempt_id && owner.generation == attempt.generation
            }),
            "cancel" => capabilities.cancel_tokens.values().any(|owner| {
                owner.attempt_id == attempt.attempt_id && owner.generation == attempt.generation
            }),
            _ => false,
        }
    }
}

impl Clone for Managed {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            history: self.history.clone(),
            input: self.input.clone(),
        }
    }
}

fn role_allows(role: ClientRole, action: &str) -> bool {
    match role {
        ClientRole::View => matches!(action, "snapshot" | "status" | "subscribe"),
        ClientRole::Tui => matches!(
            action,
            "snapshot" | "status" | "subscribe" | "attach" | "wait" | "stop"
        ),
        ClientRole::Reporter => matches!(action, "report" | "pollAttempt"),
        ClientRole::Canceller => action == "cancelAttempt",
        ClientRole::OutputReader => matches!(action, "attach" | "wait"),
        ClientRole::Control => matches!(
            action,
            "snapshot"
                | "attach"
                | "wait"
                | "start"
                | "stdin"
                | "closeStdin"
                | "stop"
                | "registerRun"
                | "registerActivity"
                | "registerAttempt"
                | "setActivityStatus"
                | "setRunStatus"
                | "setActivityTimeout"
                | "registerFlowRun"
                | "updateFlowInvocations"
                | "claimFlowRun"
                | "advanceFlowStage"
                | "completeFlowRun"
                | "requestWorkerApproval"
                | "listPendingApprovals"
                | "waitApproval"
                | "resolveApproval"
                | "requestWorkerTool"
                | "listPendingToolRequests"
                | "claimToolRequest"
                | "waitToolRequest"
                | "resolveToolRequest"
                | "finishToolRequest"
                | "bindFlowAttempt"
                | "reconcileFlowAttempt"
                | "applyFlowReview"
                | "cancelFlowTask"
                | "cancelAttempt"
                | "shutdown"
                | "status"
                | "subscribe"
                | "revokeRole"
        ),
    }
}

fn coordinator_request_id(
    request_id: &Value,
    role: ClientRole,
    action: &str,
    target: Option<&str>,
) -> Option<String> {
    let request_id = request_id.as_str()?;
    if request_id.is_empty() || request_id.len() > 128 {
        return None;
    }
    let target = target.unwrap_or("manager");
    let scoped = format!("{}:{action}:{target}:{request_id}", role.as_str());
    (scoped.len() <= 256).then_some(scoped)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn redact_snapshot_for_role(mut snapshot: Value, role: ClientRole) -> Value {
    if role == ClientRole::Control {
        return snapshot;
    }
    if let Some(approvals) = snapshot.get_mut("approvals").and_then(Value::as_array_mut) {
        for approval in approvals {
            if let Some(fields) = approval.as_object_mut() {
                fields.insert("payload".to_owned(), Value::Null);
                fields.insert("grant".to_owned(), Value::Null);
                fields.insert("reason".to_owned(), Value::Null);
                fields.insert("payloadRedacted".to_owned(), json!(true));
            }
        }
    }
    if let Some(tool_requests) = snapshot
        .get_mut("toolRequests")
        .and_then(Value::as_array_mut)
    {
        for request in tool_requests {
            if let Some(fields) = request.as_object_mut() {
                fields.insert("arguments".to_owned(), Value::Null);
                fields.insert("result".to_owned(), Value::Null);
                fields.insert("error".to_owned(), Value::Null);
                fields.insert("argumentsRedacted".to_owned(), json!(true));
                fields.insert("resultRedacted".to_owned(), json!(true));
            }
        }
    }
    snapshot
}

fn register_output_capability(
    registry: &mut CapabilityRegistry,
    token: Vec<u8>,
    process_id: String,
    now: Instant,
    ttl: Duration,
) {
    registry
        .output_tokens
        .retain(|_, capability| capability.expires_at > now);
    if registry.output_tokens.len() >= MAX_OUTPUT_CAPABILITIES {
        if let Some(oldest) = registry
            .output_tokens
            .iter()
            .min_by_key(|(_, capability)| capability.issued_at)
            .map(|(token, _)| token.clone())
        {
            registry.output_tokens.remove(&oldest);
        }
    }
    registry.output_tokens.insert(
        token,
        OutputCapability {
            process_id,
            issued_at: now,
            expires_at: now + ttl,
        },
    );
}

fn output_capability(
    registry: &mut CapabilityRegistry,
    token: &[u8],
    now: Instant,
) -> Option<(String, Instant)> {
    registry
        .output_tokens
        .retain(|_, capability| capability.expires_at > now);
    registry
        .output_tokens
        .iter()
        .find_map(|(expected, capability)| {
            constant_time_eq(expected, token)
                .then(|| (capability.process_id.clone(), capability.expires_at))
        })
}

fn output_process(registry: &mut CapabilityRegistry, token: &[u8], now: Instant) -> Option<String> {
    output_capability(registry, token, now).map(|(process_id, _)| process_id)
}

fn event_matches_filter(event: &CoordinatorEvent, filter: &Value) -> bool {
    [
        ("runId", event.run_id.as_deref()),
        ("activityId", event.activity_id.as_deref()),
        ("attemptId", event.attempt_id.as_deref()),
    ]
    .into_iter()
    .all(|(field, actual)| {
        filter
            .get(field)
            .and_then(Value::as_str)
            .is_none_or(|expected| actual == Some(expected))
    })
}

fn subscribe(
    managed: Managed,
    id: SupervisorProcessId,
    tx: mpsc::Sender<Value>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (history, mut rx) = {
            let history = managed.history.lock().unwrap();
            let rx = managed.events.subscribe();
            (history.iter().cloned().collect::<Vec<_>>(), rx)
        };
        for event in history {
            if tx.send(event.clone()).await.is_err() {
                return;
            }
            if event["type"] == "exited" {
                return;
            }
        }
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let done = event["type"] == "exited";
                    if tx.send(event).await.is_err() || done {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let _ = tx
                        .send(json!({"type":"resyncRequired","processId":id.to_string()}))
                        .await;
                }
                Err(_) => break,
            }
        }
    })
}

fn event_json(
    id: SupervisorProcessId,
    event: ProcessEvent,
    metadata: &ManagedProcessMetadata,
) -> Option<Value> {
    let context = json!({
        "framework":metadata.framework(),"runLabel":metadata.run_label(),"projectId":metadata.project_id(),
        "flowRunId":metadata.flow_run_id(),"taskId":metadata.task_id(),"postId":metadata.post_id(),
        "workerId":metadata.worker_id(),"runtimeRunId":metadata.runtime_run_id()
    });
    match event {
        ProcessEvent::Started {
            process_id,
            executable,
        } => Some(
            json!({"type":"processStarted","processId":id.to_string(),"pid":process_id,"executable":executable,"metadata":context}),
        ),
        ProcessEvent::Stdout(bytes) => Some(
            json!({"type":"stdout","processId":id.to_string(),"data":encode_base64(&bytes),"metadata":context}),
        ),
        ProcessEvent::Stderr(bytes) => Some(
            json!({"type":"stderr","processId":id.to_string(),"data":encode_base64(&bytes),"metadata":context}),
        ),
        ProcessEvent::Exited(receipt) => Some(
            json!({"type":"exited","processId":id.to_string(),"receipt":receipt,"metadata":context}),
        ),
    }
}

fn process_id(request: &Value) -> Result<SupervisorProcessId, String> {
    request
        .get("processId")
        .and_then(Value::as_str)
        .ok_or("missing processId")?
        .parse()
}

async fn read_session_frame<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
    deadline: tokio::time::Instant,
    revocation: &mut Option<watch::Receiver<bool>>,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        if revocation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "role credential revoked",
            ));
        }
        if *shutdown.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Supervisor service is shutting down",
            ));
        }
        let read = tokio::time::timeout_at(deadline, read_frame(reader, line));
        if let Some(receiver) = revocation.as_mut() {
            tokio::select! {
                result = read => return result.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut,"authenticated session expired"))),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Err(io::Error::new(io::ErrorKind::ConnectionAborted,"Supervisor service is shutting down")); }
                }
                changed = receiver.changed() => {
                    if changed.is_err() || *receiver.borrow() {
                        return Err(io::Error::new(io::ErrorKind::PermissionDenied,"role credential revoked"));
                    }
                }
            }
        } else {
            tokio::select! {
                result = read => return result.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut,"authenticated session expired"))),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Err(io::Error::new(io::ErrorKind::ConnectionAborted,"Supervisor service is shutting down")); }
                }
            }
        }
    }
}

async fn read_frame<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> io::Result<()> {
    line.clear();
    let n = tokio::time::timeout(
        Duration::from_secs(60),
        reader.take(MAX_FRAME as u64 + 1).read_until(b'\n', line),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "request timed out"))??;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "socket closed",
        ));
    }
    if n > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds 1 MiB",
        ));
    }
    Ok(())
}
async fn write_json<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    value: &Value,
) -> io::Result<()> {
    writer
        .write_all(
            serde_json::to_string(value)
                .map_err(invalid_data)?
                .as_bytes(),
        )
        .await?;
    writer.write_all(b"\n").await
}
fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
type ReadyFileIdentity = (PathBuf, u64, u64);

fn write_ready_file(path: &Path, socket: &Path, epoch: &str) -> io::Result<ReadyFileIdentity> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let content = json!({
        "type":"ready",
        "epoch":epoch,
        "managerId":epoch,
        "pid":std::process::id(),
        "socket":socket.to_string_lossy(),
    });
    file.write_all(
        serde_json::to_string(&content)
            .map_err(invalid_data)?
            .as_bytes(),
    )?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    let metadata = file.metadata()?;
    Ok((path.to_owned(), metadata.dev(), metadata.ino()))
}

fn remove_ready_file(identity: ReadyFileIdentity) {
    use std::os::unix::fs::MetadataExt;
    let (path, dev, ino) = identity;
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
        if metadata.dev() == dev && metadata.ino() == ino && metadata.file_type().is_file() {
            let _ = std::fs::remove_file(path);
        }
    }
}
fn new_token() -> io::Result<Vec<u8>> {
    let mut random = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
    Ok(random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        .into_bytes())
}
fn coordinator_error(request_id: Value, error: CoordinatorError) -> Value {
    let code = match &error {
        CoordinatorError::NotFound(_) | CoordinatorError::InvalidParent(_) => "not_found",
        CoordinatorError::StaleGeneration => "stale_generation",
        CoordinatorError::TerminalTransition => "terminal_transition",
        CoordinatorError::InvalidReport => "invalid_report",
        CoordinatorError::IdempotencyConflict => "idempotency_conflict",
        CoordinatorError::Persistence => "persistence_failed",
        CoordinatorError::Workflow(_) => "workflow_error",
        CoordinatorError::Closed => "coordinator_closed",
    };
    json!({"type":"error","requestId":request_id,"code":code,"message":error.to_string()})
}
fn validate_context(context: &Value) -> Result<(), &'static str> {
    let object = context.as_object().ok_or("context must be a JSON object")?;
    if object.len() > 32 {
        return Err("context contains more than 32 fields");
    }
    for (key, value) in object {
        if key.len() > 128 {
            return Err("context key exceeds 128 bytes");
        }
        let text = value.as_str().ok_or("context values must be strings")?;
        let limit = if matches!(key.as_str(), "framework" | "runLabel") {
            128
        } else {
            2048
        };
        if text.len() > limit {
            return Err("context value exceeds its documented byte limit");
        }
        if text.chars().any(char::is_control) {
            return Err("context contains control characters");
        }
    }
    Ok(())
}
fn sanitize_report_detail(detail: Value) -> Result<Value, &'static str> {
    if detail.is_null() {
        return Ok(Value::Null);
    }
    let object = detail
        .as_object()
        .ok_or("detail must be null or a small safe object")?;
    if object.len() > 2 {
        return Err("detail may contain only reasonCode and retryable");
    }
    let mut safe = serde_json::Map::new();
    if let Some(reason) = object.get("reasonCode") {
        let reason = reason.as_str().ok_or("reasonCode must be a string")?;
        if reason.len() > 128
            || !reason
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err("reasonCode must be a short identifier");
        }
        safe.insert("reasonCode".to_owned(), json!(reason));
    }
    if let Some(retryable) = object.get("retryable") {
        if !retryable.is_boolean() {
            return Err("retryable must be a boolean");
        }
        safe.insert("retryable".to_owned(), retryable.clone());
    }
    if safe.len() != object.len() {
        return Err("detail may contain only reasonCode and retryable");
    }
    Ok(Value::Object(safe))
}
fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    let mut diff = expected.len() ^ actual.len();
    for i in 0..expected.len().max(actual.len()) {
        diff |= usize::from(
            expected.get(i).copied().unwrap_or(0) ^ actual.get(i).copied().unwrap_or(0),
        );
    }
    diff == 0
}

fn decode_base64(input: &str) -> Result<Vec<u8>, ()> {
    let mut out = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for b in input.bytes() {
        if b == b'=' {
            break;
        }
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err(()),
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Ok(out)
}
fn encode_base64(input: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::new();
    for c in input.chunks(3) {
        let a = c[0];
        let b = *c.get(1).unwrap_or(&0);
        let d = *c.get(2).unwrap_or(&0);
        s.push(T[(a >> 2) as usize] as char);
        s.push(T[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        s.push(if c.len() > 1 {
            T[(((b & 15) << 2) | (d >> 6)) as usize] as char
        } else {
            '='
        });
        s.push(if c.len() > 2 {
            T[(d & 63) as usize] as char
        } else {
            '='
        });
    }
    s
}

/// Read one role-scoped service token. Each client receives only its role's
/// private file; the file must be a regular mode-0600 file owned by this user.
pub fn read_token_file(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path.as_ref())?;
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "token file must have mode 0600",
        ));
    }
    use std::os::unix::fs::MetadataExt;
    // SAFETY: geteuid has no preconditions and returns the effective UID of this process.
    let current_uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_file() || metadata.uid() != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "token file must be a regular file owned by the current user",
        ));
    }
    let token = std::fs::read(path)?;
    let token = token.strip_suffix(b"\n").unwrap_or(&token);
    if token.len() < 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "role token file must contain at least 32 bytes",
        ));
    }
    Ok(token.to_vec())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialsPaths {
    pub view_token_file: PathBuf,
    pub control_token_file: PathBuf,
    pub tui_token_file: PathBuf,
}

/// Ask the Supervisor to issue private role-scoped credentials in a new
/// directory. Existing directories and files are never overwritten.
pub fn create_credentials_dir(path: impl AsRef<Path>) -> io::Result<CredentialsPaths> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let path = path.as_ref();
    std::fs::create_dir(path)?;
    let result = (|| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        let view = path.join("view.token");
        let control = path.join("control.token");
        let tui = path.join("tui.token");
        let mut issued = Vec::<String>::new();
        for token_path in [&view, &control, &tui] {
            let token = loop {
                let mut random = [0u8; 32];
                std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
                let candidate = random
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                if !issued.contains(&candidate) {
                    break candidate;
                }
            };
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(token_path)?;
            std::fs::set_permissions(token_path, std::fs::Permissions::from_mode(0o600))?;
            file.write_all(token.as_bytes())?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            issued.push(token);
        }
        Ok(CredentialsPaths {
            view_token_file: view,
            control_token_file: control,
            tui_token_file: tui,
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(path.join("view.token"));
        let _ = std::fs::remove_file(path.join("control.token"));
        let _ = std::fs::remove_file(path.join("tui.token"));
        let _ = std::fs::remove_dir(path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;

    async fn connect(
        service: &SupervisorService,
        role: &str,
        token: &str,
    ) -> (
        BufReader<tokio::net::unix::OwnedReadHalf>,
        tokio::net::unix::OwnedWriteHalf,
    ) {
        let (client, server) = UnixStream::pair().unwrap();
        tokio::spawn({
            let service = service.clone();
            async move {
                let _ = service.connection(server).await;
            }
        });
        let (read, mut write) = client.into_split();
        write_json(
            &mut write,
            &json!({"type":"auth","role":role,"token":token}),
        )
        .await
        .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "ready"
        );
        (reader, write)
    }

    async fn assert_auth_rejected(service: &SupervisorService, role: &str, token: &str) {
        let (client, server) = UnixStream::pair().unwrap();
        tokio::spawn({
            let service = service.clone();
            async move {
                let _ = service.connection(server).await;
            }
        });
        let (read, mut write) = client.into_split();
        let mut reader = BufReader::new(read);
        write_json(
            &mut write,
            &json!({"type":"auth","role":role,"token":token}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["type"], "error");
        assert_eq!(response["code"], "unauthorized");
        assert!(!line.contains(token), "auth errors must never echo secrets");
    }

    #[test]
    fn base64_round_trip_known_values() {
        for bytes in [b"".as_slice(), b"a", b"ab", b"abc", &[0, 255, 19]] {
            assert_eq!(decode_base64(&encode_base64(bytes)).unwrap(), bytes);
        }
    }
    #[test]
    fn token_comparison_checks_length() {
        assert!(constant_time_eq(
            b"12345678901234567890123456789012",
            b"12345678901234567890123456789012"
        ));
        assert!(!constant_time_eq(
            b"12345678901234567890123456789012",
            b"bad"
        ));
    }

    #[test]
    fn output_capabilities_expire_and_have_a_hard_size_bound() {
        let now = Instant::now();
        let mut registry = CapabilityRegistry::default();
        register_output_capability(
            &mut registry,
            b"short-lived".to_vec(),
            "process-expiring".to_owned(),
            now,
            Duration::from_millis(5),
        );
        assert_eq!(
            output_process(&mut registry, b"short-lived", now),
            Some("process-expiring".to_owned())
        );
        assert_eq!(
            output_process(
                &mut registry,
                b"short-lived",
                now + Duration::from_millis(5)
            ),
            None
        );

        let mut registry = CapabilityRegistry::default();
        for index in 0..=MAX_OUTPUT_CAPABILITIES {
            register_output_capability(
                &mut registry,
                format!("output-token-{index}").into_bytes(),
                format!("process-{index}"),
                now + Duration::from_nanos(index as u64),
                OUTPUT_CAPABILITY_TTL,
            );
        }
        assert_eq!(registry.output_tokens.len(), MAX_OUTPUT_CAPABILITIES);
        assert_eq!(output_process(&mut registry, b"output-token-0", now), None);
        assert_eq!(
            output_process(
                &mut registry,
                format!("output-token-{MAX_OUTPUT_CAPABILITIES}").as_bytes(),
                now + Duration::from_secs(1)
            ),
            Some(format!("process-{MAX_OUTPUT_CAPABILITIES}"))
        );
    }

    #[test]
    fn state_directory_lock_rejects_second_owner_and_releases_on_drop() {
        let parent = tempfile::tempdir().unwrap();
        let state = parent.path().join("state");
        let first = acquire_instance_lock(&state).unwrap();
        match acquire_instance_lock(&state) {
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::AddrInUse),
            Ok(_) => panic!("second owner must be rejected"),
        }
        drop(first);
        let second = acquire_instance_lock(&state).unwrap();
        drop(second);
    }

    #[test]
    fn role_token_file_contains_only_one_role_credential() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let view_path = dir.path().join("view.token");
        let control_path = dir.path().join("control.token");
        std::fs::write(&view_path, "v".repeat(32)).unwrap();
        std::fs::write(&control_path, "c".repeat(32)).unwrap();
        std::fs::set_permissions(&view_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&control_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(read_token_file(&view_path).unwrap(), b"v".repeat(32));
        assert_eq!(read_token_file(&control_path).unwrap(), b"c".repeat(32));
        assert_ne!(
            read_token_file(&view_path).unwrap(),
            read_token_file(&control_path).unwrap()
        );
    }

    #[test]
    fn supervisor_issues_distinct_private_role_files_without_overwrite() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let parent = tempfile::tempdir().unwrap();
        let dir = parent.path().join("issued");
        let paths = create_credentials_dir(&dir).unwrap();
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let files = [
            &paths.view_token_file,
            &paths.control_token_file,
            &paths.tui_token_file,
        ];
        let tokens = files
            .iter()
            .map(|file| {
                let metadata = std::fs::metadata(file).unwrap();
                assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
                assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
                read_token_file(file).unwrap()
            })
            .collect::<Vec<_>>();
        assert!(tokens.iter().all(|token| token.len() == 64));
        assert_ne!(tokens[0], tokens[1]);
        assert_ne!(tokens[0], tokens[2]);
        assert_ne!(tokens[1], tokens[2]);
        let before = std::fs::read(&paths.control_token_file).unwrap();
        assert_eq!(
            create_credentials_dir(&dir).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(&paths.control_token_file).unwrap(), before);
    }

    #[tokio::test]
    async fn view_credential_reads_metadata_but_cannot_read_output_or_control() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut reader, mut writer) = connect(&service, "view", &"v".repeat(32)).await;
        write_json(&mut writer, &json!({"type":"snapshot","requestId":"snap"}))
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "snapshot"
        );
        line.clear();
        let request = json!({"type":"start","requestId":"r1","command":{"executable":"true","args":[]},"policy":crate::Policy::default()});
        write_json(&mut writer, &request).await.unwrap();
        reader.read_line(&mut line).await.unwrap();
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(response["code"], "forbidden");
        assert_eq!(response["requestId"], "r1");
        line.clear();
        write_json(
            &mut writer,
            &json!({"type":"attach","requestId":"attach","processId":"stale-process-id"}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );
    }

    #[tokio::test]
    async fn control_can_report_waiting_state_and_activity_timeout() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut reader, mut writer) = connect(&service, "control", &"c".repeat(32)).await;
        let mut line = String::new();
        write_json(
            &mut writer,
            &json!({"type":"registerRun","requestId":"r","context":{}}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let run_id = serde_json::from_str::<Value>(&line).unwrap()["run"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        line.clear();
        write_json(&mut writer, &json!({"type":"setRunStatus","requestId":"run-status","runId":run_id,"status":"running"})).await.unwrap();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["run"]["status"],
            "running"
        );
        line.clear();
        write_json(
            &mut writer,
            &json!({"type":"registerActivity","requestId":"a","runId":run_id,"kind":"worker"}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let activity_id = serde_json::from_str::<Value>(&line).unwrap()["activity"]["activityId"]
            .as_str()
            .unwrap()
            .to_owned();
        line.clear();
        write_json(&mut writer, &json!({"type":"setActivityStatus","requestId":"waiting","activityId":activity_id,"status":"waitingForApproval","reason":"review"})).await.unwrap();
        reader.read_line(&mut line).await.unwrap();
        let waiting: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(waiting["type"], "activityStatus");
        assert_eq!(waiting["activity"]["status"], "waitingForApproval");
        assert_eq!(waiting["activity"]["waitingReason"], "review");
        line.clear();
        write_json(&mut writer, &json!({"type":"setActivityTimeout","requestId":"timeout","activityId":activity_id,"timeoutMs":10})).await.unwrap();
        reader.read_line(&mut line).await.unwrap();
        let timeout: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(timeout["type"], "activityTimeoutSet");
        tokio::time::sleep(Duration::from_millis(30)).await;
        line.clear();
        write_json(
            &mut writer,
            &json!({"type":"snapshot","requestId":"snapshot"}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let snapshot: Value = serde_json::from_str(&line).unwrap();
        let activity = snapshot["activities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|value| value["activityId"] == activity_id)
            .unwrap();
        assert_eq!(activity["status"], "timedOut");
        assert_eq!(activity["health"], "stale");
    }

    #[tokio::test]
    async fn activity_timeout_is_discarded_when_a_new_attempt_generation_starts() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut reader, mut writer) = connect(&service, "control", &"c".repeat(32)).await;
        let mut line = String::new();
        write_json(
            &mut writer,
            &json!({"type":"registerRun","requestId":"r","context":{}}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let run_id = serde_json::from_str::<Value>(&line).unwrap()["run"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        line.clear();
        write_json(
            &mut writer,
            &json!({"type":"registerActivity","requestId":"a","runId":run_id,"kind":"worker"}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let activity_id = serde_json::from_str::<Value>(&line).unwrap()["activity"]["activityId"]
            .as_str()
            .unwrap()
            .to_owned();
        line.clear();
        write_json(&mut writer, &json!({"type":"setActivityTimeout","requestId":"timeout","activityId":activity_id,"timeoutMs":10})).await.unwrap();
        reader.read_line(&mut line).await.unwrap();
        line.clear();
        write_json(
            &mut writer,
            &json!({"type":"registerAttempt","requestId":"attempt","activityId":activity_id}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        line.clear();
        write_json(
            &mut writer,
            &json!({"type":"snapshot","requestId":"snapshot"}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let snapshot: Value = serde_json::from_str(&line).unwrap();
        let activity = snapshot["activities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|value| value["activityId"] == activity_id)
            .unwrap();
        assert_eq!(activity["status"], "running");
    }

    #[tokio::test]
    async fn invalid_token_is_rejected() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (client, server) = UnixStream::pair().unwrap();
        tokio::spawn({
            let service = service.clone();
            async move {
                let _ = service.connection(server).await;
            }
        });
        let (read, mut write) = client.into_split();
        let mut reader = BufReader::new(read);
        write_json(
            &mut write,
            &json!({"type":"auth","role":"control","token":"wrong"}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "unauthorized"
        );
    }

    #[tokio::test]
    async fn control_credential_starts_streams_and_reaps_process() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut reader, mut writer) = connect(&service, "control", &"c".repeat(32)).await;
        let request = json!({"type":"start","requestId":"s1","command":{"executable":"/bin/sh","args":["-c","read line; echo got:$line; echo err:$line >&2"]},"metadata":{"framework":"test"},"policy":crate::Policy::default()});
        write_json(&mut writer, &request).await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let start: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(start["type"], "started");
        let id = start["processId"].as_str().unwrap().to_owned();
        write_json(
            &mut writer,
            &json!({"type":"stdin","requestId":"i1","processId":id,"data":"aGVsbG8K"}),
        )
        .await
        .unwrap();
        let mut got_out = false;
        let mut got_err = false;
        let mut got_exit = false;
        while !got_exit {
            line.clear();
            tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            let event: Value = serde_json::from_str(&line).unwrap();
            if event["type"] == "stdout" {
                got_out |= decode_base64(event["data"].as_str().unwrap())
                    .unwrap()
                    .windows(10)
                    .any(|v| v == b"got:hello\n");
            }
            if event["type"] == "stderr" {
                got_err |= decode_base64(event["data"].as_str().unwrap())
                    .unwrap()
                    .windows(10)
                    .any(|v| v == b"err:hello\n");
            }
            got_exit = event["type"] == "exited";
        }
        assert!(got_out && got_err);
    }

    #[tokio::test]
    async fn output_reader_capability_attaches_only_to_its_process() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut control_reader, mut control_writer) =
            connect(&service, "control", &"c".repeat(32)).await;
        write_json(
            &mut control_writer,
            &json!({"type":"start","requestId":"start","command":{"executable":"/bin/sleep","args":["30"]},"policy":crate::Policy::default()}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        control_reader.read_line(&mut line).await.unwrap();
        let started: Value = serde_json::from_str(&line).unwrap();
        let process_id = started["processId"].as_str().unwrap().to_owned();
        let output_token = started["outputToken"].as_str().unwrap().to_owned();
        let (mut output_reader, mut output_writer) =
            connect(&service, "output", &output_token).await;
        write_json(
            &mut output_writer,
            &json!({"type":"attach","requestId":"own","processId":process_id}),
        )
        .await
        .unwrap();
        line.clear();
        output_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "attached"
        );
        write_json(
            &mut output_writer,
            &json!({"type":"attach","requestId":"other","processId":"not-this-process"}),
        )
        .await
        .unwrap();
        loop {
            line.clear();
            output_reader.read_line(&mut line).await.unwrap();
            let response: Value = serde_json::from_str(&line).unwrap();
            if response["requestId"] == "other" {
                assert_eq!(response["code"], "forbidden");
                break;
            }
        }
        write_json(
            &mut control_writer,
            &json!({"type":"stop","requestId":"stop","processId":process_id,"graceMs":0}),
        )
        .await
        .unwrap();
        loop {
            line.clear();
            control_reader.read_line(&mut line).await.unwrap();
            if serde_json::from_str::<Value>(&line).unwrap()["requestId"] == "stop" {
                break;
            }
        }
    }

    #[tokio::test]
    async fn tui_stop_is_broadcast_and_process_is_reaped() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut reader, mut writer) = connect(&service, "control", &"c".repeat(32)).await;
        write_json(&mut writer,&json!({"type":"start","requestId":"s","command":{"executable":"/bin/sleep","args":["30"]},"policy":crate::Policy::default()})).await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let start: Value = serde_json::from_str(&line).unwrap();
        let id = start["processId"].as_str().unwrap();
        let (mut tui_reader, mut tui_writer) = connect(&service, "tui", &"t".repeat(32)).await;
        write_json(
            &mut tui_writer,
            &json!({"type":"attach","requestId":"a","processId":id}),
        )
        .await
        .unwrap();
        let mut attached = String::new();
        tui_reader.read_line(&mut attached).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&attached).unwrap()["type"],
            "attached"
        );
        write_json(&mut tui_writer,&json!({"type":"stop","requestId":"stop","processId":id,"actor":"spoofed-client","graceMs":0})).await.unwrap();
        let mut notified = false;
        let mut exited = false;
        while !exited {
            line.clear();
            tokio::time::timeout(Duration::from_secs(5), tui_reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            let event: Value = serde_json::from_str(&line).unwrap();
            if event["type"] == "stop_requested" {
                notified = event["actor"] == "tui" && event["processId"] == id;
            }
            exited = event["type"] == "exited";
        }
        assert!(
            notified,
            "TUI stop attribution should be observable before exit"
        );
    }

    #[tokio::test]
    async fn attach_after_fast_exit_replays_terminal_event() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut owner_reader, mut owner_writer) =
            connect(&service, "control", &"c".repeat(32)).await;
        write_json(&mut owner_writer,&json!({"type":"start","requestId":"s","command":{"executable":"/bin/sh","args":["-c","true"]},"metadata":{"postId":"board:flowboard%3A...:post:00000000000000000017","workerId":"worker-1","runtimeRunId":"runtime-1"},"policy":crate::Policy::default()})).await.unwrap();
        let mut line = String::new();
        owner_reader.read_line(&mut line).await.unwrap();
        let started: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(started["type"], "started", "start response: {started}");
        let id = started["processId"].as_str().unwrap().to_owned();
        let output_token = started["outputToken"].as_str().unwrap().to_owned();
        drop(owner_writer);
        drop(owner_reader);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (mut reader, mut writer) = connect(&service, "output", &output_token).await;
        write_json(
            &mut writer,
            &json!({"type":"attach","requestId":"a","processId":id}),
        )
        .await
        .unwrap();
        let mut attached = String::new();
        reader.read_line(&mut attached).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&attached).unwrap()["type"],
            "attached"
        );
        let mut saw_exit = false;
        while !saw_exit {
            line.clear();
            tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            let event: Value = serde_json::from_str(&line).unwrap();
            if event["type"] == "exited" {
                assert_eq!(event["metadata"]["workerId"], "worker-1");
                assert_eq!(event["metadata"]["runtimeRunId"], "runtime-1");
                saw_exit = true;
            }
        }
    }

    #[tokio::test]
    async fn authenticated_session_expires_after_injected_ttl() {
        assert!(SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32)
        )
        .unwrap()
        .with_session_ttl(Duration::ZERO)
        .is_err());
        assert!(SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32)
        )
        .unwrap()
        .with_session_ttl(Duration::from_secs(24 * 60 * 60 + 1))
        .is_err());
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap()
        .with_session_ttl(Duration::from_millis(50))
        .unwrap();
        let (mut reader, _writer) = connect(&service, "control", &"c".repeat(32)).await;
        let mut line = String::new();
        let count = tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 0, "expired authenticated socket closes");
    }

    #[tokio::test]
    async fn control_revokes_view_and_tui_sessions_but_not_control() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut view_reader, _view_writer) = connect(&service, "view", &"v".repeat(32)).await;
        let (mut tui_reader, mut tui_writer) = connect(&service, "tui", &"t".repeat(32)).await;
        let (mut control_reader, mut control_writer) =
            connect(&service, "control", &"c".repeat(32)).await;

        write_json(
            &mut tui_writer,
            &json!({"type":"revokeRole","requestId":"tui-cannot-revoke","role":"view"}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        tui_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );

        for (request_id, role) in [("rv", "view"), ("rt", "tui")] {
            write_json(
                &mut control_writer,
                &json!({"type":"revokeRole","requestId":request_id,"role":role}),
            )
            .await
            .unwrap();
            let mut line = String::new();
            control_reader.read_line(&mut line).await.unwrap();
            let response: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(response["type"], "roleRevoked");
            assert_eq!(response["role"], role);
        }

        for reader in [&mut view_reader, &mut tui_reader] {
            let mut line = String::new();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        }
        assert_auth_rejected(&service, "view", &"v".repeat(32)).await;
        assert_auth_rejected(&service, "tui", &"t".repeat(32)).await;

        write_json(
            &mut control_writer,
            &json!({"type":"revokeRole","requestId":"rc","role":"control"}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );
        write_json(
            &mut control_writer,
            &json!({"type":"unknown","requestId":"still-live"}),
        )
        .await
        .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );
    }

    #[tokio::test]
    async fn reporter_scope_is_attempt_bound_and_manager_scopes_cancel_and_launch() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut manager_reader, mut manager_writer) =
            connect(&service, "control", &"c".repeat(32)).await;
        let mut line = String::new();
        write_json(&mut manager_writer, &json!({"type":"registerRun","requestId":"r","context":{"flowRunId":"flow-1","postId":"board/flow/post-00000000000000000021"}})).await.unwrap();
        manager_reader.read_line(&mut line).await.unwrap();
        let run_id = serde_json::from_str::<Value>(&line).unwrap()["run"]["runId"]
            .as_str()
            .unwrap()
            .to_owned();
        line.clear();
        write_json(
            &mut manager_writer,
            &json!({"type":"registerActivity","requestId":"a","runId":run_id,"kind":"worker"}),
        )
        .await
        .unwrap();
        manager_reader.read_line(&mut line).await.unwrap();
        let activity_id = serde_json::from_str::<Value>(&line).unwrap()["activity"]["activityId"]
            .as_str()
            .unwrap()
            .to_owned();
        line.clear();
        write_json(
            &mut manager_writer,
            &json!({"type":"registerAttempt","requestId":"attempt","activityId":activity_id}),
        )
        .await
        .unwrap();
        manager_reader.read_line(&mut line).await.unwrap();
        let attempt = serde_json::from_str::<Value>(&line).unwrap();
        let attempt_id = attempt["attempt"]["attemptId"].as_str().unwrap().to_owned();
        let generation = attempt["attempt"]["generation"].as_u64().unwrap();
        let reporter_token = attempt["reporterToken"].as_str().unwrap().to_owned();
        let cancel_token = attempt["cancelToken"].as_str().unwrap().to_owned();

        let (mut reporter_reader, mut reporter_writer) =
            connect(&service, "reporter", &reporter_token).await;
        let (mut canceller_reader, mut canceller_writer) =
            connect(&service, "canceller", &cancel_token).await;
        line.clear();
        write_json(
            &mut canceller_writer,
            &json!({"type":"cancelAttempt","requestId":"wrong-cancel","attemptId":"other-attempt","generation":generation}),
        )
        .await
        .unwrap();
        canceller_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );
        line.clear();
        write_json(
            &mut canceller_writer,
            &json!({"type":"cancelAttempt","requestId":"cancel","attemptId":attempt_id,"generation":generation}),
        )
        .await
        .unwrap();
        canceller_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["attempt"]["cancellation"],
            "requested"
        );

        line.clear();
        write_json(
            &mut reporter_writer,
            &json!({"type":"pollAttempt","requestId":"poll","attemptId":attempt_id,"generation":generation}),
        )
        .await
        .unwrap();
        reporter_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["attempt"]["cancellation"],
            "delivered"
        );

        line.clear();
        write_json(&mut reporter_writer, &json!({"type":"report","requestId":"wrong","attemptId":"some-other-attempt","generation":generation,"status":"running"})).await.unwrap();
        reporter_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );

        line.clear();
        write_json(
            &mut reporter_writer,
            &json!({"type":"start","requestId":"no-launch"}),
        )
        .await
        .unwrap();
        reporter_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );

        line.clear();
        write_json(&mut reporter_writer, &json!({"type":"report","requestId":"unsafe","attemptId":attempt_id,"generation":generation,"status":"running","detail":{"prompt":"secret"}})).await.unwrap();
        reporter_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "invalid_report"
        );
        line.clear();
        write_json(&mut reporter_writer, &json!({"type":"report","requestId":"done","attemptId":attempt_id,"generation":generation,"status":"cancelled","detail":{"reasonCode":"operator_cancelled"}})).await.unwrap();
        reporter_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "reported"
        );
        assert_auth_rejected(&service, "reporter", &reporter_token).await;
        assert_auth_rejected(&service, "canceller", &cancel_token).await;

        line.clear();
        write_json(
            &mut manager_writer,
            &json!({"type":"snapshot","requestId":"snap"}),
        )
        .await
        .unwrap();
        manager_reader.read_line(&mut line).await.unwrap();
        let snapshot: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(snapshot["runs"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["attempts"][0]["status"], "cancelled");
        assert!(!line.contains(&reporter_token));
    }

    #[tokio::test]
    async fn only_control_can_request_service_shutdown() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let mut shutdown_rx = service.shutdown.subscribe();
        let (mut tui_reader, mut tui_writer) = connect(&service, "tui", &"t".repeat(32)).await;
        write_json(
            &mut tui_writer,
            &json!({"type":"shutdown","requestId":"denied"}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        tui_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["code"],
            "forbidden"
        );
        assert!(!*shutdown_rx.borrow());

        let (mut control_reader, mut control_writer) =
            connect(&service, "control", &"c".repeat(32)).await;
        write_json(
            &mut control_writer,
            &json!({"type":"shutdown","requestId":"allowed","graceMs":0}),
        )
        .await
        .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "shutdownComplete"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), shutdown_rx.changed())
                .await
                .is_ok()
        );
        assert!(*shutdown_rx.borrow());
    }

    #[tokio::test]
    async fn subscribe_replays_from_epoch_cursor_and_resyncs_on_epoch_mismatch() {
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let (mut viewer, mut view_writer) = connect(&service, "view", &"v".repeat(32)).await;
        let (mut manager, mut manager_writer) = connect(&service, "control", &"c".repeat(32)).await;

        write_json(
            &mut view_writer,
            &json!({"type":"snapshot","requestId":"snapshot"}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        viewer.read_line(&mut line).await.unwrap();
        let initial: Value = serde_json::from_str(&line).unwrap();
        let epoch = initial["epoch"].as_str().unwrap().to_owned();
        let cursor = initial["cursor"].as_u64().unwrap();
        assert_eq!(initial["managerId"], epoch);
        assert!(initial["counts"].is_object());

        line.clear();
        write_json(
            &mut view_writer,
            &json!({"type":"subscribe","requestId":"sub","epoch":epoch,"cursor":cursor}),
        )
        .await
        .unwrap();
        viewer.read_line(&mut line).await.unwrap();
        let subscribed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(subscribed["type"], "subscribed");
        assert_eq!(subscribed["resyncRequired"], false);

        write_json(
            &mut manager_writer,
            &json!({"type":"registerRun","requestId":"run","context":{"flowRunId":"flow-1"}}),
        )
        .await
        .unwrap();
        let mut manager_line = String::new();
        manager.read_line(&mut manager_line).await.unwrap();
        line.clear();
        viewer.read_line(&mut line).await.unwrap();
        let event: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(event["type"], "coordinatorEvent");
        assert_eq!(event["event"]["kind"], "runRegistered");
        assert!(event["event"]["cursor"].as_u64().unwrap() > cursor);

        line.clear();
        write_json(
            &mut view_writer,
            &json!({"type":"subscribe","requestId":"old-epoch","epoch":"stale-epoch","cursor":0}),
        )
        .await
        .unwrap();
        viewer.read_line(&mut line).await.unwrap();
        let resync: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(resync["type"], "resyncRequired");
        assert_eq!(resync["snapshot"]["epoch"], epoch);
    }

    #[tokio::test]
    async fn readiness_status_and_shutdown_track_one_service_owner() {
        use tokio::io::AsyncBufReadExt;
        use tokio::net::UnixStream;

        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("service.sock");
        let ready = parent.path().join("service.ready");
        let _lock = acquire_instance_lock(parent.path()).unwrap();
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let ready_json: Value = serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
        assert_eq!(ready_json["type"], "ready");
        assert_eq!(ready_json["managerId"], ready_json["epoch"]);
        assert_eq!(ready_json["socket"], socket.to_string_lossy().as_ref());

        let stream = UnixStream::connect(&socket).await.unwrap();
        let (read, mut write) = stream.into_split();
        write_json(
            &mut write,
            &json!({"type":"auth","role":"control","token":"c".repeat(32)}),
        )
        .await
        .unwrap();
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "ready"
        );
        line.clear();
        write_json(&mut write, &json!({"type":"status","requestId":"status"}))
            .await
            .unwrap();
        reader.read_line(&mut line).await.unwrap();
        let status: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(status["type"], "status");
        assert_eq!(status["epoch"], ready_json["epoch"]);
        assert_eq!(status["counts"]["processes"], 0);
        assert_eq!(status["workflowDispatch"]["enabled"], false);
        assert!(status["workflowDispatch"]["pendingReason"]
            .as_str()
            .unwrap()
            .contains("worker adapter is configured"));
        line.clear();
        write_json(
            &mut write,
            &json!({"type":"shutdown","requestId":"shutdown","graceMs":0}),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "shutdownComplete"
        );
        server.await.unwrap().unwrap();
        assert!(!ready.exists());
        assert!(!socket.exists());
    }

    #[tokio::test]
    async fn durable_start_reservation_survives_lost_reply_and_restart_without_redispatch() {
        use tokio::io::AsyncBufReadExt;

        async fn call(socket: &Path, role: &str, token: &str, request: Value) -> Value {
            let stream = UnixStream::connect(socket).await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            write_json(
                &mut write,
                &json!({"type":"auth","role":role,"token":token}),
            )
            .await
            .unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["type"],
                "ready"
            );
            line.clear();
            write_json(&mut write, &request).await.unwrap();
            reader.read_line(&mut line).await.unwrap();
            serde_json::from_str(&line).unwrap()
        }

        let parent = tempfile::tempdir().unwrap();
        let state_dir = parent.path().join("state");
        let socket = parent.path().join("service.sock");
        let ready = parent.path().join("service.ready");
        let control = "c".repeat(32);
        let view = "v".repeat(32);
        let tui = "t".repeat(32);
        let service = SupervisorService::new(Arc::new(Supervisor::new()), &view, &control, &tui)
            .unwrap()
            .with_state_dir(&state_dir);
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let registered_run = call(
            &socket,
            "control",
            &control,
            json!({"type":"registerRun","requestId":"run-r1","context":{"flowRunId":"flow-r1"}}),
        )
        .await;
        let run_id = registered_run["run"]["runId"].as_str().unwrap();
        let registered_activity = call(
            &socket,
            "control",
            &control,
            json!({"type":"registerActivity","requestId":"activity-r1","runId":run_id,"kind":"worker"}),
        )
        .await;
        let activity_id = registered_activity["activity"]["activityId"]
            .as_str()
            .unwrap();
        let registered_attempt = call(
            &socket,
            "control",
            &control,
            json!({"type":"registerAttempt","requestId":"attempt-r1","activityId":activity_id}),
        )
        .await;
        let attempt_id = registered_attempt["attempt"]["attemptId"].as_str().unwrap();
        let generation = registered_attempt["attempt"]["generation"]
            .as_u64()
            .unwrap();
        let start_request = json!({
            "type":"start",
            "requestId":"lost-reply-start",
            "attemptId":attempt_id,
            "generation":generation,
            "command":{"executable":"/bin/sh","args":["-c","true"]},
            "metadata":{"framework":"test","flowRunId":"flow-r1"},
            "policy":crate::Policy::default()
        });

        // Send the first launch request and close the client before reading its
        // reply. Retrying the identical request must return its process, not
        // dispatch another child.
        let stream = UnixStream::connect(&socket).await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        write_json(
            &mut write,
            &json!({"type":"auth","role":"control","token":control}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "ready"
        );
        write_json(&mut write, &start_request).await.unwrap();
        drop(write);
        drop(reader);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let retry = call(&socket, "control", &control, start_request.clone()).await;
        assert_eq!(retry["type"], "started", "retry response: {retry}");
        let process_id = retry["processId"].as_str().unwrap().to_owned();
        let snapshot = call(
            &socket,
            "control",
            &control,
            json!({"type":"snapshot","requestId":"snapshot-before-restart"}),
        )
        .await;
        assert_eq!(snapshot["processes"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["processes"][0]["processId"], process_id);

        let mut uncertain_start = start_request.clone();
        uncertain_start["requestId"] = json!("uncertain-start");
        uncertain_start["command"]["executable"] = json!("/no/such/supervisor-fixture");
        let uncertain_response = call(&socket, "control", &control, uncertain_start.clone()).await;
        assert_eq!(uncertain_response["type"], "error");

        let shut = call(
            &socket,
            "control",
            &control,
            json!({"type":"shutdown","requestId":"shutdown-first","graceMs":0}),
        )
        .await;
        assert_eq!(
            shut["type"], "shutdownComplete",
            "shutdown response: {shut}"
        );
        server.await.unwrap().unwrap();
        assert!(!ready.exists());

        let service = SupervisorService::new(Arc::new(Supervisor::new()), &view, &control, &tui)
            .unwrap()
            .with_state_dir(&state_dir);
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let status = call(
            &socket,
            "view",
            &view,
            json!({"type":"status","requestId":"status-after-restart"}),
        )
        .await;
        assert_ne!(status["epoch"], snapshot["epoch"]);
        assert_eq!(status["recovery"]["previousEpoch"], snapshot["epoch"]);
        assert_eq!(
            status["recovery"]["attempts"][0]["classification"],
            "unverified"
        );
        assert_eq!(
            status["recovery"]["diagnostics"]["pendingCommandReservations"],
            1
        );

        let retry_after_restart = call(&socket, "control", &control, start_request).await;
        assert_eq!(retry_after_restart["type"], "error");
        assert!(retry_after_restart["message"]
            .as_str()
            .unwrap()
            .contains("no longer owned"));
        let unresolved_retry = call(&socket, "control", &control, uncertain_start).await;
        assert_eq!(unresolved_retry["type"], "error");
        assert!(unresolved_retry["message"]
            .as_str()
            .unwrap()
            .contains("refusing to dispatch it again"));
        let after_restart = call(
            &socket,
            "control",
            &control,
            json!({"type":"snapshot","requestId":"snapshot-after-restart"}),
        )
        .await;
        assert!(after_restart["processes"].as_array().unwrap().is_empty());
        let shutdown = call(
            &socket,
            "control",
            &control,
            json!({"type":"shutdown","requestId":"shutdown-second","graceMs":0}),
        )
        .await;
        assert_eq!(shutdown["type"], "shutdownComplete");
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn service_owned_dispatcher_cancels_worker_and_reconciles_flowrun_without_client() {
        use std::collections::BTreeMap;

        let parent = tempfile::tempdir().unwrap();
        let state_dir = parent.path().join("state");
        let socket = parent.path().join("service.sock");
        let ready = parent.path().join("service.ready");
        let worker = r#"
import json, sys
envelope = json.loads(sys.stdin.readline())
print(json.dumps({
    'type': 'roo.runtime.worker.started',
    'attemptId': envelope['attemptId'], 'generation': envelope['generation'],
    'sourceCursor': 1, 'status': 'running', 'reasonCode': 'worker_started'
}), flush=True)
print(json.dumps({'type':'roo.runtime.worker.tool.requested','protocolVersion':1,
    'attemptId':envelope['attemptId'],'generation':envelope['generation'],
    'requestId':'tool-cancel','toolName':'invoke','arguments':{'action':'test'},'timeoutMs':30000}), flush=True)
while True:
    command = json.loads(sys.stdin.readline())
    if command.get('type') == 'tool.result':
        assert command['state'] == 'cancelled'
        break
print(json.dumps({
    'type': 'roo.runtime.worker.completed',
    'attemptId': envelope['attemptId'], 'generation': envelope['generation'],
    'sourceCursor': 2, 'status': 'cancelled', 'reasonCode': 'worker_cancelled'
}), flush=True)
"#;
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap()
        .with_state_dir(&state_dir)
        .with_workflow_worker(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), worker.into()],
            None,
            BTreeMap::new(),
        );
        let coordinator_service = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let (mut reader, mut writer) =
            connect(&coordinator_service, "control", &"c".repeat(32)).await;
        let projection = json!({
            "flowId":"flow-worker-e2e",
            "boardId":"board-worker-e2e",
            "flowRevision":0,
            "tasks":[
                {"taskId":"task-worker-e2e","postId":"post-worker-e2e",
                "title":"worker e2e","dependencies":[],"assignee":null,
                "state":"ready","waitingReason":null,"attemptId":null,
                "generation":null,"lastSourceCursor":0},
                {"taskId":"task-worker-sibling-e2e","postId":"post-worker-sibling-e2e",
                "title":"worker sibling e2e","dependencies":[],"assignee":null,
                "state":"ready","waitingReason":null,"attemptId":null,
                "generation":null,"lastSourceCursor":0}
            ]
        });
        write_json(
            &mut writer,
            &json!({
                "type":"registerFlowRun","requestId":"register-worker-e2e",
                "projection":projection,"coordinatorId":"supervisor-worker-e2e",
                "invocations":{
                    "task-worker-e2e":{
                        "projectId":"project-worker-e2e",
                        "runtimeOptions":{"checkpointEndpoint":"http://127.0.0.1/checkpoints"},
                        "graph":{"id":"worker-e2e"},"input":{"messages":[]}
                    },
                    "task-worker-sibling-e2e":{
                        "projectId":"project-worker-e2e",
                        "runtimeOptions":{"checkpointEndpoint":"http://127.0.0.1/checkpoints"},
                        "graph":{"id":"worker-sibling-e2e"},"input":{"messages":[]}
                    }
                }
            }),
        )
        .await
        .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "flowRunRegistered"
        );
        line.clear();
        write_json(
            &mut writer,
            &json!({
                "type":"claimFlowRun","requestId":"claim-worker-e2e",
                "flowId":"flow-worker-e2e","coordinatorId":"supervisor-worker-e2e",
                "expectedFence":0
            }),
        )
        .await
        .unwrap();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "flowRunClaimed"
        );
        drop(reader);
        drop(writer);

        let running_task = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let snapshot = coordinator_service.coordinator().snapshot();
                if let Some(flow) = snapshot.flow_runs.first() {
                    let task = &flow.tasks["task-worker-e2e"];
                    let sibling = &flow.tasks["task-worker-sibling-e2e"];
                    if task.state == crate::workflow::WorkflowTaskState::WaitingForApproval
                        && task.process_id.is_some()
                        && sibling.state == crate::workflow::WorkflowTaskState::WaitingForApproval
                        && sibling.process_id.is_some()
                    {
                        break (
                            flow.coordinator_fence,
                            task.attempt_id.clone().unwrap(),
                            task.generation.unwrap(),
                            sibling.attempt_id.clone().unwrap(),
                            sibling.generation.unwrap(),
                        );
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            running_task.is_ok(),
            "Supervisor dispatcher launches the configured worker; state={:?}",
            coordinator_service.coordinator().snapshot().flow_runs
        );
        let (fence, attempt_id, generation, sibling_attempt_id, sibling_generation) =
            running_task.unwrap();
        let running_snapshot = coordinator_service.coordinator().snapshot();
        let generic_attempt = running_snapshot
            .attempts
            .iter()
            .find(|attempt| attempt.attempt_id == attempt_id)
            .expect("FlowRun worker attempt appears in generic Supervisor attempts");
        assert_eq!(generic_attempt.generation, generation);
        assert_ne!(generic_attempt.run_id, "flow-worker-e2e");
        assert!(running_snapshot.processes.iter().any(|process| {
            process.attempt_id.as_deref() == Some(attempt_id.as_str()) && process.alive
        }));
        assert!(running_snapshot.processes.iter().any(|process| {
            process.attempt_id.as_deref() == Some(sibling_attempt_id.as_str()) && process.alive
        }));
        let (mut cancel_reader, mut cancel_writer) =
            connect(&coordinator_service, "control", &"c".repeat(32)).await;
        write_json(
            &mut cancel_writer,
            &json!({
                "type":"cancelFlowTask","requestId":"cancel-worker-e2e",
                "flowId":"flow-worker-e2e","taskId":"task-worker-e2e",
                "attemptId":attempt_id,"generation":generation,"fence":fence
            }),
        )
        .await
        .unwrap();
        let mut cancel_line = String::new();
        cancel_reader.read_line(&mut cancel_line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&cancel_line).unwrap()["type"],
            "flowTaskCancellationRequested"
        );
        assert!(coordinator_service
            .coordinator()
            .request_worker_tool_with_id(
                "late-tool-after-cancel".into(),
                attempt_id.clone(),
                generation,
                fence,
                "late-tool-request".into(),
                "invoke".into(),
                json!({"late":true}),
                now_unix_ms() + 10_000,
            )
            .await
            .is_err());
        assert_eq!(
            coordinator_service
                .coordinator()
                .snapshot()
                .attempts
                .iter()
                .find(|attempt| attempt.attempt_id == attempt_id)
                .unwrap()
                .cancellation,
            crate::coordinator::CancellationState::Requested
        );
        drop(cancel_reader);
        drop(cancel_writer);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = coordinator_service.coordinator().snapshot();
                if snapshot.flow_runs.first().is_some_and(|flow| {
                    flow.tasks["task-worker-e2e"].state
                        == crate::workflow::WorkflowTaskState::Cancelled
                        && flow.tasks["task-worker-sibling-e2e"].state
                            == crate::workflow::WorkflowTaskState::WaitingForApproval
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("service-owned worker cancellation is reconciled after the client disconnects");
        assert_eq!(
            coordinator_service.coordinator().snapshot().tool_requests[0].state,
            crate::coordinator::WorkerToolRequestState::Cancelled
        );
        assert_eq!(
            coordinator_service
                .coordinator()
                .snapshot()
                .attempts
                .iter()
                .find(|attempt| attempt.attempt_id == attempt_id)
                .unwrap()
                .cancellation,
            crate::coordinator::CancellationState::Completed
        );
        assert!(!coordinator_service
            .coordinator()
            .snapshot()
            .processes
            .iter()
            .any(|process| {
                process.attempt_id.as_deref() == Some(attempt_id.as_str()) && process.alive
            }));
        let after_first_cancel = coordinator_service.coordinator().snapshot();
        let after_first_cancel = after_first_cancel.flow_runs.first().unwrap();
        let sibling = &after_first_cancel.tasks["task-worker-sibling-e2e"];
        assert!(!sibling.cancel_requested);
        assert_eq!(
            sibling.state,
            crate::workflow::WorkflowTaskState::WaitingForApproval,
            "cancelling one attempt leaves the sibling active"
        );
        assert!(coordinator_service
            .coordinator()
            .snapshot()
            .processes
            .iter()
            .any(
                |process| process.attempt_id.as_deref() == Some(sibling_attempt_id.as_str())
                    && process.alive
            ));
        assert!(after_first_cancel.outbox.iter().any(|event| {
            event.kind == "task_cancelled" && event.task_id.as_deref() == Some("task-worker-e2e")
        }));

        let (mut sibling_cancel_reader, mut sibling_cancel_writer) =
            connect(&coordinator_service, "control", &"c".repeat(32)).await;
        write_json(
            &mut sibling_cancel_writer,
            &json!({
                "type":"cancelFlowTask","requestId":"cancel-worker-sibling-e2e",
                "flowId":"flow-worker-e2e","taskId":"task-worker-sibling-e2e",
                "attemptId":sibling_attempt_id,"generation":sibling_generation,"fence":fence
            }),
        )
        .await
        .unwrap();
        let mut sibling_cancel_line = String::new();
        sibling_cancel_reader
            .read_line(&mut sibling_cancel_line)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&sibling_cancel_line).unwrap()["type"],
            "flowTaskCancellationRequested"
        );
        drop(sibling_cancel_reader);
        drop(sibling_cancel_writer);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = coordinator_service.coordinator().snapshot();
                if snapshot.flow_runs.first().is_some_and(|flow| {
                    flow.tasks["task-worker-sibling-e2e"].state
                        == crate::workflow::WorkflowTaskState::Cancelled
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the sibling can be cancelled independently after the first attempt settles");
        assert!(coordinator_service
            .coordinator()
            .snapshot()
            .flow_runs
            .first()
            .unwrap()
            .outbox
            .iter()
            .any(|event| {
                event.kind == "task_cancelled"
                    && event.task_id.as_deref() == Some("task-worker-sibling-e2e")
            }));

        let (mut status_reader, mut status_writer) =
            connect(&coordinator_service, "view", &"v".repeat(32)).await;
        write_json(
            &mut status_writer,
            &json!({"type":"status","requestId":"worker-status"}),
        )
        .await
        .unwrap();
        let mut status_line = String::new();
        status_reader.read_line(&mut status_line).await.unwrap();
        let status: Value = serde_json::from_str(&status_line).unwrap();
        assert_eq!(status["workflowDispatch"]["enabled"], true);
        coordinator_service.shutdown.send_replace(true);
        server.await.unwrap().unwrap();

        let (store, recovery) = crate::coordinator_store::CoordinatorStore::open(
            state_dir.join("coordinator-v1.jsonl"),
        )
        .unwrap();
        drop(store);
        let recovered = recovery.snapshot.unwrap();
        let recovered_flow = &recovered["flowRuns"][0];
        assert_eq!(
            recovered_flow["tasks"]["task-worker-e2e"]["state"],
            "cancelled"
        );
        assert_eq!(
            recovered_flow["tasks"]["task-worker-sibling-e2e"]["state"],
            "cancelled"
        );
        assert!(recovered_flow["outbox"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["kind"] == "task_cancelled" && event["taskId"] == "task-worker-e2e"
            }));
        assert!(recovered_flow["outbox"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["kind"] == "task_cancelled" && event["taskId"] == "task-worker-sibling-e2e"
            }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_approval_is_durable_fenced_and_hidden_from_view_role() {
        use std::collections::BTreeMap;

        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("approval-service.sock");
        let ready = parent.path().join("approval-service.ready");
        let state_dir = parent.path().join("approval-state");
        let worker = r#"
import json, sys
envelope = json.loads(sys.stdin.readline())
print(json.dumps({
  'type':'roo.runtime.worker.approval.requested', 'protocolVersion':1,
  'attemptId':envelope['attemptId'], 'generation':envelope['generation'],
  'requestId':'approval-1', 'kind':'command.execute', 'reason':'Run build command',
  'timeoutMs':30000, 'payload':{'commandPreview':'echo token=private-value', 'taskId':envelope['invocation']['taskId']}
}), flush=True)
decision = json.loads(sys.stdin.readline())
assert decision['type'] == 'approval.result'
assert decision['decision'] == 'approved'
print(json.dumps({
  'type':'roo.runtime.worker.completed', 'attemptId':envelope['attemptId'],
  'generation':envelope['generation'], 'sourceCursor':1, 'status':'succeeded',
  'output':'build finished'
}), flush=True)
"#;
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "view-approval-token-00000000000000",
            "control-approval-token-000000000000",
            "tui-approval-token-00000000000000000",
        )
        .unwrap()
        .with_state_dir(&state_dir)
        .with_workflow_worker(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), worker.into()],
            None,
            BTreeMap::new(),
        );
        let coordinator_service = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let mut projection = serde_json::from_value::<FlowRunProjection>(json!({
            "flowId":"approval-flow", "boardId":"approval-board", "flowRevision":0,
            "tasks":[{"taskId":"approval-task","postId":"approval-post","title":"Approve build","dependencies":[],"assignee":null,"state":"ready","waitingReason":null,"attemptId":null,"generation":null,"lastSourceCursor":0}]
        })).unwrap();
        projection.max_attempts = 2;
        coordinator_service
            .coordinator()
            .register_flowrun_with_id(
                "approval-register-flow".into(),
                projection,
                "supervisor".into(),
            )
            .await
            .unwrap();
        let claimed = coordinator_service
            .coordinator()
            .claim_flowrun_with_id(
                "approval-claim-flow".into(),
                "approval-flow".into(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        coordinator_service.workflow_invocations.lock().unwrap().insert((
            "approval-flow".into(), "approval-task".into()
        ), json!({"projectId":"project-approval","runtimeOptions":{},"graph":{"id":"approval"},"input":{"messages":[]}}));

        let request = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = coordinator_service.coordinator().snapshot();
                if let Some(approval) = snapshot.approvals.iter().find(|approval| {
                    approval.state == crate::coordinator::ApprovalRequestState::Pending
                }) {
                    break approval.clone();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker approval request persisted");
        assert_eq!(request.flow_id, "approval-flow");
        assert_eq!(request.task_id, "approval-task");
        assert_eq!(request.payload["commandPreview"], "echo token=[REDACTED]");
        assert_eq!(
            coordinator_service.coordinator().snapshot().flow_runs[0].tasks["approval-task"].state,
            crate::workflow::WorkflowTaskState::WaitingForApproval
        );
        assert!(coordinator_service
            .coordinator()
            .request_worker_approval_with_id(
                "stale-approval-event".into(),
                request.attempt_id.clone(),
                request.generation,
                claimed.coordinator_fence.saturating_sub(1),
                "stale-approval-request".into(),
                "command.execute".into(),
                "late worker request".into(),
                json!({"commandPreview":"echo late"}),
                now_unix_ms().saturating_add(60_000),
            )
            .await
            .is_err());
        assert!(!coordinator_service
            .coordinator()
            .snapshot()
            .approvals
            .iter()
            .any(|approval| approval.request_id == "stale-approval-request"));

        let (mut view_reader, mut view_writer) = connect(
            &coordinator_service,
            "view",
            "view-approval-token-00000000000000",
        )
        .await;
        write_json(
            &mut view_writer,
            &json!({"type":"snapshot","requestId":"approval-view-snapshot"}),
        )
        .await
        .unwrap();
        let mut view_line = String::new();
        view_reader.read_line(&mut view_line).await.unwrap();
        let view_snapshot: Value = serde_json::from_str(&view_line).unwrap();
        assert!(view_snapshot["approvals"][0]["payload"].is_null());
        assert!(view_snapshot["approvals"][0]["reason"].is_null());
        assert!(view_snapshot["approvals"][0]["payloadRedacted"]
            .as_bool()
            .unwrap());
        drop(view_reader);
        drop(view_writer);
        let (mut control_reader, mut control_writer) = connect(
            &coordinator_service,
            "control",
            "control-approval-token-000000000000",
        )
        .await;
        write_json(
            &mut control_writer,
            &json!({"type":"listPendingApprovals","requestId":"approval-list"}),
        )
        .await
        .unwrap();
        let mut list_line = String::new();
        control_reader.read_line(&mut list_line).await.unwrap();
        let listed: Value = serde_json::from_str(&list_line).unwrap();
        assert_eq!(
            listed["approvals"][0]["payload"]["commandPreview"],
            "echo token=[REDACTED]"
        );

        write_json(
            &mut control_writer,
            &json!({
                "type":"resolveApproval","requestId":"approval-resolve",
                "attemptId":request.attempt_id,"generation":request.generation,
                "approvalRequestId":request.request_id,"resolutionId":"resolution-1",
                "decision":"approved","grant":"once","fence":claimed.coordinator_fence
            }),
        )
        .await
        .unwrap();
        let mut resolve_line = String::new();
        control_reader.read_line(&mut resolve_line).await.unwrap();
        let resolved: Value = serde_json::from_str(&resolve_line).unwrap();
        assert_eq!(resolved["type"], "approvalResolved");
        drop(control_reader);
        drop(control_writer);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let task = &coordinator_service.coordinator().snapshot().flow_runs[0].tasks
                    ["approval-task"];
                if task.state == crate::workflow::WorkflowTaskState::ReadyForReview {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("approved worker continues and reports success");
        let task =
            &coordinator_service.coordinator().snapshot().flow_runs[0].tasks["approval-task"];
        assert_eq!(
            task.last_outcome.as_ref().unwrap().evidence,
            Some(json!("build finished"))
        );
        coordinator_service.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_exit_interrupts_pending_approval_immediately() {
        use std::collections::BTreeMap;

        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("approval-exit.sock");
        let ready = parent.path().join("approval-exit.ready");
        let worker = r#"
import json, sys
envelope = json.loads(sys.stdin.readline())
print(json.dumps({
  'type':'roo.runtime.worker.approval.requested', 'protocolVersion':1,
  'attemptId':envelope['attemptId'], 'generation':envelope['generation'],
  'requestId':'approval-exit', 'kind':'command.execute', 'reason':'build',
  'timeoutMs':300000, 'payload':{'commandPreview':'make'}}), flush=True)
print(json.dumps({'type':'roo.runtime.worker.tool.requested','protocolVersion':1,
  'attemptId':envelope['attemptId'],'generation':envelope['generation'],'requestId':'tool-exit',
  'toolName':'invoke','arguments':{'action':'build'},'timeoutMs':300000}), flush=True)
"#;
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "view-approval-exit-token-00000000",
            "control-approval-exit-token-000000",
            "tui-approval-exit-token-0000000000",
        )
        .unwrap()
        .with_workflow_worker(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), worker.into()],
            None,
            BTreeMap::new(),
        );
        let coordinator_service = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let projection = serde_json::from_value::<FlowRunProjection>(json!({
            "flowId":"approval-exit-flow", "boardId":"approval-exit-board", "flowRevision":0,
            "tasks":[{"taskId":"task","postId":"post","title":"Task","dependencies":[],"assignee":null,"state":"ready","waitingReason":null,"attemptId":null,"generation":null,"lastSourceCursor":0}]
        })).unwrap();
        coordinator_service
            .coordinator()
            .register_flowrun_with_id(
                "register-approval-exit".into(),
                projection,
                "supervisor".into(),
            )
            .await
            .unwrap();
        let claimed = coordinator_service
            .coordinator()
            .claim_flowrun_with_id(
                "claim-approval-exit".into(),
                "approval-exit-flow".into(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        coordinator_service.workflow_invocations.lock().unwrap().insert(
            ("approval-exit-flow".into(), "task".into()),
            json!({"projectId":"project","runtimeOptions":{},"graph":{"id":"g"},"input":{"messages":[]}}),
        );

        let record = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(record) = coordinator_service
                    .coordinator()
                    .snapshot()
                    .approvals
                    .into_iter()
                    .find(|row| row.request_id == "approval-exit")
                {
                    if record.state != crate::coordinator::ApprovalRequestState::Pending {
                        break record;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker exit resolves pending request without waiting for its timeout");
        assert_eq!(
            record.state,
            crate::coordinator::ApprovalRequestState::Interrupted
        );
        assert_eq!(
            record.resolution_reason.as_deref(),
            Some("worker process exited before approval was resolved")
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if coordinator_service
                    .coordinator()
                    .snapshot()
                    .tool_requests
                    .iter()
                    .any(|tool| {
                        tool.request_id == "tool-exit"
                            && tool.state == crate::coordinator::WorkerToolRequestState::Interrupted
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker exit interrupts pending tool call and wakes the waiter");
        let snapshot = coordinator_service.coordinator().snapshot();
        assert_eq!(
            snapshot.flow_runs[0].coordinator_fence,
            claimed.coordinator_fence
        );
        assert_eq!(
            snapshot.flow_runs[0].tasks["task"].state,
            crate::workflow::WorkflowTaskState::Waiting
        );
        assert!(!snapshot.flow_runs[0].tasks["task"].dispatch_pending);
        coordinator_service.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_tool_broker_is_fenced_role_filtered_and_idempotent() {
        use std::collections::BTreeMap;
        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("tool-broker.sock");
        let ready = parent.path().join("tool-broker.ready");
        let worker = r#"
import json, sys
envelope = json.loads(sys.stdin.readline())
print(json.dumps({'type':'roo.runtime.worker.tool.requested','protocolVersion':1,
  'attemptId':envelope['attemptId'],'generation':envelope['generation'],'requestId':'tool-1',
  'toolName':'search','timeoutMs':30000,'arguments':{'query':'private query','secret':'never-view'}}), flush=True)
reply = json.loads(sys.stdin.readline())
assert reply['type'] == 'tool.result' and reply['requestId'] == 'tool-1'
assert reply['state'] == 'completed' and reply['result']['items'] == ['ok']
print(json.dumps({'type':'roo.runtime.worker.completed','attemptId':envelope['attemptId'],
  'generation':envelope['generation'],'sourceCursor':1,'status':'succeeded','output':'tool finished'}), flush=True)
"#;
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "view-tool-token-0000000000000000",
            "control-tool-token-00000000000000",
            "tui-tool-token-000000000000000000",
        )
        .unwrap()
        .with_workflow_worker(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), worker.into()],
            None,
            BTreeMap::new(),
        );
        let service_state = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let projection = serde_json::from_value::<FlowRunProjection>(json!({
            "flowId":"tool-flow","boardId":"tool-board","flowRevision":0,
            "tasks":[{"taskId":"task","postId":"post","title":"Tool task","dependencies":[],"assignee":null,"state":"ready","waitingReason":null,"attemptId":null,"generation":null,"lastSourceCursor":0}]
        })).unwrap();
        service_state
            .coordinator()
            .register_flowrun_with_id("tool-register".into(), projection, "supervisor".into())
            .await
            .unwrap();
        let flow = service_state
            .coordinator()
            .claim_flowrun_with_id(
                "tool-claim-flow".into(),
                "tool-flow".into(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        service_state.workflow_invocations.lock().unwrap().insert(
            ("tool-flow".into(), "task".into()),
            json!({"runtimeOptions":{},"graph":{"id":"tool"},"input":{"messages":[]}}),
        );
        let tool = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(tool) = service_state
                    .coordinator()
                    .snapshot()
                    .tool_requests
                    .into_iter()
                    .find(|item| item.request_id == "tool-1")
                {
                    break tool;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker tool request persisted");
        assert_eq!(
            tool.state,
            crate::coordinator::WorkerToolRequestState::Pending
        );
        let duplicate_request = service_state
            .coordinator()
            .request_worker_tool_with_id(
                "tool-duplicate-request".into(),
                tool.attempt_id.clone(),
                tool.generation,
                flow.coordinator_fence,
                tool.request_id.clone(),
                tool.tool_name.clone(),
                tool.arguments.clone(),
                tool.deadline_at_ms,
            )
            .await
            .unwrap();
        assert_eq!(duplicate_request.request_id, tool.request_id);
        assert!(service_state
            .coordinator()
            .request_worker_tool_with_id(
                "tool-conflicting-request".into(),
                tool.attempt_id.clone(),
                tool.generation,
                flow.coordinator_fence,
                tool.request_id.clone(),
                tool.tool_name.clone(),
                json!({"different":true}),
                tool.deadline_at_ms,
            )
            .await
            .is_err());
        assert!(service_state
            .coordinator()
            .request_worker_tool_with_id(
                "tool-stale-fence-request".into(),
                tool.attempt_id.clone(),
                tool.generation,
                flow.coordinator_fence.saturating_sub(1),
                "stale-fence-call".into(),
                tool.tool_name.clone(),
                json!({"query":"stale"}),
                tool.deadline_at_ms,
            )
            .await
            .is_err());

        let (mut view_reader, mut view_writer) =
            connect(&service_state, "view", "view-tool-token-0000000000000000").await;
        write_json(
            &mut view_writer,
            &json!({"type":"snapshot","requestId":"tool-view"}),
        )
        .await
        .unwrap();
        let mut view_line = String::new();
        view_reader.read_line(&mut view_line).await.unwrap();
        let view: Value = serde_json::from_str(&view_line).unwrap();
        assert!(view["toolRequests"][0]["arguments"].is_null());
        assert!(view["toolRequests"][0]["result"].is_null());
        assert_eq!(view["toolRequests"][0]["argumentsRedacted"], true);
        drop(view_reader);
        drop(view_writer);
        let (mut tui_reader, mut tui_writer) =
            connect(&service_state, "tui", "tui-tool-token-000000000000000000").await;
        write_json(
            &mut tui_writer,
            &json!({"type":"snapshot","requestId":"tui-tool-view"}),
        )
        .await
        .unwrap();
        let mut tui_line = String::new();
        tui_reader.read_line(&mut tui_line).await.unwrap();
        let tui_snapshot: Value = serde_json::from_str(&tui_line).unwrap();
        assert!(tui_snapshot["toolRequests"][0]["arguments"].is_null());
        assert!(tui_snapshot["toolRequests"][0]["result"].is_null());
        write_json(
            &mut tui_writer,
            &json!({"type":"listPendingToolRequests","requestId":"tui-list-tool"}),
        )
        .await
        .unwrap();
        let mut tui_denied = String::new();
        tui_reader.read_line(&mut tui_denied).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&tui_denied).unwrap()["type"],
            "error"
        );
        drop(tui_reader);
        drop(tui_writer);
        let tui_view = redact_snapshot_for_role(
            json!({"toolRequests":[{"arguments":{"secret":"never-view"},"result":{"secret":"never-view"},"error":"secret"}]}),
            ClientRole::Tui,
        );
        assert!(tui_view["toolRequests"][0]["arguments"].is_null());
        assert!(tui_view["toolRequests"][0]["result"].is_null());
        assert!(tui_view["toolRequests"][0]["error"].is_null());

        let (mut denied_reader, mut denied_writer) =
            connect(&service_state, "view", "view-tool-token-0000000000000000").await;
        write_json(
            &mut denied_writer,
            &json!({"type":"listPendingToolRequests","requestId":"view-list-tool"}),
        )
        .await
        .unwrap();
        let mut denied_line = String::new();
        denied_reader.read_line(&mut denied_line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&denied_line).unwrap()["type"],
            "error"
        );
        drop(denied_reader);
        drop(denied_writer);

        let (mut control_reader, mut control_writer) = connect(
            &service_state,
            "control",
            "control-tool-token-00000000000000",
        )
        .await;
        write_json(
            &mut control_writer,
            &json!({"type":"listPendingToolRequests","requestId":"list-tool"}),
        )
        .await
        .unwrap();
        let mut line = String::new();
        control_reader.read_line(&mut line).await.unwrap();
        let list: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            list["toolRequests"][0]["arguments"]["query"],
            "private query"
        );
        let stale_claim = json!({"type":"claimToolRequest","requestId":"stale-claim-tool","attemptId":tool.attempt_id,"generation":tool.generation,"toolRequestId":tool.request_id,"claimId":"stale-claim","fence":flow.coordinator_fence.saturating_sub(1)});
        write_json(&mut control_writer, &stale_claim).await.unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "error"
        );
        let oversized = Value::String("x".repeat(64 * 1024 + 1));
        assert!(service_state
            .coordinator()
            .request_worker_tool_with_id(
                "tool-oversized-request".into(),
                tool.attempt_id.clone(),
                tool.generation,
                flow.coordinator_fence,
                "too-large".into(),
                "search".into(),
                oversized,
                now_unix_ms().saturating_add(30_000),
            )
            .await
            .is_err());
        let claim = json!({"type":"claimToolRequest","requestId":"claim-tool","attemptId":tool.attempt_id,"generation":tool.generation,"toolRequestId":tool.request_id,"claimId":"desktop-tool-claim-1","fence":flow.coordinator_fence});
        write_json(&mut control_writer, &claim).await.unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["toolRequest"]["state"],
            "claimed"
        );
        let mut duplicate_claim = claim.clone();
        duplicate_claim["requestId"] = json!("claim-tool-duplicate");
        write_json(&mut control_writer, &duplicate_claim)
            .await
            .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["toolRequest"]["claimId"],
            "desktop-tool-claim-1"
        );
        let mut conflicting_claim = claim.clone();
        conflicting_claim["requestId"] = json!("claim-tool-conflict");
        conflicting_claim["claimId"] = json!("different-owner");
        write_json(&mut control_writer, &conflicting_claim)
            .await
            .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "error"
        );
        assert!(service_state
            .coordinator()
            .resolve_worker_tool_with_id(
                "tool-oversized-result".into(),
                tool.attempt_id.clone(),
                tool.generation,
                tool.request_id.clone(),
                "desktop-tool-claim-1".into(),
                "too-large-result".into(),
                Some(Value::String("x".repeat(64 * 1024 + 1))),
                None,
                flow.coordinator_fence,
            )
            .await
            .is_err());
        let resolution = json!({"type":"resolveToolRequest","requestId":"resolve-tool","attemptId":tool.attempt_id,"generation":tool.generation,"toolRequestId":tool.request_id,"claimId":"desktop-tool-claim-1","resolutionId":"desktop-tool-result-1","result":{"items":["ok"]},"fence":flow.coordinator_fence});
        let mut stale_resolution = resolution.clone();
        stale_resolution["requestId"] = json!("stale-resolve-tool");
        stale_resolution["fence"] = json!(flow.coordinator_fence.saturating_sub(1));
        write_json(&mut control_writer, &stale_resolution)
            .await
            .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "error"
        );
        write_json(&mut control_writer, &resolution).await.unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["toolRequest"]["state"],
            "completed"
        );
        let mut conflicting_resolution = resolution.clone();
        conflicting_resolution["requestId"] = json!("resolve-tool-conflict");
        conflicting_resolution["resolutionId"] = json!("different-result-id");
        write_json(&mut control_writer, &conflicting_resolution)
            .await
            .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["type"],
            "error"
        );
        let mut duplicate_resolution = resolution.clone();
        duplicate_resolution["requestId"] = json!("resolve-tool-duplicate");
        write_json(&mut control_writer, &duplicate_resolution)
            .await
            .unwrap();
        line.clear();
        control_reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["toolRequest"]["result"]["items"][0],
            "ok"
        );
        drop(control_reader);
        drop(control_writer);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if service_state.coordinator().snapshot().flow_runs[0].tasks["task"].state
                    == crate::workflow::WorkflowTaskState::ReadyForReview
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker received durable tool result");
        service_state.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_tool_timeout_wakes_the_worker_with_terminal_state() {
        use std::collections::BTreeMap;
        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("tool-timeout.sock");
        let ready = parent.path().join("tool-timeout.ready");
        let worker = r#"
import json, sys
envelope = json.loads(sys.stdin.readline())
print(json.dumps({'type':'roo.runtime.worker.tool.requested','protocolVersion':1,
  'attemptId':envelope['attemptId'],'generation':envelope['generation'],'requestId':'timeout-1',
  'toolName':'slow','arguments':{},'timeoutMs':100}), flush=True)
reply = json.loads(sys.stdin.readline())
assert reply['type'] == 'tool.result' and reply['state'] == 'timedOut'
print(json.dumps({'type':'roo.runtime.worker.completed','attemptId':envelope['attemptId'],
  'generation':envelope['generation'],'sourceCursor':1,'status':'failed','reasonCode':'tool_timed_out'}), flush=True)
"#;
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "view-tool-timeout-token-000000000000",
            "control-tool-timeout-token-0000000000",
            "tui-tool-timeout-token-00000000000000",
        )
        .unwrap()
        .with_workflow_worker(
            "/usr/bin/python3",
            vec!["-u".into(), "-c".into(), worker.into()],
            None,
            BTreeMap::new(),
        );
        let shared = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let projection = serde_json::from_value::<FlowRunProjection>(json!({"flowId":"timeout-flow","boardId":"timeout-board","flowRevision":0,"tasks":[{"taskId":"task","postId":"post","title":"Timeout","dependencies":[],"assignee":null,"state":"ready","waitingReason":null,"attemptId":null,"generation":null,"lastSourceCursor":0}]})).unwrap();
        shared
            .coordinator()
            .register_flowrun_with_id("timeout-register".into(), projection, "supervisor".into())
            .await
            .unwrap();
        shared
            .coordinator()
            .claim_flowrun_with_id(
                "timeout-claim".into(),
                "timeout-flow".into(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        shared.workflow_invocations.lock().unwrap().insert(
            ("timeout-flow".into(), "task".into()),
            json!({"runtimeOptions":{},"graph":{"id":"timeout"},"input":{"messages":[]}}),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if shared
                    .coordinator()
                    .snapshot()
                    .tool_requests
                    .iter()
                    .any(|tool| {
                        tool.request_id == "timeout-1"
                            && tool.state == crate::coordinator::WorkerToolRequestState::TimedOut
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tool deadline persists and wakes worker");
        shared.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn service_restart_interrupts_pending_and_unknowns_claimed_tool_requests() {
        let parent = tempfile::tempdir().unwrap();
        let state_dir = parent.path().join("state");
        let socket = parent.path().join("tool-restart.sock");
        let ready = parent.path().join("tool-restart.ready");
        let view = "view-tool-restart-token-000000000000";
        let control = "control-tool-restart-token-000000000";
        let tui = "tui-tool-restart-token-00000000000000";
        let service = SupervisorService::new(Arc::new(Supervisor::new()), view, control, tui)
            .unwrap()
            .with_state_dir(&state_dir);
        let shared = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let flow = shared.coordinator().register_flowrun_with_id("restart-tool-register".into(), serde_json::from_value::<FlowRunProjection>(json!({
            "flowId":"restart-tool-flow","boardId":"restart-tool-board","flowRevision":0,
            "tasks":[{"taskId":"task","postId":"post","title":"Tool restart","dependencies":[],"assignee":null,"state":"ready","waitingReason":null,"attemptId":null,"generation":null,"lastSourceCursor":0}]
        })).unwrap(), "supervisor".into()).await.unwrap();
        let flow = shared
            .coordinator()
            .claim_flowrun_with_id(
                "restart-tool-claim-flow".into(),
                flow.flow_id.clone(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        let flow = shared
            .coordinator()
            .reserve_flow_dispatch_with_id(
                "restart-tool-reserve".into(),
                flow.flow_id.clone(),
                "task".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let dispatch = flow.dispatch_request("task").unwrap();
        let attempt = shared
            .coordinator()
            .register_flow_worker_attempt_with_id(
                format!("restart-tool-attempt:{}", dispatch.idempotency_key),
                dispatch.flow_id.clone(),
                dispatch.task_id.clone(),
                dispatch.coordinator_fence,
            )
            .await
            .unwrap();
        shared
            .coordinator()
            .complete_flow_dispatch_with_id(
                "restart-tool-complete".into(),
                dispatch.flow_id.clone(),
                dispatch.clone(),
                crate::workflow::WorkflowDispatchReceipt {
                    process_id: "restart-tool-process".into(),
                    runtime_run_id: "restart-tool-runtime".into(),
                },
                flow.coordinator_fence,
            )
            .await
            .unwrap();

        let (mut reader, mut writer) = connect(&shared, "control", control).await;
        for (id, tool_id, tool_name) in [
            ("route-pending", "pending-tool", "search"),
            ("route-claimed", "claimed-tool", "invoke"),
        ] {
            write_json(&mut writer, &json!({"type":"requestWorkerTool","requestId":id,"attemptId":attempt.attempt_id,"generation":attempt.generation,"fence":flow.coordinator_fence,"toolRequestId":tool_id,"toolName":tool_name,"arguments":{"arg":tool_id},"timeoutMs":300000})).await.unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&line).unwrap()["type"],
                "toolRequestCreated"
            );
        }
        write_json(&mut writer, &json!({"type":"claimToolRequest","requestId":"restart-tool-desktop-claim","attemptId":attempt.attempt_id,"generation":attempt.generation,"toolRequestId":"claimed-tool","claimId":"desktop-claim","fence":flow.coordinator_fence})).await.unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["toolRequest"]["state"],
            "claimed"
        );
        drop(reader);
        drop(writer);
        shared.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
        let _ = std::fs::remove_file(&ready);
        drop(shared);

        let service = SupervisorService::new(Arc::new(Supervisor::new()), view, control, tui)
            .unwrap()
            .with_state_dir(&state_dir);
        let shared = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let snapshot = shared.coordinator().snapshot();
        assert_eq!(
            snapshot
                .tool_requests
                .iter()
                .find(|item| item.request_id == "pending-tool")
                .unwrap()
                .state,
            crate::coordinator::WorkerToolRequestState::Interrupted
        );
        assert_eq!(
            snapshot
                .tool_requests
                .iter()
                .find(|item| item.request_id == "claimed-tool")
                .unwrap()
                .state,
            crate::coordinator::WorkerToolRequestState::Unknown
        );
        assert!(snapshot
            .tool_requests
            .iter()
            .find(|item| item.request_id == "claimed-tool")
            .unwrap()
            .error
            .as_deref()
            .unwrap()
            .contains("will not be replayed"));
        shared.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rust_service_client_registers_and_cancels_flowrun_tasks() {
        use crate::service_client::{
            ExecutionStatus, ServiceFlowAttemptOutcome, ServiceFlowRunProjection,
            ServiceFlowTaskProjection, ServiceRole, SupervisorServiceClient,
        };

        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("service.sock");
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap();
        let owned_service = service.clone();
        let server_socket = socket.clone();
        let server = tokio::spawn(async move { owned_service.serve_unix(server_socket).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !socket.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let client =
            SupervisorServiceClient::connect(&socket, "c".repeat(32), ServiceRole::Control)
                .await
                .unwrap();
        let flow = client
            .register_flow_run(
                ServiceFlowRunProjection {
                    flow_id: "flow-rust-client".into(),
                    board_id: "board-rust-client".into(),
                    flow_revision: 0,
                    current_stage: None,
                    stage_transitions: Default::default(),
                    max_attempts: 3,
                    tasks: vec![ServiceFlowTaskProjection {
                        task_id: "task-rust-client".into(),
                        post_id: "post-rust-client".into(),
                        title: "Rust client task".into(),
                        stage_id: None,
                        dependencies: Vec::new(),
                        assignee: None,
                        state: "ready".into(),
                        waiting_reason: None,
                        attempt_id: None,
                        generation: None,
                        last_source_cursor: 0,
                    }],
                },
                "rust-client-e2e",
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .update_flow_invocations(
                    &flow.flow_id,
                    json!({"task-rust-client":{
                        "runtimeOptions":{"checkpointEndpoint":"http://127.0.0.1/checkpoints"},
                        "graph":{"id":"rust-client"},"input":{"messages":[]}
                    }}),
                    0,
                )
                .await
                .unwrap(),
            1
        );
        let flow = client
            .claim_flow_run(&flow.flow_id, "rust-client-e2e", 0)
            .await
            .unwrap();
        let flow = client
            .bind_flow_attempt(
                &flow.flow_id,
                "task-rust-client",
                "attempt-rust-client",
                1,
                1,
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let flow = client
            .cancel_flow_task(
                &flow.flow_id,
                "task-rust-client",
                "attempt-rust-client",
                1,
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        assert!(flow.tasks["task-rust-client"].cancel_requested);
        let flow = client
            .reconcile_flow_attempt(
                &flow.flow_id,
                "task-rust-client",
                ServiceFlowAttemptOutcome {
                    attempt_id: "attempt-rust-client".into(),
                    generation: 1,
                    source_cursor: 2,
                    status: ExecutionStatus::Cancelled,
                    reason: Some("worker_cancelled".into()),
                    workflow_cancel_requested: true,
                    retryable: false,
                    evidence: None,
                },
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        assert_eq!(flow.tasks["task-rust-client"].state, "cancelled");
        assert_eq!(
            client.snapshot().await.unwrap().flow_runs[0].tasks["task-rust-client"].state,
            "cancelled"
        );
        drop(client);
        service.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn service_owned_flowboard_outbox_retries_lost_mcp_reply_and_durably_acks_without_client()
    {
        use crate::coordinator_store::CoordinatorStore;
        use std::collections::BTreeMap;

        let parent = tempfile::tempdir().unwrap();
        let state_dir = parent.path().join("state");
        let socket = parent.path().join("service.sock");
        let ready = parent.path().join("service.ready");
        let event_log = parent.path().join("flowboard-events.jsonl");
        let crash_marker = parent.path().join("crashed-once");
        // The MCP stand-in validates the actual Flowboard tool arguments. It
        // records one append, exits before replying once, then returns the
        // committed revision for the stable idempotency key on reconnect.
        let mcp_script = r#"
import json, os, sys
events, marker = sys.argv[1], sys.argv[2]
for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion': request['params']['protocolVersion'], 'capabilities': {}, 'serverInfo': {'name': 'flowboard-test', 'version': '1'}}
    elif method == 'notifications/initialized':
        continue
    elif method == 'tools/call':
        params = request['params']
        assert params['name'] in ('flow_event_append', 'flow_get')
        if params['name'] == 'flow_event_append':
            args = params['arguments']
            required = {'actor', 'event_id', 'expected_revision', 'flow_id', 'kind', 'request_id'}
            assert required <= args.keys()
            assert isinstance(args['expected_revision'], int)
            assert isinstance(args['coordinator_fence'], int)
            assert args['flow_id'] == 'flow-e2e'
            old = []
            if os.path.exists(events):
                with open(events) as f: old = [json.loads(x) for x in f]
            if not any(item['event_id'] == args['event_id'] for item in old):
                with open(events, 'a') as f: f.write(json.dumps(args) + '\n')
                if not os.path.exists(marker):
                    open(marker, 'w').close()
                    os._exit(71)
            result = {'structuredContent': {'revision': args['expected_revision'] + 1}}
        else:
            result = {'structuredContent': {'revision': 1, 'events': []}}
    else:
        raise AssertionError('unexpected MCP method: ' + str(method))
    if 'id' in request:
        print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}), flush=True)
"#;
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap()
        .with_state_dir(&state_dir)
        .with_flowboard_mcp(
            "python3",
            vec![
                "-u".to_owned(),
                "-c".to_owned(),
                mcp_script.to_owned(),
                event_log.to_string_lossy().into_owned(),
                crash_marker.to_string_lossy().into_owned(),
            ],
            BTreeMap::new(),
        );
        let coordinator_service = service.clone();
        let server_socket = socket.clone();
        let server_ready = ready.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(server_socket, Some(&server_ready))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        // No Desktop/manager client is connected. The service actor creates
        // the event and its own worker drains the outbox independently.
        let coordinator = coordinator_service.coordinator();
        let projection = json!({
            "flowId":"flow-e2e",
            "boardId":"board-e2e",
            "flowRevision":0,
            "tasks":[{
                "taskId":"task-e2e","postId":"post-e2e","title":"e2e task",
                "dependencies":[],"assignee":null,"state":"ready",
                "waitingReason":null,"attemptId":null,"generation":null,"lastSourceCursor":0
            }]
        });
        let (mut desktop_reader, mut desktop_writer) =
            connect(&coordinator_service, "control", &"c".repeat(32)).await;
        write_json(
            &mut desktop_writer,
            &json!({
                "type":"registerFlowRun","requestId":"register-flow-e2e",
                "projection":projection,"coordinatorId":"supervisor-e2e"
            }),
        )
        .await
        .unwrap();
        let mut reply_line = String::new();
        desktop_reader.read_line(&mut reply_line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&reply_line).unwrap()["type"],
            "flowRunRegistered"
        );
        reply_line.clear();
        write_json(
            &mut desktop_writer,
            &json!({
                "type":"claimFlowRun","requestId":"claim-flow-e2e",
                "flowId":"flow-e2e","coordinatorId":"supervisor-e2e","expectedFence":0
            }),
        )
        .await
        .unwrap();
        desktop_reader.read_line(&mut reply_line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&reply_line).unwrap()["type"],
            "flowRunClaimed"
        );
        // The service keeps coordinating after this caller disconnects.
        drop(desktop_reader);
        drop(desktop_writer);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = coordinator.snapshot();
                if snapshot.flow_runs.first().is_some_and(|flow| {
                    !flow.pending_sync && flow.flow_revision == 1 && flow.outbox.is_empty()
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("service MCP worker retries and acknowledges the pending event");
        let committed = std::fs::read_to_string(&event_log).unwrap();
        let appended = committed
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            appended.len(),
            1,
            "retry uses the stable Flowboard event ID"
        );
        assert_eq!(appended[0]["kind"], "coordinator_claimed");
        assert_eq!(appended[0]["actor"], "supervisor-e2e");
        assert!(crash_marker.exists(), "first reply was deliberately lost");

        coordinator_service.shutdown.send_replace(true);
        server.await.unwrap().unwrap();
        let journal = state_dir.join("coordinator-v1.jsonl");
        let (store, recovery) = CoordinatorStore::open(journal).unwrap();
        drop(store);
        let recovered_snapshot = recovery.snapshot.unwrap();
        assert_eq!(recovered_snapshot["flowRuns"][0]["flowRevision"], 1);
        assert!(recovered_snapshot["flowRuns"][0]["outbox"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn flowboard_mcp_requires_persistent_state_before_binding_socket() {
        let parent = tempfile::tempdir().unwrap();
        let socket = parent.path().join("service.sock");
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "v".repeat(32),
            "c".repeat(32),
            "t".repeat(32),
        )
        .unwrap()
        .with_flowboard_mcp("unused-flowboard-mcp", Vec::new(), Default::default());
        let error = service.serve_unix(&socket).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!socket.exists());
    }

    #[test]
    fn server_role_scope_matrix_separates_launch_cancel_stop_and_shutdown() {
        assert!(role_allows(ClientRole::Control, "start"));
        assert!(!role_allows(ClientRole::Reporter, "start"));
        assert!(role_allows(ClientRole::Reporter, "report"));
        assert!(!role_allows(ClientRole::Reporter, "cancelAttempt"));
        assert!(role_allows(ClientRole::Canceller, "cancelAttempt"));
        assert!(!role_allows(ClientRole::Canceller, "report"));
        assert!(!role_allows(ClientRole::Canceller, "stop"));
        assert!(role_allows(ClientRole::Tui, "stop"));
        assert!(!role_allows(ClientRole::Tui, "shutdown"));
        assert!(role_allows(ClientRole::Control, "shutdown"));
        assert!(role_allows(ClientRole::Control, "setActivityStatus"));
        assert!(role_allows(ClientRole::Control, "setRunStatus"));
        assert!(role_allows(ClientRole::Control, "setActivityTimeout"));
        assert!(!role_allows(ClientRole::Reporter, "setActivityStatus"));
        assert!(!role_allows(ClientRole::View, "setActivityTimeout"));
        assert!(role_allows(ClientRole::Control, "cancelFlowTask"));
        assert!(!role_allows(ClientRole::View, "cancelFlowTask"));
        assert!(!role_allows(ClientRole::Tui, "cancelFlowTask"));
        assert!(!role_allows(ClientRole::Control, "futureUnknownMutation"));
        assert!(!role_allows(ClientRole::View, "stop"));
    }
}
