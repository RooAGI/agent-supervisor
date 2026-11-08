//! Authenticated per-user Unix socket service for a long-lived [`Supervisor`].
//!
//! The service owns process event streams and continues draining/reaping when
//! clients disconnect. Messages are UTF-8 JSON objects delimited by newlines.
//! Every connection must begin with `{"type":"auth","token":"..."}`.

use crate::{Command, ManagedProcessMetadata, ProcessEvent, Supervisor, SupervisorProcessId};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, watch};

const MAX_FRAME: usize = 1024 * 1024;
const EVENT_HISTORY: usize = 256;
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const MAX_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClientRole {
    View,
    Control,
    Tui,
}

impl ClientRole {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "view" => Some(Self::View),
            "control" => Some(Self::Control),
            "tui" => Some(Self::Tui),
            _ => None,
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Control => "control",
            Self::Tui => "tui",
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
            ClientRole::Control => None,
        }
    }
    fn revoked(&self, role: ClientRole) -> bool {
        match role {
            ClientRole::View => *self.view.borrow(),
            ClientRole::Tui => *self.tui.borrow(),
            ClientRole::Control => false,
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
        }
    }
}

struct Managed {
    events: broadcast::Sender<Value>,
    history: Arc<Mutex<VecDeque<Value>>>,
    input: mpsc::Sender<InputCommand>,
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
        Ok(Self {
            supervisor,
            view_token: Arc::new(view_token.to_vec()),
            control_token: Arc::new(control_token.to_vec()),
            tui_token: Arc::new(tui_token.to_vec()),
            processes: Arc::new(Mutex::new(HashMap::new())),
            session_ttl: DEFAULT_SESSION_TTL,
            revocations: RoleRevocations::new(),
        })
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

    /// Bind a private socket and serve authenticated client connections.
    pub async fn serve_unix(self, socket_path: impl AsRef<Path>) -> io::Result<()> {
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
        loop {
            let (stream, _) = listener.accept().await?;
            let service = self.clone();
            tokio::spawn(async move {
                let _ = service.connection(stream).await;
            });
        }
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
        let authorized = match role {
            Some(ClientRole::View) => constant_time_eq(&self.view_token, supplied),
            Some(ClientRole::Control) => constant_time_eq(&self.control_token, supplied),
            Some(ClientRole::Tui) => constant_time_eq(&self.tui_token, supplied),
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
        let session_deadline = tokio::time::Instant::now() + self.session_ttl;
        loop {
            line.clear();
            match read_session_frame(&mut reader, &mut line, session_deadline, &mut revocation_rx)
                .await
            {
                Ok(()) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::PermissionDenied
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
            if (role == ClientRole::View
                && matches!(action, "start" | "stdin" | "closeStdin" | "stop"))
                || (role == ClientRole::Tui && matches!(action, "start" | "stdin" | "closeStdin"))
                || (role == ClientRole::Control && action == "snapshot")
                || (role != ClientRole::Control && action == "revokeRole")
            {
                let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"forbidden","message":"view credentials cannot control processes"})).await;
                continue;
            }
            match action {
                "start" => match self.start(&request).await {
                    Ok((id, managed)) => {
                        let _ = out_tx.send(json!({"type":"started","requestId":request_id,"processId":id.to_string()})).await;
                        subscriptions.push(subscribe(managed, id, out_tx.clone()));
                    }
                    Err(e) => {
                        let _ = out_tx.send(json!({"type":"error","requestId":request_id,"code":"start_failed","message":e})).await;
                    }
                },
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
                    let _ = out_tx
                        .send(
                            json!({"type":"snapshot","requestId":request_id,"processes":processes}),
                        )
                        .await;
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
        writer_task.abort();
        Ok(())
    }

    async fn start(&self, request: &Value) -> Result<(SupervisorProcessId, Managed), String> {
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
                                let mut h = history.lock().unwrap(); h.push_back(value.clone()); while h.len() > EVENT_HISTORY { h.pop_front(); }
                                let _ = events.send(value.clone());
                                if value["type"] == "exited" { break; }
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
        let read = tokio::time::timeout_at(deadline, read_frame(reader, line));
        if let Some(receiver) = revocation.as_mut() {
            tokio::select! {
                result = read => return result.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut,"authenticated session expired"))),
                changed = receiver.changed() => {
                    if changed.is_err() || *receiver.borrow() {
                        return Err(io::Error::new(io::ErrorKind::PermissionDenied,"role credential revoked"));
                    }
                }
            }
        } else {
            return read.await.unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "authenticated session expired",
                ))
            });
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
    async fn view_credential_can_attach_but_cannot_start_or_stop() {
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
        drop(owner_writer);
        drop(owner_reader);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (mut reader, mut writer) = connect(&service, "view", &"v".repeat(32)).await;
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
            "unknown_command"
        );
    }
}
