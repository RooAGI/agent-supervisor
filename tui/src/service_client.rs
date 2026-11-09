use agent_supervisor::tui::{
    read_frame, write_frame, TuiActivity, TuiAttempt, TuiProcess, TuiRequest, TuiResponse, TuiRun,
    TuiStopOutcome, PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream,
};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

const MAX_SERVICE_FRAME: usize = 1024 * 1024;

enum Pending {
    Snapshot { tui_id: u64, subscribe: bool },
    Stop { tui_id: u64, process_id: String },
    StopAllSnapshot { tui_id: u64, grace_ms: u64 },
    StopAllItem { tui_id: u64, process_id: String },
}

/// Connect a local TUI protocol stream to the authenticated service socket.
/// The returned duplex stream preserves the existing TUI rendering/input loop.
pub async fn connect(
    socket_path: &Path,
    token_file: &Path,
    role: &str,
) -> Result<(DuplexStream, JoinHandle<()>), Box<dyn std::error::Error>> {
    if role != "view" && role != "tui" {
        return Err("TUI role must be view or tui".into());
    }
    let token = read_scoped_token(token_file)?;
    let (socket_read, socket_write) = connect_service(socket_path, role, &token).await?;
    let socket_path = socket_path.to_owned();
    let role = role.to_owned();

    let (ui, bridge) = tokio::io::duplex(64 * 1024);
    let task = tokio::spawn(async move {
        if let Err(error) = proxy(bridge, socket_path, role, token, socket_read, socket_write).await
        {
            eprintln!("agent-supervisor-tui: service connection ended: {error}");
        }
    });
    Ok((ui, task))
}

async fn connect_service(
    socket_path: &Path,
    role: &str,
    token: &str,
) -> io::Result<(
    BufReader<tokio::net::unix::OwnedReadHalf>,
    tokio::net::unix::OwnedWriteHalf,
)> {
    let stream = UnixStream::connect(socket_path).await?;
    let (socket_read, mut socket_write) = stream.into_split();
    let mut socket_read = BufReader::new(socket_read);
    write_json(
        &mut socket_write,
        &json!({"type":"auth","role":role,"token":token}),
    )
    .await?;
    let ready = read_json_line(&mut socket_read).await?;
    if ready.get("type").and_then(Value::as_str) != Some("ready")
        || ready.get("role").and_then(Value::as_str) != Some(role)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Supervisor service rejected the TUI credentials",
        ));
    }
    Ok((socket_read, socket_write))
}

fn read_scoped_token(path: &Path) -> io::Result<String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = std::fs::symlink_metadata(path)?;
    // SAFETY: geteuid has no preconditions and returns the effective UID.
    let current_uid = unsafe { libc::geteuid() };
    if !metadata.file_type().is_file()
        || metadata.uid() != current_uid
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > 4096
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "role token file must be a regular file owned by the current user with mode 0600",
        ));
    }
    let token = std::fs::read_to_string(path)?.trim().to_owned();
    if token.len() < 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "role token must contain at least 32 bytes",
        ));
    }
    if !token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "role token file must contain only one base64url-style token, not a credential bundle",
        ));
    }
    Ok(token)
}

async fn proxy(
    ui: DuplexStream,
    socket_path: std::path::PathBuf,
    role: String,
    token: String,
    mut socket_read: BufReader<tokio::net::unix::OwnedReadHalf>,
    mut socket_write: tokio::net::unix::OwnedWriteHalf,
) -> io::Result<()> {
    let (mut ui_read, mut ui_write) = tokio::io::split(ui);
    let hello: TuiRequest =
        serde_json::from_slice(&read_frame(&mut ui_read).await?).map_err(invalid_data)?;
    if !matches!(hello, TuiRequest::Hello { .. }) {
        return Err(invalid_data("TUI must begin with hello"));
    }
    write_frame(
        &mut ui_write,
        &TuiResponse::Hello {
            version: PROTOCOL_VERSION,
            selected_version: PROTOCOL_VERSION,
            supervisor_id: "supervisor-service".into(),
        },
    )
    .await?;

    let mut next_service_id = 1_u64;
    let mut pending: HashMap<String, Pending> = HashMap::new();
    let mut stop_all: HashMap<u64, (usize, Vec<TuiStopOutcome>)> = HashMap::new();
    let mut attached = HashSet::new();
    let mut known_processes: HashMap<String, TuiProcess> = HashMap::new();
    let mut event_id = 1_u64;
    let (request_tx, mut requests) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            let request = match read_frame(&mut ui_read).await {
                Ok(frame) => serde_json::from_slice(&frame).map_err(invalid_data),
                Err(error) => Err(error),
            };
            let done = request.is_err();
            if request_tx.send(request).await.is_err() || done {
                break;
            }
        }
    });
    loop {
        let (message_tx, mut messages) = tokio::sync::mpsc::channel(64);
        let mut service_reader = socket_read;
        tokio::spawn(async move {
            loop {
                let message = read_json_line(&mut service_reader).await;
                let done = message.is_err();
                if message_tx.send(message).await.is_err() || done {
                    break;
                }
            }
        });
        'session: loop {
            tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { return Ok(()); };
                let request = request?;
                match request {
                    TuiRequest::Subscribe { request_id, .. } => {
                        let id = service_id(&mut next_service_id)?;
                        pending.insert(id.clone(), Pending::Snapshot { tui_id: request_id, subscribe: true });
                        write_json(&mut socket_write, &json!({"type":"snapshot","requestId":id})).await?;
                    }
                    TuiRequest::ListProcesses { request_id, .. } => {
                        let id = service_id(&mut next_service_id)?;
                        pending.insert(id.clone(), Pending::Snapshot { tui_id: request_id, subscribe: false });
                        write_json(&mut socket_write, &json!({"type":"snapshot","requestId":id})).await?;
                    }
                    TuiRequest::StopProcess { request_id, process_id, grace_ms, .. } => {
                        let id = service_id(&mut next_service_id)?;
                        pending.insert(id.clone(), Pending::Stop { tui_id: request_id, process_id: process_id.clone() });
                        write_json(&mut socket_write, &json!({"type":"stop","requestId":id,"processId":process_id,"graceMs":grace_ms,"actor":"tui"})).await?;
                    }
                    TuiRequest::ShutdownAll { request_id, grace_ms, .. } => {
                        let id = service_id(&mut next_service_id)?;
                        pending.insert(id.clone(), Pending::StopAllSnapshot { tui_id: request_id, grace_ms });
                        write_json(&mut socket_write, &json!({"type":"snapshot","requestId":id})).await?;
                    }
                    TuiRequest::Hello { .. } => return Err(invalid_data("duplicate TUI hello")),
                }
            }
            line = messages.recv() => {
                let Some(line) = line else { break 'session; };
                let message = match line { Ok(message) => message, Err(_) => break 'session };
                let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
                let request_id = message.get("requestId").and_then(Value::as_str).unwrap_or("");
                if let Some(action) = pending.remove(request_id) {
                    match action {
                        Pending::Snapshot { tui_id, subscribe } => {
                            let processes = parse_processes(&message);
                            let runs = parse_runs(&message);
                            let activities = parse_activities(&message);
                            let attempts = parse_attempts(&message);
                            let flow_runs = parse_flow_runs(&message);
                            known_processes = processes
                                .iter()
                                .cloned()
                                .map(|process| (process.process_id.clone(), process))
                                .collect();
                            let response = if subscribe {
                                TuiResponse::Snapshot { version: PROTOCOL_VERSION, request_id: tui_id, processes: processes.clone(), runs, activities, attempts, flow_runs }
                            } else {
                                TuiResponse::Processes { version: PROTOCOL_VERSION, request_id: tui_id, processes: processes.clone(), runs, activities, attempts, flow_runs }
                            };
                            write_frame(&mut ui_write, &response).await?;
                            if role == "tui" {
                                for process in processes.into_iter().filter(|p| p.alive) {
                                    if attached.insert(process.process_id.clone()) {
                                        let attach_id = service_id(&mut next_service_id)?;
                                        write_json(&mut socket_write, &json!({"type":"attach","requestId":attach_id,"processId":process.process_id})).await?;
                                    }
                                }
                            }
                        }
                        Pending::Stop { tui_id, process_id } => {
                            if message_type == "stopped" {
                                write_frame(&mut ui_write, &TuiResponse::StopResult { version: PROTOCOL_VERSION, request_id: tui_id, process_id, outcome: message.get("status").and_then(Value::as_str).unwrap_or("stop requested").to_owned() }).await?;
                            } else {
                                write_frame(&mut ui_write, &TuiResponse::Error { version: PROTOCOL_VERSION, request_id: Some(tui_id), code: message.get("code").and_then(Value::as_str).unwrap_or("stop_failed").to_owned(), message: message.get("message").and_then(Value::as_str).unwrap_or("Supervisor rejected stop request").to_owned() }).await?;
                            }
                        }
                        Pending::StopAllSnapshot { tui_id, grace_ms } => {
                            let processes = parse_processes(&message).into_iter().filter(|p| p.alive).collect::<Vec<_>>();
                            if processes.is_empty() {
                                write_frame(&mut ui_write, &TuiResponse::ShutdownResult { version: PROTOCOL_VERSION, request_id: tui_id, outcomes: Vec::new() }).await?;
                            } else {
                                stop_all.insert(tui_id, (processes.len(), Vec::new()));
                                for process in processes {
                                    let id = service_id(&mut next_service_id)?;
                                    pending.insert(id.clone(), Pending::StopAllItem { tui_id, process_id: process.process_id.clone() });
                                    write_json(&mut socket_write, &json!({"type":"stop","requestId":id,"processId":process.process_id,"graceMs":grace_ms,"actor":"tui"})).await?;
                                }
                            }
                        }
                        Pending::StopAllItem { tui_id, process_id } => {
                            let state = stop_all.get_mut(&tui_id).ok_or_else(|| invalid_data("stop-all state disappeared"))?;
                            state.1.push(TuiStopOutcome { process_id, outcome: message.get("status").and_then(Value::as_str).unwrap_or("stop failed").to_owned() });
                            state.0 -= 1;
                            if state.0 == 0 {
                                let (_, outcomes) = stop_all.remove(&tui_id).unwrap();
                                write_frame(&mut ui_write, &TuiResponse::ShutdownResult { version: PROTOCOL_VERSION, request_id: tui_id, outcomes }).await?;
                            }
                        }
                    }
                    continue;
                }
                match message_type {
                    "attached" => {}
                    "processStarted" => {
                        let process_id = message.get("processId").and_then(Value::as_str).unwrap_or("").to_owned();
                        if !process_id.is_empty() {
                            let process = known_processes.entry(process_id.clone()).or_insert_with(|| TuiProcess {
                                process_id,
                                pid: 0,
                                alive: true,
                                executable: None,
                                framework: None,
                                run_label: None,
                                project_id: None,
                                flow_run_id: None,
                                task_id: None,
                                post_id: None,
                                worker_id: None,
                                runtime_run_id: None,
                            });
                            process.pid = message.get("pid").and_then(Value::as_u64).unwrap_or(process.pid as u64) as u32;
                            process.alive = true;
                            if let Some(executable) = message.get("executable").and_then(Value::as_str) {
                                process.executable = Some(executable.to_owned());
                            }
                            write_frame(&mut ui_write, &TuiResponse::ProcessChanged { version: PROTOCOL_VERSION, event_id, process: process.clone() }).await?;
                            event_id = event_id.wrapping_add(1);
                        }
                    }
                    "exited" => {
                        let process_id = message.get("processId").and_then(Value::as_str).unwrap_or("").to_owned();
                        attached.remove(&process_id);
                        known_processes.remove(&process_id);
                        write_frame(&mut ui_write, &TuiResponse::ProcessRemoved { version: PROTOCOL_VERSION, event_id, process_id }).await?;
                        event_id = event_id.wrapping_add(1);
                    }
                    "resyncRequired" => {
                        write_frame(&mut ui_write, &TuiResponse::ResyncRequired { version: PROTOCOL_VERSION, event_id }).await?;
                        event_id = event_id.wrapping_add(1);
                    }
                    "stop_requested" | "stdout" | "stderr" => {}
                    "error" => {
                        write_frame(&mut ui_write, &TuiResponse::Error { version: PROTOCOL_VERSION, request_id: None, code: message.get("code").and_then(Value::as_str).unwrap_or("service_error").to_owned(), message: message.get("message").and_then(Value::as_str).unwrap_or("Supervisor service error").to_owned() }).await?;
                    }
                    _ => {}
                }
            }
            }
        }
        pending.clear();
        stop_all.clear();
        attached.clear();
        write_frame(
            &mut ui_write,
            &TuiResponse::Error {
                version: PROTOCOL_VERSION,
                request_id: None,
                code: "disconnected".into(),
                message: "Supervisor service disconnected; reconnecting.".into(),
            },
        )
        .await?;
        write_frame(
            &mut ui_write,
            &TuiResponse::ResyncRequired {
                version: PROTOCOL_VERSION,
                event_id,
            },
        )
        .await?;
        event_id = event_id.wrapping_add(1);
        loop {
            match connect_service(&socket_path, &role, &token).await {
                Ok((reader, writer)) => {
                    socket_read = reader;
                    socket_write = writer;
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
            }
        }
    }
}

fn parse_processes(value: &Value) -> Vec<TuiProcess> {
    value
        .get("processes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|p| {
            Some(TuiProcess {
                process_id: p.get("processId")?.as_str()?.to_owned(),
                pid: p.get("pid")?.as_u64()? as u32,
                alive: p.get("alive")?.as_bool()?,
                executable: p
                    .get("executable")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                framework: p
                    .get("framework")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                run_label: p.get("runLabel").and_then(Value::as_str).map(str::to_owned),
                project_id: p
                    .get("projectId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                flow_run_id: p
                    .get("flowRunId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                task_id: p.get("taskId").and_then(Value::as_str).map(str::to_owned),
                post_id: p.get("postId").and_then(Value::as_str).map(str::to_owned),
                worker_id: p.get("workerId").and_then(Value::as_str).map(str::to_owned),
                runtime_run_id: p
                    .get("runtimeRunId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        })
        .collect()
}

fn records(value: &Value, key: &str) -> Value {
    Value::Array(
        value
            .get(key)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    )
}

fn parse_runs(value: &Value) -> Vec<TuiRun> {
    serde_json::from_value(records(value, "runs")).unwrap_or_default()
}

fn parse_activities(value: &Value) -> Vec<TuiActivity> {
    serde_json::from_value(records(value, "activities")).unwrap_or_default()
}

fn parse_attempts(value: &Value) -> Vec<TuiAttempt> {
    serde_json::from_value(records(value, "attempts")).unwrap_or_default()
}

fn parse_flow_runs(value: &Value) -> Vec<Value> {
    value
        .get("flowRuns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn service_id(next: &mut u64) -> io::Result<String> {
    let id = *next;
    *next = next
        .checked_add(1)
        .ok_or_else(|| invalid_data("request ID exhausted"))?;
    Ok(format!("tui-{id}"))
}

async fn read_json_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Value> {
    let mut line = Vec::new();
    let count = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        reader
            .take(MAX_SERVICE_FRAME as u64 + 1)
            .read_until(b'\n', &mut line),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "service response timed out"))??;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "service connection closed",
        ));
    }
    if count > MAX_SERVICE_FRAME {
        return Err(invalid_data("service frame exceeds 1 MiB"));
    }
    serde_json::from_slice(&line).map_err(invalid_data)
}

async fn write_json<W: AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> io::Result<()> {
    writer
        .write_all(&serde_json::to_vec(value).map_err(invalid_data)?)
        .await?;
    writer.write_all(b"\n").await
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        connect, parse_activities, parse_attempts, parse_flow_runs, parse_processes, parse_runs,
        proxy, read_json_line, read_scoped_token, write_json,
    };
    use agent_supervisor::tui::{
        read_frame, write_frame, TuiActivity, TuiAttempt, TuiProcess, TuiRequest, TuiResponse,
        TuiRun, PROTOCOL_VERSION,
    };
    use serde_json::{json, Value};
    use std::path::Path;
    use tokio::io::BufReader;
    use tokio::net::{UnixListener, UnixStream};
    use tokio::time::{timeout, Duration};

    #[test]
    fn service_snapshot_preserves_supervisor_and_flow_context_fields() {
        let processes = parse_processes(&json!({
            "type": "snapshot",
            "processes": [{
                "processId": "v1:supervisor:9",
                "pid": 1234,
                "alive": true,
                "executable": "/usr/bin/codex",
                "framework": "codex",
                "runLabel": "worker-3",
                "projectId": "project-a",
                "flowRunId": "flow-b",
                "taskId": "task-c"
                ,"postId":"post-d","workerId":"worker-e","runtimeRunId":"runtime-f"
            }]
        }));
        assert_eq!(processes.len(), 1);
        assert_eq!(processes[0].process_id, "v1:supervisor:9");
        assert_eq!(processes[0].pid, 1234);
        assert_eq!(processes[0].project_id.as_deref(), Some("project-a"));
        assert_eq!(processes[0].flow_run_id.as_deref(), Some("flow-b"));
        assert_eq!(processes[0].task_id.as_deref(), Some("task-c"));
        assert_eq!(processes[0].post_id.as_deref(), Some("post-d"));
        assert_eq!(processes[0].worker_id.as_deref(), Some("worker-e"));
        assert_eq!(processes[0].runtime_run_id.as_deref(), Some("runtime-f"));
    }

    #[test]
    fn service_snapshot_preserves_run_activity_attempt_and_waiting_context() {
        let value = json!({
            "runs": [{
                "runId": "run-1", "managerId": "manager-1", "status": "running",
                "context": {"framework": "langgraph", "projectId": "project-a", "flowRunId": "flow-b"},
                "revision": 3, "updatedAtMs": 1000
            }],
            "activities": [{
                "activityId": "activity-1", "runId": "run-1", "kind": "node",
                "status": "waitingForInput", "health": "healthy", "lastContactAtMs": 900,
                "waitingReason": "approval", "revision": 2, "updatedAtMs": 1000
            }],
            "attempts": [{
                "attemptId": "attempt-1", "activityId": "activity-1", "runId": "run-1",
                "generation": 2, "status": "running", "cancellation": "none",
                "processIds": [], "reports": [], "revision": 4, "updatedAtMs": 1000
            }]
        });
        let runs = parse_runs(&value);
        let activities = parse_activities(&value);
        let attempts = parse_attempts(&value);
        assert_eq!(runs[0].run_id, "run-1");
        assert_eq!(runs[0].context["flowRunId"], "flow-b");
        assert_eq!(activities[0].waiting_reason.as_deref(), Some("approval"));
        assert_eq!(attempts[0].generation, 2);
        assert!(attempts[0].process_ids.is_empty());
    }

    #[test]
    fn service_snapshot_preserves_canonical_flowrun_projection() {
        let flows = parse_flow_runs(&json!({
            "flowRuns": [{
                "flowId": "flow-1",
                "boardId": "board-1",
                "flowRevision": 8,
                "coordinatorFence": 3,
                "pendingSync": true,
                "syncError": "temporary failure",
                "tasks": {"task-1": {"title": "Implement", "state": "running", "waitingReason": null}}
            }]
        }));
        assert_eq!(flows.len(), 1);
        assert_eq!(flows[0]["flowId"], "flow-1");
        assert_eq!(flows[0]["boardId"], "board-1");
        assert_eq!(flows[0]["flowRevision"], 8);
        assert_eq!(flows[0]["coordinatorFence"], 3);
        assert_eq!(flows[0]["pendingSync"], true);
        assert_eq!(flows[0]["syncError"], "temporary failure");
        assert_eq!(flows[0]["tasks"]["task-1"]["state"], "running");
    }

    #[test]
    fn role_token_reader_accepts_one_secret_and_rejects_a_credential_bundle() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!(
            "agent-supervisor-tui-token-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "a-view-scoped-token-that-is-long-enough_123\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            read_scoped_token(&path).unwrap(),
            "a-view-scoped-token-that-is-long-enough_123"
        );
        std::fs::write(
            &path,
            r#"{"viewToken":"view-secret","controlToken":"control-secret"}"#,
        )
        .unwrap();
        assert!(read_scoped_token(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn service_snapshot_attach_and_exit_reach_the_private_tui_protocol() {
        let (ui, mut ui_client) = tokio::io::duplex(64 * 1024);
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/astui-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = UnixListener::bind(&socket_path).unwrap();
        let service = UnixStream::connect(&socket_path).await.unwrap();
        let (service_peer, _) = listener.accept().await.unwrap();
        let (service_reader, service_writer) = service.into_split();
        let proxy_task = tokio::spawn(proxy(
            ui,
            socket_path.clone(),
            "tui".into(),
            "test-token-with-more-than-thirty-two-characters".into(),
            BufReader::new(service_reader),
            service_writer,
        ));
        let (mut ui_reader, mut ui_writer) = tokio::io::split(&mut ui_client);

        write_frame(
            &mut ui_writer,
            &TuiRequest::Hello {
                version: PROTOCOL_VERSION,
                min_version: PROTOCOL_VERSION,
                max_version: PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();
        let hello: TuiResponse =
            serde_json::from_slice(&read_frame(&mut ui_reader).await.unwrap()).unwrap();
        assert!(matches!(hello, TuiResponse::Hello { .. }));

        write_frame(
            &mut ui_writer,
            &TuiRequest::Subscribe {
                version: PROTOCOL_VERSION,
                request_id: 7,
            },
        )
        .await
        .unwrap();
        let (mut service_reader, mut service_writer) = service_peer.into_split();
        let snapshot_request = read_json_line(&mut BufReader::new(&mut service_reader))
            .await
            .unwrap();
        assert_eq!(snapshot_request["type"], "snapshot");
        let snapshot_id = snapshot_request["requestId"].as_str().unwrap();
        write_json(
            &mut service_writer,
            &json!({
                "type":"snapshot",
                "requestId":snapshot_id,
                "processes":[{
                    "processId":"v1:manager:9",
                    "pid":91,
                    "alive":true,
                    "executable":"codex",
                    "framework":"codex",
                    "runLabel":"worker-9",
                    "projectId":"p",
                    "flowRunId":"f",
                    "taskId":"t"
                }],
                "runs":[{"runId":"run-1","managerId":"manager-1","status":"running","context":{"projectId":"p","flowRunId":"f","framework":"codex"},"revision":1,"updatedAtMs":1000}],
                "activities":[{"activityId":"activity-1","runId":"run-1","kind":"chat","status":"running","health":"healthy","lastContactAtMs":1000,"waitingReason":null,"revision":1,"updatedAtMs":1000}],
                "attempts":[{"attemptId":"attempt-1","activityId":"activity-1","runId":"run-1","generation":1,"status":"running","cancellation":"none","processIds":["v1:manager:9"],"reports":[],"revision":1,"updatedAtMs":1000}]
            }),
        )
        .await
        .unwrap();
        let snapshot: TuiResponse =
            serde_json::from_slice(&read_frame(&mut ui_reader).await.unwrap()).unwrap();
        assert!(
            matches!(snapshot, TuiResponse::Snapshot { request_id: 7, runs, activities, attempts, .. }
            if runs.len() == 1 && activities[0].status == "running" && attempts[0].process_ids.len() == 1)
        );

        let attach = read_json_line(&mut BufReader::new(&mut service_reader))
            .await
            .unwrap();
        assert_eq!(attach["type"], "attach");
        assert_eq!(attach["processId"], "v1:manager:9");
        write_json(
            &mut service_writer,
            &json!({"type":"attached","requestId":attach["requestId"],"processId":"v1:manager:9"}),
        )
        .await
        .unwrap();
        write_json(&mut service_writer, &json!({"type":"processStarted","processId":"v1:manager:9","pid":91,"executable":"codex"})).await.unwrap();
        let changed: TuiResponse = serde_json::from_slice(
            &timeout(Duration::from_secs(2), read_frame(&mut ui_reader))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        match changed {
            TuiResponse::ProcessChanged { process, .. } => {
                assert_eq!(process.project_id.as_deref(), Some("p"));
                assert_eq!(process.flow_run_id.as_deref(), Some("f"));
                assert_eq!(process.task_id.as_deref(), Some("t"));
            }
            other => panic!("expected process update, got {other:?}"),
        }

        write_json(
            &mut service_writer,
            &json!({"type":"exited","processId":"v1:manager:9","receipt":{}}),
        )
        .await
        .unwrap();
        let removed: TuiResponse = serde_json::from_slice(
            &timeout(Duration::from_secs(2), read_frame(&mut ui_reader))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(removed, TuiResponse::ProcessRemoved { process_id, .. } if process_id == "v1:manager:9")
        );

        drop(service_reader);
        drop(service_writer);
        let disconnected: TuiResponse = serde_json::from_slice(
            &timeout(Duration::from_secs(2), read_frame(&mut ui_reader))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(disconnected, TuiResponse::Error { code, .. } if code == "disconnected"));
        let resync: TuiResponse = serde_json::from_slice(
            &timeout(Duration::from_secs(2), read_frame(&mut ui_reader))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(resync, TuiResponse::ResyncRequired { .. }));

        let (reconnected, _) = timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (reconnected_reader, mut reconnected_writer) = reconnected.into_split();
        let mut reconnected_reader = BufReader::new(reconnected_reader);
        let auth = read_json_line(&mut reconnected_reader).await.unwrap();
        assert_eq!(auth["role"], "tui");
        assert_eq!(
            auth["token"],
            "test-token-with-more-than-thirty-two-characters"
        );
        write_json(
            &mut reconnected_writer,
            &json!({"type":"ready","version":1,"role":"tui"}),
        )
        .await
        .unwrap();

        write_frame(
            &mut ui_writer,
            &TuiRequest::ListProcesses {
                version: PROTOCOL_VERSION,
                request_id: 8,
            },
        )
        .await
        .unwrap();
        let list = read_json_line(&mut reconnected_reader).await.unwrap();
        assert_eq!(list["type"], "snapshot");
        write_json(
            &mut reconnected_writer,
            &json!({"type":"snapshot","requestId":list["requestId"],"processes":[]}),
        )
        .await
        .unwrap();
        let refreshed: TuiResponse = serde_json::from_slice(
            &timeout(Duration::from_secs(2), read_frame(&mut ui_reader))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(refreshed, TuiResponse::Processes { request_id: 8, processes, .. } if processes.is_empty())
        );
        proxy_task.abort();
        drop(reconnected_reader);
        drop(reconnected_writer);
        drop(listener);
        std::fs::remove_file(socket_path).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn live_service_flowrun_snapshot_reconnects_without_duplicate_worker_launch() {
        use agent_supervisor::service::SupervisorService;
        use agent_supervisor::service_client::{
            ActivityStatus, ServiceFlowRunProjection, ServiceFlowTaskProjection, ServiceRole,
            SupervisorServiceClient,
        };
        use agent_supervisor::Supervisor;
        use std::collections::BTreeMap;
        use std::os::unix::fs::PermissionsExt;
        use std::sync::Arc;

        let temp_dir = TestDirectory::new();
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/astui-live-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let ready_path = temp_dir.path.join("service.ready");
        let token_path = temp_dir.path.join("tui.token");
        let tui_token = "tui-live-flowrun-token-for-integration-tests";
        let control_token = "control-live-flowrun-token-for-integration-tests";
        std::fs::write(&token_path, format!("{tui_token}\n")).unwrap();
        std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600)).unwrap();

        let python = find_program("python3").expect("python3 is needed for the worker fixture");
        let worker = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/python/supervisor_jsonl_worker.py")
            .canonicalize()
            .unwrap();
        let service = SupervisorService::new(
            Arc::new(Supervisor::new()),
            "view-live-flowrun-token-for-integration-tests",
            control_token,
            tui_token,
        )
        .unwrap()
        .with_state_dir(temp_dir.path.join("state"))
        .with_workflow_worker(
            python,
            vec!["-u".into(), worker.to_string_lossy().into_owned()],
            None,
            BTreeMap::new(),
        );
        let service_socket = socket_path.clone();
        let service_ready = ready_path.clone();
        let server = tokio::spawn(async move {
            service
                .serve_unix_with_ready(service_socket, Some(&service_ready))
                .await
        });
        let ready_result = tokio::time::timeout(Duration::from_secs(5), async {
            while !ready_path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        if ready_result.is_err() {
            if server.is_finished() {
                panic!(
                    "Supervisor service exited before readiness: {:?}",
                    server.await
                );
            }
            panic!("Supervisor service did not publish readiness before timeout");
        }

        let control =
            SupervisorServiceClient::connect(&socket_path, control_token, ServiceRole::Control)
                .await
                .unwrap();
        let (mut tui_stream, tui_bridge) = connect(&socket_path, &token_path, "tui").await.unwrap();
        let initial = tui_subscribe(&mut tui_stream, 1).await;
        assert!(initial.runs.is_empty());
        assert!(initial.processes.is_empty());

        let projection = ServiceFlowRunProjection {
            flow_id: "flow-tui-live".into(),
            board_id: "board-tui-live".into(),
            flow_revision: 0,
            current_stage: None,
            stage_transitions: BTreeMap::new(),
            max_attempts: 3,
            tasks: vec![ServiceFlowTaskProjection {
                task_id: "task-live".into(),
                post_id: "post-live".into(),
                title: "Live TUI worker".into(),
                stage_id: None,
                dependencies: Vec::new(),
                assignee: None,
                state: "ready".into(),
                waiting_reason: None,
                attempt_id: None,
                generation: None,
                last_source_cursor: 0,
            }],
        };
        let registration = control
            .register_flow_run(
                projection,
                "tui-integration-coordinator",
                Some(json!({
                    "task-live": {
                        "projectId":"project-live",
                        "runtimeOptions":{"checkpointEndpoint":"http://127.0.0.1/checkpoints"},
                        "graph":{"id":"fixture-graph"},
                        "input":{"messages":[],"context":{}},
                        "durationMs":30000,
                        "cancelDelayMs":500
                    }
                })),
            )
            .await
            .unwrap();
        let claimed = control
            .claim_flow_run(&registration.flow_id, "tui-integration-coordinator", 0)
            .await
            .unwrap();

        let mut request_id = 2_u64;
        let running = tui_wait_snapshot(&mut tui_stream, &mut request_id, |snapshot| {
            snapshot.flow_runs.first().is_some_and(|flow| {
                flow["tasks"]["task-live"]["state"] == "running"
                    && flow["tasks"]["task-live"]["processId"].is_string()
                    && flow["tasks"]["task-live"]["lastOutcome"]["evidence"]["message"]
                        == "framework-neutral worker started"
            }) && snapshot.attempts.iter().any(|attempt| {
                attempt.reports.iter().any(|report| {
                    report["detail"]["evidence"]["message"] == "framework-neutral worker started"
                })
            })
        })
        .await;
        assert_eq!(running.runs.len(), 1);
        assert_eq!(running.processes.len(), 1);
        assert_eq!(running.attempts.len(), 1);
        let run = &running.runs[0];
        assert_eq!(run.context["flowRunId"], "flow-tui-live");
        let activity = &running.activities[0];
        assert_eq!(activity.run_id, run.run_id);
        let attempt = &running.attempts[0];
        assert_eq!(attempt.run_id, run.run_id);
        assert_eq!(attempt.activity_id, activity.activity_id);
        assert_eq!(attempt.generation, 1);
        let task = &running.flow_runs[0]["tasks"]["task-live"];
        assert_eq!(task["attemptId"], attempt.attempt_id);
        assert_eq!(task["generation"], attempt.generation);
        assert_eq!(task["postId"], "post-live");
        let process_id = task["processId"].as_str().unwrap();
        assert!(attempt.process_ids.iter().any(|id| id == process_id));
        let process = running
            .processes
            .iter()
            .find(|process| process.process_id == process_id)
            .expect("FlowRun attempt process should appear in the TUI snapshot");
        assert!(process.alive);
        assert_eq!(process.framework.as_deref(), Some("rooagiRuntime"));
        assert_eq!(process.project_id.as_deref(), Some("project-live"));
        assert_eq!(process.flow_run_id.as_deref(), Some("flow-tui-live"));
        assert_eq!(process.task_id.as_deref(), Some("task-live"));
        assert_eq!(process.post_id.as_deref(), Some("post-live"));
        assert_eq!(
            process.runtime_run_id,
            task["runtimeRunId"].as_str().map(str::to_owned)
        );
        assert!(attempt.reports.iter().any(|report| {
            report["detail"]["evidence"]["message"] == "framework-neutral worker started"
        }));
        assert_eq!(task["lastOutcome"]["status"], "running");
        assert_eq!(
            task["lastOutcome"]["evidence"]["message"],
            "framework-neutral worker started"
        );

        // Closing and reopening the read-only TUI client attaches to the same
        // service-owned process. Its protocol has no work-launch operation.
        drop(tui_stream);
        tui_bridge.abort();
        let (mut reconnected, reconnect_bridge) =
            connect(&socket_path, &token_path, "tui").await.unwrap();
        let reattached = tui_subscribe(&mut reconnected, 50).await;
        assert_eq!(reattached.runs.len(), 1);
        assert_eq!(reattached.attempts.len(), 1);
        assert_eq!(reattached.processes.len(), 1);
        assert_eq!(reattached.processes[0].process_id, process_id);
        let durable_snapshot = control.snapshot().await.unwrap();
        assert_eq!(durable_snapshot.counts.runs, 1);
        assert_eq!(durable_snapshot.counts.attempts, 1);
        assert_eq!(durable_snapshot.counts.processes, 1);
        assert_eq!(
            durable_snapshot.flow_runs[0].tasks["task-live"].attempts_started,
            1
        );
        assert_eq!(
            durable_snapshot.flow_runs[0].tasks["task-live"]
                .process_id
                .as_deref(),
            Some(process_id)
        );

        let activity_id = reattached.activities[0].activity_id.clone();
        control
            .set_waiting(
                &activity_id,
                Some("fixture awaiting approval"),
                ActivityStatus::WaitingForApproval,
            )
            .await
            .unwrap();
        let waiting = tui_wait_snapshot(&mut reconnected, &mut request_id, |snapshot| {
            snapshot.activities.iter().any(|activity| {
                activity.activity_id == activity_id
                    && activity.waiting_reason.as_deref() == Some("fixture awaiting approval")
            })
        })
        .await;
        assert_eq!(waiting.activities[0].status, "waitingForApproval");
        assert_eq!(waiting.activities[0].health, "healthy");
        assert!(waiting.activities[0].last_contact_at_ms.is_some());

        control
            .cancel_flow_task(
                "flow-tui-live",
                "task-live",
                &attempt.attempt_id,
                attempt.generation,
                claimed.coordinator_fence,
            )
            .await
            .unwrap();
        let cancel_requested = tui_wait_snapshot(&mut reconnected, &mut request_id, |snapshot| {
            snapshot.flow_runs[0]["tasks"]["task-live"]["cancelRequested"] == true
        })
        .await;
        assert_eq!(
            cancel_requested.flow_runs[0]["tasks"]["task-live"]["state"],
            "running"
        );
        assert_eq!(cancel_requested.attempts[0].cancellation, "requested");
        assert_eq!(
            cancel_requested.flow_runs[0]["tasks"]["task-live"]["waitingReason"],
            "worker cancellation requested"
        );

        let cancelled = tui_wait_snapshot(&mut reconnected, &mut request_id, |snapshot| {
            snapshot.flow_runs[0]["tasks"]["task-live"]["state"] == "cancelled"
        })
        .await;
        let cancelled_task = &cancelled.flow_runs[0]["tasks"]["task-live"];
        assert_eq!(cancelled_task["cancelRequested"], false);
        assert_eq!(cancelled_task["lastOutcome"]["status"], "cancelled");
        assert_eq!(
            cancelled_task["lastOutcome"]["evidence"]["summary"],
            "cooperative cancellation acknowledged"
        );
        assert_eq!(cancelled.attempts[0].cancellation, "completed");
        assert_eq!(cancelled.runs.len(), 1);

        drop(reconnected);
        reconnect_bridge.abort();
        shutdown_test_service(&socket_path, control_token).await;
        server.await.unwrap().unwrap();
    }

    #[derive(Default)]
    struct TuiSnapshot {
        processes: Vec<TuiProcess>,
        runs: Vec<TuiRun>,
        activities: Vec<TuiActivity>,
        attempts: Vec<TuiAttempt>,
        flow_runs: Vec<Value>,
    }

    impl TuiSnapshot {
        fn from_response(response: TuiResponse) -> Option<Self> {
            match response {
                TuiResponse::Snapshot {
                    processes,
                    runs,
                    activities,
                    attempts,
                    flow_runs,
                    ..
                }
                | TuiResponse::Processes {
                    processes,
                    runs,
                    activities,
                    attempts,
                    flow_runs,
                    ..
                } => Some(Self {
                    processes,
                    runs,
                    activities,
                    attempts,
                    flow_runs,
                }),
                _ => None,
            }
        }
    }

    async fn tui_subscribe(stream: &mut tokio::io::DuplexStream, request_id: u64) -> TuiSnapshot {
        let (mut reader, mut writer) = tokio::io::split(stream);
        write_frame(
            &mut writer,
            &TuiRequest::Hello {
                version: PROTOCOL_VERSION,
                min_version: PROTOCOL_VERSION,
                max_version: PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();
        let hello = read_tui_response(&mut reader).await;
        assert!(matches!(hello, TuiResponse::Hello { .. }));
        write_frame(
            &mut writer,
            &TuiRequest::Subscribe {
                version: PROTOCOL_VERSION,
                request_id,
            },
        )
        .await
        .unwrap();
        loop {
            let response = read_tui_response(&mut reader).await;
            match response {
                response @ TuiResponse::Snapshot {
                    request_id: found, ..
                } if found == request_id => return TuiSnapshot::from_response(response).unwrap(),
                TuiResponse::Error { code, message, .. } => {
                    panic!("TUI subscribe failed: {code}: {message}")
                }
                _ => {}
            }
        }
    }

    async fn tui_wait_snapshot(
        stream: &mut tokio::io::DuplexStream,
        next_request_id: &mut u64,
        mut predicate: impl FnMut(&TuiSnapshot) -> bool,
    ) -> TuiSnapshot {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let request_id = *next_request_id;
                *next_request_id += 1;
                let snapshot = tui_list_snapshot(stream, request_id).await;
                if predicate(&snapshot) {
                    return snapshot;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("TUI snapshot did not reach the expected live state")
    }

    async fn tui_list_snapshot(
        stream: &mut tokio::io::DuplexStream,
        request_id: u64,
    ) -> TuiSnapshot {
        let (mut reader, mut writer) = tokio::io::split(stream);
        write_frame(
            &mut writer,
            &TuiRequest::ListProcesses {
                version: PROTOCOL_VERSION,
                request_id,
            },
        )
        .await
        .unwrap();
        loop {
            let response = read_tui_response(&mut reader).await;
            match response {
                response @ TuiResponse::Processes {
                    request_id: found, ..
                } if found == request_id => return TuiSnapshot::from_response(response).unwrap(),
                TuiResponse::Error {
                    code,
                    message,
                    request_id: None,
                    ..
                } if code == "not_found" && message == "unknown process" => {}
                TuiResponse::Error { code, message, .. } => {
                    panic!("TUI snapshot failed: {code}: {message}")
                }
                _ => {}
            }
        }
    }

    async fn read_tui_response(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> TuiResponse {
        let frame = tokio::time::timeout(Duration::from_secs(5), read_frame(reader))
            .await
            .expect("timed out waiting for TUI response")
            .unwrap();
        serde_json::from_slice(&frame).unwrap()
    }

    fn find_program(name: &str) -> Option<std::path::PathBuf> {
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|directory| directory.join(name))
            .find(|path| path.is_file())
            .and_then(|path| path.canonicalize().ok())
    }

    async fn shutdown_test_service(socket: &Path, control_token: &str) {
        let stream = UnixStream::connect(socket).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        write_json(
            &mut writer,
            &json!({"type":"auth","role":"control","token":control_token}),
        )
        .await
        .unwrap();
        assert_eq!(read_json_line(&mut reader).await.unwrap()["type"], "ready");
        write_json(
            &mut writer,
            &json!({"type":"shutdown","requestId":"tui-integration-shutdown","graceMs":0}),
        )
        .await
        .unwrap();
        assert_eq!(
            read_json_line(&mut reader).await.unwrap()["type"],
            "shutdownComplete"
        );
    }

    struct TestDirectory {
        path: std::path::PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "agent-supervisor-tui-live-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
