//! Private, framed control protocol used by the Supervisor-owned TUI.

use crate::{ManagedProcessInfo, ProcessStopResult, Supervisor, SupervisorProcessId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u8 = 1;
pub const MAX_FRAME_BYTES: usize = 1_048_576;
pub const MAX_SHUTDOWN_GRACE_MS: u64 = 60_000;

/// A request sent by the TUI over its private standard input pipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum TuiRequest {
    #[serde(rename = "hello")]
    Hello {
        version: u8,
        min_version: u8,
        max_version: u8,
    },
    #[serde(rename = "subscribe")]
    Subscribe { version: u8, request_id: u64 },
    #[serde(rename = "list_processes")]
    ListProcesses { version: u8, request_id: u64 },
    #[serde(rename = "stop_process")]
    StopProcess {
        version: u8,
        request_id: u64,
        process_id: String,
        grace_ms: u64,
    },
    #[serde(rename = "shutdown_all")]
    ShutdownAll {
        version: u8,
        request_id: u64,
        grace_ms: u64,
    },
}

/// A response sent by the supervisor over the TUI's private standard output pipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum TuiResponse {
    #[serde(rename = "hello")]
    Hello {
        version: u8,
        selected_version: u8,
        supervisor_id: String,
    },
    #[serde(rename = "unsupported_version")]
    UnsupportedVersion { version: u8 },
    #[serde(rename = "snapshot")]
    Snapshot {
        version: u8,
        request_id: u64,
        processes: Vec<TuiProcess>,
    },
    #[serde(rename = "processes")]
    Processes {
        version: u8,
        request_id: u64,
        processes: Vec<TuiProcess>,
    },
    #[serde(rename = "process_changed")]
    ProcessChanged {
        version: u8,
        event_id: u64,
        process: TuiProcess,
    },
    #[serde(rename = "process_removed")]
    ProcessRemoved {
        version: u8,
        event_id: u64,
        process_id: String,
    },
    #[serde(rename = "resync_required")]
    ResyncRequired { version: u8, event_id: u64 },
    #[serde(rename = "stop_result")]
    StopResult {
        version: u8,
        request_id: u64,
        process_id: String,
        outcome: String,
    },
    #[serde(rename = "shutdown_result")]
    ShutdownResult {
        version: u8,
        request_id: u64,
        outcomes: Vec<TuiStopOutcome>,
    },
    #[serde(rename = "error")]
    Error {
        version: u8,
        request_id: Option<u64>,
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TuiProcess {
    pub process_id: String,
    pub pid: u32,
    pub alive: bool,
    pub executable: Option<String>,
    pub framework: Option<String>,
    pub run_label: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub flow_run_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub post_id: Option<String>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub runtime_run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TuiStopOutcome {
    pub process_id: String,
    pub outcome: String,
}

impl From<&ManagedProcessInfo> for TuiProcess {
    fn from(process: &ManagedProcessInfo) -> Self {
        Self {
            process_id: process.supervisor_id.to_string(),
            pid: process.process_id,
            alive: process.alive,
            executable: process
                .executable
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            framework: process.framework.clone(),
            run_label: process.run_label.clone(),
            project_id: process.project_id.clone(),
            flow_run_id: process.flow_run_id.clone(),
            task_id: process.task_id.clone(),
            post_id: process.post_id.clone(),
            worker_id: process.worker_id.clone(),
            runtime_run_id: process.runtime_run_id.clone(),
        }
    }
}

/// Read one length-prefixed JSON frame, rejecting empty and oversized frames.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    reader.read_exact(&mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid TUI frame length",
        ));
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Write one length-prefixed JSON frame.
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> io::Result<()> {
    let payload = serde_json::to_vec(value).map_err(io::Error::other)?;
    if payload.is_empty() || payload.len() > MAX_FRAME_BYTES || payload.len() > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TUI frame exceeds the protocol limit",
        ));
    }
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&payload).await?;
    writer.flush().await
}

pub(crate) async fn serve(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    supervisor: Supervisor,
) {
    if serve_inner(&mut reader, &mut writer, supervisor)
        .await
        .is_err()
    {
        // A malformed/disconnected client ends only its own TUI session.
    }
}

async fn serve_inner<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    supervisor: Supervisor,
) -> io::Result<()> {
    let mut lifecycle_events = supervisor.subscribe();
    let hello = read_request(reader).await?;
    let TuiRequest::Hello {
        version,
        min_version,
        max_version,
    } = hello
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TUI handshake required",
        ));
    };
    if version != PROTOCOL_VERSION
        || min_version > PROTOCOL_VERSION
        || max_version < PROTOCOL_VERSION
        || min_version > max_version
    {
        write_frame(
            writer,
            &TuiResponse::UnsupportedVersion {
                version: PROTOCOL_VERSION,
            },
        )
        .await?;
        return Ok(());
    }
    write_frame(
        writer,
        &TuiResponse::Hello {
            version: PROTOCOL_VERSION,
            selected_version: PROTOCOL_VERSION,
            supervisor_id: supervisor.uuid().to_string(),
        },
    )
    .await?;

    let mut subscribed = false;
    let mut last_request_id = 0_u64;
    let mut event_id = 0_u64;
    let mut known_processes = BTreeMap::<String, TuiProcess>::new();
    loop {
        let incoming = tokio::select! {
            request = read_request(reader) => match request {
                Ok(request) => Incoming::Request(request),
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            },
            event = lifecycle_events.recv(), if subscribed => match event {
                Ok(_) => Incoming::LifecycleChanged,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => Incoming::Lagged,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        };
        let request = match incoming {
            Incoming::LifecycleChanged => {
                let current = process_snapshot(&supervisor)
                    .into_iter()
                    .map(|process| (process.process_id.clone(), process))
                    .collect::<BTreeMap<_, _>>();
                for (id, process) in &current {
                    if known_processes.get(id) != Some(process) {
                        event_id = event_id.saturating_add(1);
                        write_frame(
                            writer,
                            &TuiResponse::ProcessChanged {
                                version: PROTOCOL_VERSION,
                                event_id,
                                process: process.clone(),
                            },
                        )
                        .await?;
                    }
                }
                for id in known_processes
                    .keys()
                    .filter(|id| !current.contains_key(*id))
                {
                    event_id = event_id.saturating_add(1);
                    write_frame(
                        writer,
                        &TuiResponse::ProcessRemoved {
                            version: PROTOCOL_VERSION,
                            event_id,
                            process_id: id.clone(),
                        },
                    )
                    .await?;
                }
                known_processes = current;
                continue;
            }
            Incoming::Lagged => {
                event_id = event_id.saturating_add(1);
                write_frame(
                    writer,
                    &TuiResponse::ResyncRequired {
                        version: PROTOCOL_VERSION,
                        event_id,
                    },
                )
                .await?;
                continue;
            }
            Incoming::Request(request) => request,
        };
        let (version, request_id) = request_identity(&request);
        if version != PROTOCOL_VERSION || request_id == 0 || request_id <= last_request_id {
            write_frame(
                writer,
                &TuiResponse::Error {
                    version: PROTOCOL_VERSION,
                    request_id: Some(request_id),
                    code: "invalid_request".into(),
                    message: "Request version or sequence is invalid.".into(),
                },
            )
            .await?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid request sequence",
            ));
        }
        last_request_id = request_id;
        if !subscribed && !matches!(request, TuiRequest::Subscribe { .. }) {
            write_frame(
                writer,
                &TuiResponse::Error {
                    version: PROTOCOL_VERSION,
                    request_id: Some(request_id),
                    code: "invalid_request".into(),
                    message: "Subscribe before sending commands.".into(),
                },
            )
            .await?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "subscription required",
            ));
        }
        match request {
            TuiRequest::Hello { .. } => {
                write_frame(
                    writer,
                    &TuiResponse::Error {
                        version: PROTOCOL_VERSION,
                        request_id: Some(request_id),
                        code: "invalid_request".into(),
                        message: "Handshake has already completed.".into(),
                    },
                )
                .await?;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate handshake",
                ));
            }
            TuiRequest::Subscribe { request_id, .. } => {
                if subscribed {
                    write_frame(
                        writer,
                        &TuiResponse::Error {
                            version: PROTOCOL_VERSION,
                            request_id: Some(request_id),
                            code: "invalid_request".into(),
                            message: "Subscription has already started.".into(),
                        },
                    )
                    .await?;
                    continue;
                }
                subscribed = true;
                let processes = process_snapshot(&supervisor);
                known_processes = processes
                    .iter()
                    .map(|process| (process.process_id.clone(), process.clone()))
                    .collect();
                write_frame(
                    writer,
                    &TuiResponse::Snapshot {
                        version: PROTOCOL_VERSION,
                        request_id,
                        processes,
                    },
                )
                .await?;
            }
            TuiRequest::ListProcesses { request_id, .. } => {
                let processes = process_snapshot(&supervisor);
                known_processes = processes
                    .iter()
                    .map(|process| (process.process_id.clone(), process.clone()))
                    .collect();
                write_frame(
                    writer,
                    &TuiResponse::Processes {
                        version: PROTOCOL_VERSION,
                        request_id,
                        processes,
                    },
                )
                .await?;
            }
            TuiRequest::StopProcess {
                request_id,
                process_id,
                grace_ms,
                ..
            } => {
                let id = match process_id.parse::<SupervisorProcessId>() {
                    Ok(id) if supervisor.process(id).is_some() => id,
                    _ => {
                        write_frame(
                            writer,
                            &TuiResponse::Error {
                                version: PROTOCOL_VERSION,
                                request_id: Some(request_id),
                                code: "unknown_process".into(),
                                message:
                                    "The process is no longer registered with this supervisor."
                                        .into(),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                let outcome = match supervisor
                    .shutdown_process(
                        id,
                        std::time::Duration::from_millis(grace_ms.min(MAX_SHUTDOWN_GRACE_MS)),
                    )
                    .await
                {
                    Ok(outcome) => stop_result_name(outcome).to_string(),
                    Err(error) => {
                        write_frame(
                            writer,
                            &TuiResponse::Error {
                                version: PROTOCOL_VERSION,
                                request_id: Some(request_id),
                                code: "operation_failed".into(),
                                message: error.to_string(),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                write_frame(
                    writer,
                    &TuiResponse::StopResult {
                        version: PROTOCOL_VERSION,
                        request_id,
                        process_id: id.to_string(),
                        outcome,
                    },
                )
                .await?;
            }
            TuiRequest::ShutdownAll {
                request_id,
                grace_ms,
                ..
            } => {
                let ids = supervisor
                    .managed_processes()
                    .into_iter()
                    .map(|process| process.supervisor_id)
                    .collect::<Vec<_>>();
                match supervisor
                    .shutdown_all(std::time::Duration::from_millis(
                        grace_ms.min(MAX_SHUTDOWN_GRACE_MS),
                    ))
                    .await
                {
                    Ok(results) => {
                        let mut outcomes = ids
                            .into_iter()
                            .map(|id| TuiStopOutcome {
                                process_id: id.to_string(),
                                outcome: "stop_requested".into(),
                            })
                            .collect::<Vec<_>>();
                        for (id, result) in results {
                            if let Some(outcome) = outcomes
                                .iter_mut()
                                .find(|item| item.process_id == id.to_string())
                            {
                                outcome.outcome =
                                    format!("completed:{:?}", result.receipt.termination);
                            }
                        }
                        write_frame(
                            writer,
                            &TuiResponse::ShutdownResult {
                                version: PROTOCOL_VERSION,
                                request_id,
                                outcomes,
                            },
                        )
                        .await?;
                    }
                    Err(error) => {
                        write_frame(
                            writer,
                            &TuiResponse::Error {
                                version: PROTOCOL_VERSION,
                                request_id: Some(request_id),
                                code: "operation_failed".into(),
                                message: error.to_string(),
                            },
                        )
                        .await?
                    }
                }
            }
        }
    }
}

enum Incoming {
    Request(TuiRequest),
    LifecycleChanged,
    Lagged,
}

async fn read_request<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<TuiRequest> {
    let payload = read_frame(reader).await?;
    let text = std::str::from_utf8(&payload)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    serde_json::from_str(text).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn request_identity(request: &TuiRequest) -> (u8, u64) {
    match request {
        TuiRequest::Hello { version, .. } => (*version, 0),
        TuiRequest::Subscribe {
            version,
            request_id,
        }
        | TuiRequest::ListProcesses {
            version,
            request_id,
        }
        | TuiRequest::StopProcess {
            version,
            request_id,
            ..
        }
        | TuiRequest::ShutdownAll {
            version,
            request_id,
            ..
        } => (*version, *request_id),
    }
}

fn process_snapshot(supervisor: &Supervisor) -> Vec<TuiProcess> {
    supervisor
        .managed_processes()
        .iter()
        .map(TuiProcess::from)
        .collect()
}

fn stop_result_name(result: ProcessStopResult) -> &'static str {
    match result {
        ProcessStopResult::GracefulStopRequested => "graceful_stop_requested",
        ProcessStopResult::ForceStopRequested => "force_stop_requested",
        ProcessStopResult::AlreadyStopping => "already_stopping",
        ProcessStopResult::AlreadyExited => "already_exited",
        ProcessStopResult::NotFound => "not_found",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    async fn client_request<T: Serialize>(writer: &mut (impl AsyncWrite + Unpin), request: &T) {
        write_frame(writer, request).await.unwrap();
    }

    async fn client_response<R: AsyncRead + Unpin>(reader: &mut R) -> TuiResponse {
        let payload = read_frame(reader).await.unwrap();
        serde_json::from_slice(&payload).unwrap()
    }

    #[test]
    fn process_context_roundtrips_through_tui_protocol() {
        let response = TuiResponse::ProcessChanged {
            version: PROTOCOL_VERSION,
            event_id: 7,
            process: TuiProcess {
                process_id: "v1:supervisor:1".into(),
                pid: 42,
                alive: true,
                executable: Some("/usr/bin/agent".into()),
                framework: Some("LangGraph".into()),
                run_label: Some("support-agent".into()),
                project_id: Some("project-a".into()),
                flow_run_id: Some("flow-12".into()),
                task_id: Some("task-17".into()),
                post_id: Some("post-17".into()),
                worker_id: Some("worker-1".into()),
                runtime_run_id: Some("runtime-2".into()),
            },
        };

        let json = serde_json::to_vec(&response).unwrap();
        let decoded: TuiResponse = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn handshake_and_subscribe_return_snapshot_before_commands() {
        let supervisor = Supervisor::new();
        let command = crate::Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let process_id = supervisor
            .start_background_with_metadata(
                &command,
                &crate::Policy::default(),
                crate::ManagedProcessMetadata::new(
                    Some("OpenAI Agents SDK".into()),
                    Some("support-run".into()),
                )
                .unwrap(),
            )
            .unwrap();
        let (server_stream, client_stream) = duplex(8192);
        let (server_read, server_write) = tokio::io::split(server_stream);
        let (mut client_read, mut client_write) = tokio::io::split(client_stream);
        let server_task = tokio::spawn(serve(server_read, server_write, supervisor.tui_view()));
        client_request(
            &mut client_write,
            &TuiRequest::Hello {
                version: 1,
                min_version: 1,
                max_version: 1,
            },
        )
        .await;
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client_response(&mut client_read)
            )
            .await
            .expect("hello response timeout"),
            TuiResponse::Hello {
                selected_version: 1,
                ..
            }
        ));
        client_request(
            &mut client_write,
            &TuiRequest::Subscribe {
                version: 1,
                request_id: 1,
            },
        )
        .await;
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client_response(&mut client_read)
            )
            .await
            .expect("snapshot response timeout"),
            TuiResponse::Snapshot { request_id: 1, processes, .. }
                if processes.len() == 1
                    && processes[0].process_id == process_id.to_string()
                    && processes[0].framework.as_deref() == Some("OpenAI Agents SDK")
                    && processes[0].run_label.as_deref() == Some("support-run")
        ));
        drop(client_write);
        drop(client_read);
        tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
            .await
            .expect("server task timeout")
            .unwrap();
        supervisor
            .shutdown_process(process_id, std::time::Duration::ZERO)
            .await
            .unwrap();
        supervisor.wait_process(process_id).await.unwrap();
    }

    #[tokio::test]
    async fn stop_request_rejects_foreign_supervisor_ids() {
        let supervisor = Supervisor::new();
        let foreign_process_id = format!("v1:{}:1", uuid::Uuid::new_v4());
        let (server_stream, client_stream) = duplex(8192);
        let (server_read, server_write) = tokio::io::split(server_stream);
        let (mut client_read, mut client_write) = tokio::io::split(client_stream);
        let server_task = tokio::spawn(serve(server_read, server_write, supervisor));
        client_request(
            &mut client_write,
            &TuiRequest::Hello {
                version: 1,
                min_version: 1,
                max_version: 1,
            },
        )
        .await;
        let _ = client_response(&mut client_read).await;
        client_request(
            &mut client_write,
            &TuiRequest::Subscribe {
                version: 1,
                request_id: 1,
            },
        )
        .await;
        let _ = client_response(&mut client_read).await;
        client_request(
            &mut client_write,
            &TuiRequest::StopProcess {
                version: 1,
                request_id: 2,
                process_id: foreign_process_id,
                grace_ms: 10,
            },
        )
        .await;
        assert!(matches!(
            client_response(&mut client_read).await,
            TuiResponse::Error { code, request_id: Some(2), .. } if code == "unknown_process"
        ));
        drop(client_write);
        drop(client_read);
        tokio::time::timeout(std::time::Duration::from_secs(2), server_task)
            .await
            .expect("server task timeout")
            .unwrap();
    }

    #[tokio::test]
    async fn frame_reader_rejects_zero_and_oversized_lengths() {
        for length in [0_u32, (MAX_FRAME_BYTES as u32) + 1] {
            let (mut writer, mut reader) = duplex(8);
            writer.write_all(&length.to_be_bytes()).await.unwrap();
            assert_eq!(
                read_frame(&mut reader).await.unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_supervisor_terminates_and_reaps_tui_child() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("tui-child.pid");
        let script = format!("echo $$ > '{}'; exec /bin/sleep 30", pid_file.display());
        let supervisor = Supervisor::new();
        supervisor
            .start_tui("/bin/sh", &["-c".into(), script.into()])
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        let pid = loop {
            if let Ok(contents) = std::fs::read_to_string(&pid_file) {
                break contents.trim().parse::<i32>().unwrap();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "TUI child did not start"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };

        drop(supervisor);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let alive = unsafe { libc::kill(pid, 0) == 0 };
            if !alive {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "TUI child remained alive after owner drop"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
