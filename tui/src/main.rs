use agent_supervisor::tui::{
    read_frame, write_frame, TuiProcess, TuiRequest, TuiResponse, PROTOCOL_VERSION,
};
use std::io::{self, BufRead, BufReader, Write};
use std::time::Duration;
use tokio::io::{stdin, stdout};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};

#[cfg(unix)]
mod service_client;

const REFRESH_PERIOD: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Confirmation {
    Stop(usize),
    StopAll,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingAction {
    Subscribe,
    List,
    Stop,
    StopAll,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("agent-supervisor-tui: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--service-socket") {
        #[cfg(unix)]
        {
            let options = ServiceOptions::parse(&args)?;
            let (protocol, bridge) =
                service_client::connect(&options.socket, &options.token_file, &options.role)
                    .await?;
            let (reader, writer) = tokio::io::split(protocol);
            let result = run_with_protocol(reader, writer, options.role == "tui").await;
            bridge.abort();
            return result;
        }
        #[cfg(not(unix))]
        return Err("service-client mode currently requires Unix sockets".into());
    }
    if !args.is_empty() {
        return Err("usage: agent-supervisor-tui [--service-socket PATH --token-file PATH [--role view|control]]".into());
    }
    run_with_protocol(stdin(), stdout(), true).await
}

#[cfg(unix)]
struct ServiceOptions {
    socket: std::path::PathBuf,
    token_file: std::path::PathBuf,
    role: String,
}

#[cfg(unix)]
impl ServiceOptions {
    fn parse(args: &[String]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut socket = None;
        let mut token_file = None;
        let mut role = "view".to_owned();
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--service-socket" => socket = args.next().map(std::path::PathBuf::from),
                "--token-file" => token_file = args.next().map(std::path::PathBuf::from),
                "--role" => role = args.next().ok_or("--role requires a value")?.clone(),
                "--help" | "-h" => return Err("usage: agent-supervisor-tui --service-socket PATH --token-file PATH [--role view|tui]".into()),
                _ => return Err(format!("unknown argument: {arg}").into()),
            }
        }
        if role != "view" && role != "tui" {
            return Err("--role must be view or tui".into());
        }
        Ok(Self {
            socket: socket.ok_or("--service-socket is required")?,
            token_file: token_file.ok_or("--token-file is required")?,
            role,
        })
    }
}

async fn run_with_protocol<R, W>(
    protocol_in: R,
    mut protocol_out: W,
    can_control: bool,
) -> Result<(), Box<dyn std::error::Error>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (terminal_in, mut terminal_out) = open_terminal()?;
    let (input_tx, mut input_rx) = mpsc::channel::<String>(8);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(terminal_in);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if input_tx.blocking_send(line.trim().to_owned()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let (response_tx, mut response_rx) = mpsc::channel(64);
    tokio::spawn(read_responses(protocol_in, response_tx));
    write_frame(
        &mut protocol_out,
        &TuiRequest::Hello {
            version: PROTOCOL_VERSION,
            min_version: PROTOCOL_VERSION,
            max_version: PROTOCOL_VERSION,
        },
    )
    .await?;
    match receive_response(&mut response_rx).await? {
        TuiResponse::Hello {
            selected_version, ..
        } if selected_version == PROTOCOL_VERSION => {}
        TuiResponse::UnsupportedVersion { .. } => {
            return Err("the supervisor does not support this TUI protocol version".into())
        }
        other => return Err(format!("unexpected handshake response: {other:?}").into()),
    }

    let mut next_request_id = 1_u64;
    let first_id = take_request_id(&mut next_request_id)?;
    write_frame(
        &mut protocol_out,
        &TuiRequest::Subscribe {
            version: PROTOCOL_VERSION,
            request_id: first_id,
        },
    )
    .await?;
    let mut processes = Vec::new();
    let mut pending = Some((first_id, PendingAction::Subscribe));
    let mut resync_after_pending = false;
    loop {
        let response = receive_response(&mut response_rx).await?;
        match response {
            TuiResponse::ProcessChanged { process, .. } => update_process(&mut processes, process),
            TuiResponse::ProcessRemoved { process_id, .. } => {
                processes.retain(|process| process.process_id != process_id);
            }
            TuiResponse::ResyncRequired { .. } => resync_after_pending = true,
            TuiResponse::Snapshot {
                request_id,
                processes: snapshot,
                ..
            } if pending == Some((request_id, PendingAction::Subscribe)) => {
                processes = snapshot;
                pending = None;
                break;
            }
            TuiResponse::Error { code, message, .. } => {
                return Err(format!("{code}: {message}").into())
            }
            other => return Err(format!("unexpected subscription response: {other:?}").into()),
        }
    }
    if resync_after_pending {
        let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
        pending = Some((request_id, PendingAction::List));
    }

    let mut selected = 0_usize;
    let mut detail = false;
    let mut confirmation = None;
    let mut notice = String::from("Connected. Process list refreshes every second.");
    let mut refresh = interval(REFRESH_PERIOD);
    refresh.tick().await;

    loop {
        draw(
            &mut terminal_out,
            &processes,
            selected,
            detail,
            confirmation,
            &notice,
            can_control,
        )?;

        tokio::select! {
            _ = refresh.tick() => {
                if pending.is_none() {
                    let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
                    pending = Some((request_id, PendingAction::List));
                }
            }
            response = response_rx.recv() => {
                let Some(response) = response else {
                    return Err("the supervisor control channel closed".into());
                };
                match response {
                    TuiResponse::ProcessChanged { process, .. } => update_process(&mut processes, process),
                    TuiResponse::ProcessRemoved { process_id, .. } => processes.retain(|process| process.process_id != process_id),
                    TuiResponse::ResyncRequired { .. } => {
                        resync_after_pending = true;
                        notice = "Supervisor event stream requested a process-list resync.".into();
                    }
                    TuiResponse::Snapshot { request_id, processes: snapshot, .. }
                    | TuiResponse::Processes { request_id, processes: snapshot, .. } => {
                        if let Some((expected, action)) = pending {
                            if request_id == expected && matches!(action, PendingAction::Subscribe | PendingAction::List) {
                                processes = snapshot;
                                pending = None;
                                resync_after_pending = false;
                            }
                        }
                    }
                    TuiResponse::StopResult { request_id, outcome, .. } => {
                        if pending.is_some_and(|(expected, action)| expected == request_id && matches!(action, PendingAction::Stop)) {
                            pending = None;
                            notice = format!("Stop requested: {outcome}.");
                        }
                    }
                    TuiResponse::ShutdownResult { request_id, outcomes, .. } => {
                        if pending.is_some_and(|(expected, action)| expected == request_id && matches!(action, PendingAction::StopAll)) {
                            pending = None;
                            notice = format!("Stop requested for {} process(es).", outcomes.len());
                        }
                    }
                    TuiResponse::Error { request_id, code, message, .. } => {
                        if request_id.is_none() || pending.is_some_and(|(expected, _)| Some(expected) == request_id) {
                            pending = None;
                            notice = format!("{code}: {message}");
                        }
                    }
                    TuiResponse::Hello { .. } | TuiResponse::UnsupportedVersion { .. } => {
                        notice = "Received an unexpected supervisor protocol response.".into();
                    }
                }
                selected = selected.min(processes.len().saturating_sub(1));
                if pending.is_none() && resync_after_pending {
                    let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
                    pending = Some((request_id, PendingAction::List));
                }
            }
            command = input_rx.recv() => {
                let Some(command) = command else { return Ok(()); };
                let command = command.trim();
                if let Some(confirmed_action) = confirmation.take() {
                    if command.eq_ignore_ascii_case("y") || command.eq_ignore_ascii_case("yes") {
                        match confirmed_action {
                            Confirmation::Stop(index) => {
                                if let Some(process) = processes.get(index).cloned() {
                                    if pending.is_some() {
                                        notice = "A supervisor request is already in progress.".into();
                                    } else {
                                        let request_id = take_request_id(&mut next_request_id)?;
                                        write_frame(&mut protocol_out, &TuiRequest::StopProcess { version: PROTOCOL_VERSION, request_id, process_id: process.process_id, grace_ms: 5_000 }).await?;
                                        pending = Some((request_id, PendingAction::Stop));
                                    }
                                } else {
                                    notice = "That process has already exited.".into();
                                }
                            }
                            Confirmation::StopAll => {
                                if pending.is_some() {
                                    notice = "A supervisor request is already in progress.".into();
                                } else {
                                    let request_id = take_request_id(&mut next_request_id)?;
                                    write_frame(&mut protocol_out, &TuiRequest::ShutdownAll { version: PROTOCOL_VERSION, request_id, grace_ms: 5_000 }).await?;
                                    pending = Some((request_id, PendingAction::StopAll));
                                }
                            }
                        }
                    } else {
                        notice = "Cancelled.".into();
                    }
                    continue;
                }
                match command.to_ascii_lowercase().as_str() {
                    "q" | "quit" | "exit" => return Ok(()),
                    "j" | "down" => {
                        selected = (selected + 1).min(processes.len().saturating_sub(1));
                        detail = false;
                    }
                    "k" | "up" => {
                        selected = selected.saturating_sub(1);
                        detail = false;
                    }
                    "i" | "info" => detail = !detail,
                    "s" | "stop" => {
                        if !can_control {
                            notice = "This TUI connection has view-only permission.".into();
                        } else if processes.get(selected).is_some() {
                            confirmation = Some(Confirmation::Stop(selected));
                            notice = format!("Stop process #{}? Type y to confirm, anything else to cancel.", selected + 1);
                        } else {
                            notice = "There is no process selected.".into();
                        }
                    }
                    "a" | "all" => {
                        if !can_control {
                            notice = "This TUI connection has view-only permission.".into();
                        } else if processes.is_empty() {
                            notice = "No managed processes are running.".into();
                        } else {
                            confirmation = Some(Confirmation::StopAll);
                            notice = "Stop all managed processes? Type y to confirm, anything else to cancel.".into();
                        }
                    }
                    "r" | "refresh" => {
                        if pending.is_none() {
                            let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
                            pending = Some((request_id, PendingAction::List));
                            notice = "Process list refresh requested.".into();
                        } else {
                            notice = "A supervisor request is already in progress.".into();
                        }
                    }
                    "?" | "help" => notice = if can_control { "Commands: j/k move, i details, s stop selected, a stop all, r refresh, q quit." } else { "Commands: j/k move, i details, r refresh, q quit." }.into(),
                    _ => notice = "Unknown command. Type ? for help.".into(),
                }
            }
        }
    }
}

async fn send_list_request(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    next_id: &mut u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let request_id = take_request_id(next_id)?;
    write_frame(
        writer,
        &TuiRequest::ListProcesses {
            version: PROTOCOL_VERSION,
            request_id,
        },
    )
    .await?;
    Ok(request_id)
}

async fn read_responses(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    sender: mpsc::Sender<TuiResponse>,
) {
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(frame) => frame,
            Err(_) => break,
        };
        let response = match serde_json::from_slice(&frame) {
            Ok(response) => response,
            Err(_) => break,
        };
        if sender.send(response).await.is_err() {
            break;
        }
    }
}

async fn receive_response(
    responses: &mut mpsc::Receiver<TuiResponse>,
) -> Result<TuiResponse, Box<dyn std::error::Error>> {
    timeout(REQUEST_TIMEOUT, responses.recv())
        .await?
        .ok_or_else(|| "the supervisor control channel closed".into())
}

fn update_process(processes: &mut Vec<TuiProcess>, process: TuiProcess) {
    if let Some(existing) = processes
        .iter_mut()
        .find(|existing| existing.process_id == process.process_id)
    {
        *existing = process;
    } else {
        processes.push(process);
        processes.sort_by(|a, b| a.process_id.cmp(&b.process_id));
    }
}

#[cfg(test)]
fn apply_event(processes: &mut Vec<TuiProcess>, response: TuiResponse) -> Option<bool> {
    match response {
        TuiResponse::ProcessChanged { process, .. } => {
            update_process(processes, process);
            Some(false)
        }
        TuiResponse::ProcessRemoved { process_id, .. } => {
            processes.retain(|process| process.process_id != process_id);
            Some(false)
        }
        TuiResponse::ResyncRequired { .. } => Some(true),
        _ => None,
    }
}

fn take_request_id(next_id: &mut u64) -> Result<u64, Box<dyn std::error::Error>> {
    let current = *next_id;
    *next_id = next_id.checked_add(1).ok_or("request ID exhausted")?;
    Ok(current)
}

fn draw(
    output: &mut impl Write,
    processes: &[TuiProcess],
    selected: usize,
    detail: bool,
    confirmation: Option<Confirmation>,
    notice: &str,
    can_control: bool,
) -> io::Result<()> {
    write!(output, "\x1b[2J\x1b[H")?;
    writeln!(output, "Agent Supervisor · managed processes")?;
    writeln!(
        output,
        "{} process(es) · refreshing every second",
        processes.iter().filter(|p| p.alive).count()
    )?;
    writeln!(output, "")?;
    writeln!(output, "  #  STATE    PID       FRAMEWORK      PROCESS")?;
    writeln!(
        output,
        "  -- -------  --------  -------------  --------------------------------"
    )?;
    if processes.is_empty() {
        writeln!(output, "     (no processes are currently managed)")?;
    } else {
        let mut ordered = processes.iter().enumerate().collect::<Vec<_>>();
        ordered.sort_by(|(_, a), (_, b)| {
            a.project_id
                .cmp(&b.project_id)
                .then(a.flow_run_id.cmp(&b.flow_run_id))
                .then(a.task_id.cmp(&b.task_id))
                .then(a.post_id.cmp(&b.post_id))
                .then(a.worker_id.cmp(&b.worker_id))
                .then(a.runtime_run_id.cmp(&b.runtime_run_id))
                .then(a.pid.cmp(&b.pid))
        });
        for (row_index, (original_index, process)) in ordered.into_iter().enumerate() {
            let marker = if original_index == selected { ">" } else { " " };
            let status = if process.alive { "running" } else { "exited " };
            let framework = process.framework.as_deref().unwrap_or("-");
            let command = process
                .run_label
                .as_deref()
                .or(process.executable.as_deref())
                .unwrap_or("unknown");
            writeln!(
                output,
                "{marker} {:>2} {status}  {:>8}  {:<13.13}  {}",
                row_index + 1,
                process.pid,
                framework,
                sanitize(command, 56)
            )?;
        }
    }
    if detail {
        if let Some(process) = processes.get(selected) {
            writeln!(output, "\nSelected process details")?;
            writeln!(
                output,
                "  supervisor ID: {}",
                sanitize(&process.process_id, 96)
            )?;
            writeln!(output, "  OS PID:        {}", process.pid)?;
            writeln!(
                output,
                "  executable:    {}",
                sanitize(process.executable.as_deref().unwrap_or("unknown"), 160)
            )?;
            writeln!(
                output,
                "  framework:     {}",
                sanitize(process.framework.as_deref().unwrap_or("not provided"), 80)
            )?;
            writeln!(
                output,
                "  run label:     {}",
                sanitize(process.run_label.as_deref().unwrap_or("not provided"), 80)
            )?;
            writeln!(
                output,
                "  project:       {}",
                sanitize(process.project_id.as_deref().unwrap_or("not provided"), 80)
            )?;
            writeln!(
                output,
                "  FlowRun:       {}",
                sanitize(process.flow_run_id.as_deref().unwrap_or("not provided"), 80)
            )?;
            writeln!(
                output,
                "  task:          {}",
                sanitize(process.task_id.as_deref().unwrap_or("not provided"), 80)
            )?;
            writeln!(
                output,
                "  task post:     {}",
                sanitize(process.post_id.as_deref().unwrap_or("not provided"), 96)
            )?;
            writeln!(
                output,
                "  worker:        {}",
                sanitize(process.worker_id.as_deref().unwrap_or("not provided"), 96)
            )?;
            writeln!(
                output,
                "  runtime run:   {}",
                sanitize(
                    process.runtime_run_id.as_deref().unwrap_or("not provided"),
                    96
                )
            )?;
            writeln!(
                output,
                "  state:         {}",
                if process.alive { "running" } else { "exited" }
            )?;
        }
    }
    writeln!(output, "\n{notice}")?;
    if confirmation.is_some() {
        write!(output, "confirm > ")?;
    } else {
        if can_control {
            write!(
                output,
                "command [j/k move, i details, s stop, a stop all, r refresh, q quit, ? help] > "
            )?;
        } else {
            write!(
                output,
                "command [j/k move, i details, r refresh, q quit, ? help] > "
            )?;
        }
    }
    output.flush()
}

fn sanitize(text: &str, max_chars: usize) -> String {
    text.chars()
        .filter(|ch| !ch.is_control())
        .take(max_chars)
        .collect()
}

#[cfg(unix)]
fn open_terminal() -> io::Result<(std::fs::File, std::fs::File)> {
    use std::fs::OpenOptions;
    let input = OpenOptions::new().read(true).open("/dev/tty")?;
    let output = OpenOptions::new().write(true).open("/dev/tty")?;
    Ok((input, output))
}

#[cfg(windows)]
fn open_terminal() -> io::Result<(std::fs::File, std::fs::File)> {
    use std::fs::OpenOptions;
    let input = OpenOptions::new().read(true).open("CONIN$")?;
    let output = OpenOptions::new().write(true).open("CONOUT$")?;
    Ok((input, output))
}

#[cfg(not(any(unix, windows)))]
fn open_terminal() -> io::Result<(std::fs::File, std::fs::File)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this platform does not provide a supported controlling-terminal device",
    ))
}

#[cfg(test)]
mod tests {
    use super::{apply_event, draw, read_responses, sanitize};
    use agent_supervisor::tui::{read_frame, write_frame, TuiProcess, TuiRequest, TuiResponse};
    use tokio::io::duplex;

    #[test]
    fn sanitize_removes_terminal_control_characters_and_bounds_text() {
        assert_eq!(sanitize("ok\x1b[31mBAD\n", 8), "ok[31mBA");
        assert_eq!(sanitize("abcdef", 3), "abc");
    }

    #[test]
    fn view_only_tui_prompt_does_not_offer_stop_controls() {
        let mut output = Vec::new();
        draw(&mut output, &[], 0, false, None, "Connected", false).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("j/k move, i details, r refresh"));
        assert!(!output.contains("s stop"));
        assert!(!output.contains("a stop all"));
    }

    #[tokio::test]
    async fn interleaved_process_event_and_resync_before_list_response_are_handled() {
        let (client, server) = duplex(4096);
        let (client_reader, mut client_writer) = tokio::io::split(client);
        let (mut server_reader, mut server_writer) = tokio::io::split(server);
        let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(read_responses(client_reader, response_tx));

        write_frame(
            &mut client_writer,
            &TuiRequest::ListProcesses {
                version: 1,
                request_id: 1,
            },
        )
        .await
        .unwrap();
        let server_task = tokio::spawn(async move {
            let request: TuiRequest =
                serde_json::from_slice(&read_frame(&mut server_reader).await.unwrap()).unwrap();
            assert!(matches!(
                request,
                TuiRequest::ListProcesses { request_id: 1, .. }
            ));

            let event_process = TuiProcess {
                process_id: "v1:supervisor:1".into(),
                pid: 42,
                alive: true,
                executable: Some("agent-worker".into()),
                framework: Some("LangGraph".into()),
                run_label: Some("research".into()),
                project_id: Some("demo-project".into()),
                flow_run_id: Some("flow-demo".into()),
                task_id: Some("task-17".into()),
                post_id: Some("post-17".into()),
                worker_id: Some("worker-1".into()),
                runtime_run_id: Some("runtime-2".into()),
            };
            write_frame(
                &mut server_writer,
                &TuiResponse::ProcessChanged {
                    version: 1,
                    event_id: 9,
                    process: event_process.clone(),
                },
            )
            .await
            .unwrap();
            write_frame(
                &mut server_writer,
                &TuiResponse::ResyncRequired {
                    version: 1,
                    event_id: 10,
                },
            )
            .await
            .unwrap();
            write_frame(
                &mut server_writer,
                &TuiResponse::Processes {
                    version: 1,
                    request_id: 1,
                    processes: vec![event_process],
                },
            )
            .await
            .unwrap();
        });

        let mut processes = Vec::new();
        let mut saw_resync = false;
        let mut list_snapshot = None;
        while list_snapshot.is_none() {
            let response = response_rx.recv().await.unwrap();
            if let Some(resync) = apply_event(&mut processes, response.clone()) {
                saw_resync |= resync;
                continue;
            }
            if let TuiResponse::Processes {
                request_id: 1,
                processes: snapshot,
                ..
            } = response
            {
                list_snapshot = Some(snapshot);
            }
        }
        if saw_resync {
            processes = list_snapshot.unwrap();
        }
        server_task.await.unwrap();

        assert!(saw_resync);
        assert_eq!(processes.len(), 1);
        assert_eq!(processes[0].framework.as_deref(), Some("LangGraph"));
        assert_eq!(processes[0].process_id, "v1:supervisor:1");
    }
}
