use agent_supervisor::tui::{
    read_frame, write_frame, TuiActivity, TuiAttempt, TuiCoordinatorProcess, TuiProcess, TuiRequest, TuiResponse, TuiRun, TuiToolActivity,
    PROTOCOL_VERSION,
};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::backend::{CrosstermBackend, TestBackend};
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, BorderType, Cell, Clear, Paragraph, Row, Table, TableState, Wrap};
use ratatui::{Frame, Terminal};
use std::io::{self, Write};
use std::time::Duration;
use tokio::io::{stdin, stdout};
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};

#[cfg(unix)]
mod service_client;

const REFRESH_PERIOD: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_SNAPSHOT_SIZE: (u16, u16) = (110, 34);
const USAGE: &str =
    "usage: agent-supervisor-tui [--service-socket PATH --token-file PATH [--role view|tui]] \
[--snapshot [--from FILE] [--size COLSxROWS] [--format text|svg]]";

#[derive(Clone, Debug, PartialEq, Eq)]
enum Confirmation {
    /// Stop the process with this Supervisor process ID.
    Stop(String),
    StopAll,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingAction {
    List,
    Stop,
    StopAll,
}

/// Everything the screen shows. `render` draws it; nothing else writes to the terminal.
#[derive(Default)]
struct View {
    runs: Vec<TuiRun>,
    activities: Vec<TuiActivity>,
    attempts: Vec<TuiAttempt>,
    flow_runs: Vec<serde_json::Value>,
    coordinator_process: Option<TuiCoordinatorProcess>,
    tool_requests: Vec<TuiToolActivity>,
    processes: Vec<TuiProcess>,
    /// Position in display order (see `display_order`).
    selected: usize,
    detail: bool,
    help: bool,
    confirmation: Option<Confirmation>,
    notice: String,
    can_control: bool,
    /// Lines scrolled in the runs and FlowRuns panel.
    scroll: u16,
}

impl View {
    fn apply_snapshot(
        &mut self,
        processes: Vec<TuiProcess>,
        runs: Vec<TuiRun>,
        activities: Vec<TuiActivity>,
        attempts: Vec<TuiAttempt>,
        flow_runs: Vec<serde_json::Value>,
        coordinator_process: Option<TuiCoordinatorProcess>,
        tool_requests: Vec<TuiToolActivity>,
    ) {
        self.processes = processes;
        self.runs = runs;
        self.activities = activities;
        self.attempts = attempts;
        self.flow_runs = flow_runs;
        self.coordinator_process = coordinator_process;
        self.tool_requests = tool_requests;
        self.clamp_selection();
    }

    fn clamp_selection(&mut self) {
        self.selected = self.selected.min(self.processes.len().saturating_sub(1));
    }

    /// The selected process, in display order.
    fn selected_process(&self) -> Option<&TuiProcess> {
        display_order(&self.processes)
            .get(self.selected)
            .map(|index| &self.processes[*index])
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SnapshotFormat {
    Text,
    Svg,
}

#[derive(Debug, PartialEq, Eq)]
struct SnapshotOptions {
    from: Option<std::path::PathBuf>,
    size: (u16, u16),
    format: SnapshotFormat,
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    service: Option<ServiceOptions>,
    snapshot: Option<SnapshotOptions>,
}

#[derive(Debug, PartialEq, Eq)]
struct ServiceOptions {
    socket: std::path::PathBuf,
    token_file: std::path::PathBuf,
    role: String,
}

impl Options {
    fn parse(args: &[String]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut socket = None;
        let mut token_file = None;
        let mut role = "view".to_owned();
        let mut snapshot = false;
        let mut from = None;
        let mut size = DEFAULT_SNAPSHOT_SIZE;
        let mut format = SnapshotFormat::Text;
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--service-socket" => socket = args.next().map(std::path::PathBuf::from),
                "--token-file" => token_file = args.next().map(std::path::PathBuf::from),
                "--role" => role = args.next().ok_or("--role requires a value")?.clone(),
                "--snapshot" => snapshot = true,
                "--from" => {
                    from = Some(std::path::PathBuf::from(
                        args.next().ok_or("--from requires a file")?,
                    ))
                }
                "--size" => size = parse_size(args.next().ok_or("--size requires COLSxROWS")?)?,
                "--format" => {
                    format = match args.next().map(String::as_str) {
                        Some("text") => SnapshotFormat::Text,
                        Some("svg") => SnapshotFormat::Svg,
                        _ => return Err("--format must be text or svg".into()),
                    }
                }
                "--help" | "-h" => return Err(USAGE.into()),
                _ => return Err(format!("unknown argument: {arg}\n{USAGE}").into()),
            }
        }
        if role != "view" && role != "tui" {
            return Err("--role must be view or tui".into());
        }
        let service = match (socket, token_file) {
            (Some(socket), Some(token_file)) => Some(ServiceOptions {
                socket,
                token_file,
                role,
            }),
            (None, None) => None,
            (None, Some(_)) => return Err("--service-socket is required".into()),
            (Some(_), None) => return Err("--token-file is required".into()),
        };
        if !snapshot
            && (from.is_some() || size != DEFAULT_SNAPSHOT_SIZE || format != SnapshotFormat::Text)
        {
            return Err("--from, --size and --format need --snapshot".into());
        }
        if snapshot && from.is_none() && service.is_none() {
            return Err(
                "--snapshot needs --from FILE, or --service-socket PATH --token-file PATH".into(),
            );
        }
        Ok(Self {
            service,
            snapshot: snapshot.then_some(SnapshotOptions { from, size, format }),
        })
    }
}

fn parse_size(value: &str) -> Result<(u16, u16), Box<dyn std::error::Error>> {
    let (cols, rows) = value
        .split_once('x')
        .ok_or("--size must look like 110x34")?;
    let size = (cols.parse::<u16>()?, rows.parse::<u16>()?);
    if size.0 < 40 || size.1 < 12 {
        return Err("--size must be at least 40x12".into());
    }
    Ok(size)
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
    let options = Options::parse(&args)?;
    if let Some(snapshot) = options.snapshot {
        let view = match (&snapshot.from, &options.service) {
            (Some(path), _) => view_from_file(path)?,
            (None, Some(service)) => view_from_service(service).await?,
            (None, None) => unreachable!("checked by Options::parse"),
        };
        let buffer = render_to_buffer(&view, snapshot.size);
        let output = match snapshot.format {
            SnapshotFormat::Text => buffer_to_text(&buffer),
            SnapshotFormat::Svg => buffer_to_svg(&buffer),
        };
        io::stdout().write_all(output.as_bytes())?;
        return Ok(());
    }
    if let Some(service) = options.service {
        #[cfg(unix)]
        {
            let (protocol, bridge) =
                service_client::connect(&service.socket, &service.token_file, &service.role)
                    .await?;
            let (reader, writer) = tokio::io::split(protocol);
            let result = run_with_protocol(reader, writer, service.role == "tui").await;
            bridge.abort();
            return result;
        }
        #[cfg(not(unix))]
        {
            let _ = service;
            return Err("service-client mode currently requires Unix sockets".into());
        }
    }
    run_with_protocol(stdin(), stdout(), true).await
}

/// A view from a saved Supervisor state: a JSON object with the `processes`,
/// `runs`, `activities`, `attempts` and `flowRuns` of a TUI snapshot
/// response. Missing lists are empty.
fn view_from_file(path: &std::path::Path) -> Result<View, Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    fn list<T: serde::de::DeserializeOwned>(
        value: &serde_json::Value,
        key: &str,
    ) -> Result<Vec<T>, Box<dyn std::error::Error>> {
        Ok(match value.get(key) {
            Some(items) => {
                serde_json::from_value(items.clone()).map_err(|error| format!("{key}: {error}"))?
            }
            None => Vec::new(),
        })
    }
    let mut view = View {
        notice: "Snapshot of a saved Supervisor state.".into(),
        can_control: true,
        ..View::default()
    };
    view.apply_snapshot(
        list(&value, "processes")?,
        list(&value, "runs")?,
        list(&value, "activities")?,
        list(&value, "attempts")?,
        list(&value, "flowRuns")?,
        value.get("coordinatorProcess").cloned().map(serde_json::from_value).transpose()?,
        list(&value, "toolRequests")?,
    );
    Ok(view)
}

/// One snapshot of the live Supervisor, read over its service socket.
async fn view_from_service(service: &ServiceOptions) -> Result<View, Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        let (protocol, bridge) =
            service_client::connect(&service.socket, &service.token_file, &service.role).await?;
        let (reader, mut writer) = tokio::io::split(protocol);
        let (response_tx, mut response_rx) = mpsc::channel(64);
        tokio::spawn(read_responses(reader, response_tx));
        let mut next_request_id = 1_u64;
        let mut view = View {
            notice: "Snapshot of the live Supervisor.".into(),
            can_control: service.role == "tui",
            ..View::default()
        };
        subscribe(
            &mut writer,
            &mut response_rx,
            &mut next_request_id,
            &mut view,
        )
        .await?;
        bridge.abort();
        Ok(view)
    }
    #[cfg(not(unix))]
    {
        let _ = service;
        Err("service-client mode currently requires Unix sockets".into())
    }
}

/// Handshakes and subscribes, filling `view` from the first snapshot. Returns
/// whether a resync was requested while waiting for it.
async fn subscribe<W>(
    protocol_out: &mut W,
    response_rx: &mut mpsc::Receiver<TuiResponse>,
    next_request_id: &mut u64,
    view: &mut View,
) -> Result<bool, Box<dyn std::error::Error>>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    write_frame(
        protocol_out,
        &TuiRequest::Hello {
            version: PROTOCOL_VERSION,
            min_version: PROTOCOL_VERSION,
            max_version: PROTOCOL_VERSION,
        },
    )
    .await?;
    match receive_response(response_rx).await? {
        TuiResponse::Hello {
            selected_version, ..
        } if selected_version == PROTOCOL_VERSION => {}
        TuiResponse::UnsupportedVersion { .. } => {
            return Err("the supervisor does not support this TUI protocol version".into())
        }
        other => return Err(format!("unexpected handshake response: {other:?}").into()),
    }
    let first_id = take_request_id(next_request_id)?;
    write_frame(
        protocol_out,
        &TuiRequest::Subscribe {
            version: PROTOCOL_VERSION,
            request_id: first_id,
        },
    )
    .await?;
    let mut resync = false;
    loop {
        match receive_response(response_rx).await? {
            TuiResponse::ToolActivityChanged { activity, .. } => update_tool_activity(&mut view.tool_requests, activity),
            TuiResponse::ProcessChanged { process, .. } => {
                update_process(&mut view.processes, process)
            }
            TuiResponse::ProcessRemoved { process_id, .. } => {
                view.processes
                    .retain(|process| process.process_id != process_id);
            }
            TuiResponse::ResyncRequired { .. } => resync = true,
            TuiResponse::Snapshot {
                request_id,
                processes,
                runs,
                activities,
                attempts,
                flow_runs,
                coordinator_process,
                tool_requests,
                ..
            } if request_id == first_id => {
                view.apply_snapshot(processes, runs, activities, attempts, flow_runs, coordinator_process, tool_requests);
                return Ok(resync);
            }
            TuiResponse::Error { code, message, .. } => {
                return Err(format!("{code}: {message}").into())
            }
            other => return Err(format!("unexpected subscription response: {other:?}").into()),
        }
    }
}

/// Restores the terminal however the TUI exits.
struct TerminalGuard {
    output: std::fs::File,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(
            self.output,
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show
        );
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
    // The screen goes to the controlling terminal: when the Supervisor
    // launches the TUI, stdin and stdout are its private protocol pipes.
    let terminal_out = open_terminal_output().map_err(|error| {
        format!(
            "an attached interactive terminal is required ({error}); --snapshot renders without one"
        )
    })?;
    let (response_tx, mut response_rx) = mpsc::channel(64);
    tokio::spawn(read_responses(protocol_in, response_tx));
    let mut next_request_id = 1_u64;
    let mut view = View {
        notice: "Connected. Process list refreshes every second.".into(),
        can_control,
        ..View::default()
    };
    let mut resync_after_pending = subscribe(
        &mut protocol_out,
        &mut response_rx,
        &mut next_request_id,
        &mut view,
    )
    .await?;
    let mut pending = None;
    if resync_after_pending {
        let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
        pending = Some((request_id, PendingAction::List));
    }

    crossterm::terminal::enable_raw_mode()?;
    let guard = TerminalGuard {
        output: terminal_out.try_clone()?,
    };
    let mut terminal = Terminal::new(CrosstermBackend::new(terminal_out))?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::terminal::EnterAlternateScreen,
        crossterm::terminal::Clear(crossterm::terminal::ClearType::All),
        crossterm::cursor::Hide
    )?;
    // Not terminal.clear(): it asks the terminal where the cursor is, and a
    // terminal that does not answer would stop the TUI. The first draw fills
    // the whole alternate screen anyway.

    // Keys are read on their own thread, because crossterm's read blocks.
    let (key_tx, mut key_rx) = mpsc::channel::<KeyEvent>(16);
    std::thread::spawn(move || loop {
        match crossterm::event::read() {
            Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                if key_tx.blocking_send(key).is_err() {
                    break;
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    });

    let mut service_stale = false;
    let mut refresh = interval(REFRESH_PERIOD);
    refresh.tick().await;

    let result: Result<(), Box<dyn std::error::Error>> = async {
        loop {
            terminal.draw(|frame| render(frame, &view))?;

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
                        TuiResponse::ToolActivityChanged { activity, .. } => update_tool_activity(&mut view.tool_requests, activity),
                        TuiResponse::ProcessChanged { process, .. } => update_process(&mut view.processes, process),
                        TuiResponse::ProcessRemoved { process_id, .. } => view.processes.retain(|process| process.process_id != process_id),
                        TuiResponse::ResyncRequired { .. } => {
                            resync_after_pending = true;
                            view.notice = "Supervisor event stream requested a process-list resync.".into();
                        }
                        TuiResponse::Snapshot { request_id, processes, runs, activities, attempts, flow_runs, coordinator_process, tool_requests, .. }
                        | TuiResponse::Processes { request_id, processes, runs, activities, attempts, flow_runs, coordinator_process, tool_requests, .. } => {
                            if let Some((expected, action)) = pending {
                                if request_id == expected && action == PendingAction::List {
                                    view.apply_snapshot(processes, runs, activities, attempts, flow_runs, coordinator_process, tool_requests);
                                    pending = None;
                                    resync_after_pending = false;
                                    if service_stale {
                                        service_stale = false;
                                        view.notice = "Supervisor reconnected; current state refreshed.".into();
                                    }
                                }
                            }
                        }
                        TuiResponse::StopResult { request_id, outcome, .. } => {
                            if pending.is_some_and(|(expected, action)| expected == request_id && matches!(action, PendingAction::Stop)) {
                                pending = None;
                                view.notice = format!("Stop request issued: {outcome}; waiting for worker exit evidence.");
                            }
                        }
                        TuiResponse::ShutdownResult { request_id, outcomes, .. } => {
                            if pending.is_some_and(|(expected, action)| expected == request_id && matches!(action, PendingAction::StopAll)) {
                                pending = None;
                                view.notice = format!("Stop requested for {} process(es); waiting for exit evidence.", outcomes.len());
                            }
                        }
                        TuiResponse::Error { request_id, code, message, .. } => {
                            if request_id.is_none() || pending.is_some_and(|(expected, _)| Some(expected) == request_id) {
                                pending = None;
                                if code == "disconnected" {
                                    service_stale = true;
                                    view.notice = format!("Supervisor connection stale/disconnected: {message}");
                                } else {
                                    view.notice = format!("{code}: {message}");
                                }
                            }
                        }
                        TuiResponse::Hello { .. } | TuiResponse::UnsupportedVersion { .. } => {
                            view.notice = "Received an unexpected supervisor protocol response.".into();
                        }
                    }
                    view.clamp_selection();
                    if pending.is_none() && resync_after_pending {
                        let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
                        pending = Some((request_id, PendingAction::List));
                    }
                }
                key = key_rx.recv() => {
                    let Some(key) = key else { return Ok(()); };
                    match handle_key(&mut view, key, pending.is_some()) {
                        KeyOutcome::Quit => return Ok(()),
                        KeyOutcome::None => {}
                        KeyOutcome::Refresh => {
                            let request_id = send_list_request(&mut protocol_out, &mut next_request_id).await?;
                            pending = Some((request_id, PendingAction::List));
                        }
                        KeyOutcome::Stop(process_id) => {
                            let request_id = take_request_id(&mut next_request_id)?;
                            write_frame(&mut protocol_out, &TuiRequest::StopProcess { version: PROTOCOL_VERSION, request_id, process_id, grace_ms: 5_000 }).await?;
                            pending = Some((request_id, PendingAction::Stop));
                        }
                        KeyOutcome::StopAll => {
                            let request_id = take_request_id(&mut next_request_id)?;
                            write_frame(&mut protocol_out, &TuiRequest::ShutdownAll { version: PROTOCOL_VERSION, request_id, grace_ms: 5_000 }).await?;
                            pending = Some((request_id, PendingAction::StopAll));
                        }
                    }
                }
            }
        }
    }
    .await;
    drop(terminal);
    drop(guard);
    result
}

/// What a key asks the event loop to do.
#[derive(Debug, PartialEq, Eq)]
enum KeyOutcome {
    None,
    Quit,
    Refresh,
    Stop(String),
    StopAll,
}

/// Applies one key to the view. Requests to the Supervisor are returned, not sent.
fn handle_key(view: &mut View, key: KeyEvent, request_pending: bool) -> KeyOutcome {
    let busy = "A supervisor request is already in progress.";
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        return KeyOutcome::Quit;
    }
    if let Some(confirmation) = view.confirmation.take() {
        if !matches!(key.code, KeyCode::Char('y') | KeyCode::Char('Y')) {
            view.notice = "Cancelled.".into();
            return KeyOutcome::None;
        }
        if request_pending {
            view.notice = busy.into();
            return KeyOutcome::None;
        }
        return match confirmation {
            Confirmation::Stop(process_id) => {
                if view
                    .processes
                    .iter()
                    .any(|process| process.process_id == process_id)
                {
                    KeyOutcome::Stop(process_id)
                } else {
                    view.notice = "That process has already exited.".into();
                    KeyOutcome::None
                }
            }
            Confirmation::StopAll => KeyOutcome::StopAll,
        };
    }
    if view.help {
        view.help = false;
        return KeyOutcome::None;
    }
    match key.code {
        KeyCode::Char('q') => return KeyOutcome::Quit,
        KeyCode::Esc if view.detail => view.detail = false,
        KeyCode::Esc => return KeyOutcome::Quit,
        KeyCode::Char('j') | KeyCode::Down => {
            view.selected = (view.selected + 1).min(view.processes.len().saturating_sub(1));
            view.detail = false;
        }
        KeyCode::Char('k') | KeyCode::Up => {
            view.selected = view.selected.saturating_sub(1);
            view.detail = false;
        }
        KeyCode::Char('i') | KeyCode::Enter => {
            view.detail = !view.detail && view.selected_process().is_some()
        }
        KeyCode::PageDown => view.scroll = view.scroll.saturating_add(10),
        KeyCode::PageUp => view.scroll = view.scroll.saturating_sub(10),
        KeyCode::Char('s') => {
            if !view.can_control {
                view.notice = "This TUI connection has view-only permission.".into();
            } else if let Some(process) = view.selected_process() {
                view.confirmation = Some(Confirmation::Stop(process.process_id.clone()));
            } else {
                view.notice = "There is no process selected.".into();
            }
        }
        KeyCode::Char('a') => {
            if !view.can_control {
                view.notice = "This TUI connection has view-only permission.".into();
            } else if view.processes.is_empty() {
                view.notice = "No managed processes are running.".into();
            } else {
                view.confirmation = Some(Confirmation::StopAll);
            }
        }
        KeyCode::Char('r') => {
            if request_pending {
                view.notice = busy.into();
            } else {
                view.notice = "Process list refresh requested.".into();
                return KeyOutcome::Refresh;
            }
        }
        KeyCode::Char('?') => view.help = true,
        _ => view.notice = "Unknown key. Press ? for help.".into(),
    }
    KeyOutcome::None
}

/// Process indices in observed OS ancestry order. Parent links require a
/// known parent PID and start time preceding the child's start time, avoiding
/// false links when an OS PID has been reused. Missing data leaves a root row.
fn display_order(processes: &[TuiProcess]) -> Vec<usize> {
    let mut parents = vec![None; processes.len()];
    for (child_index, child) in processes.iter().enumerate() {
        let (Some(parent_pid), Some(child_start)) =
            (child.parent_process_id, child.start_time_ticks)
        else {
            continue;
        };
        parents[child_index] = processes
            .iter()
            .enumerate()
            .filter(|(parent_index, parent)| {
                *parent_index != child_index
                    && parent.pid == parent_pid
                    && parent.start_time_ticks.is_some_and(|start| start < child_start)
            })
            .max_by_key(|(_, parent)| parent.start_time_ticks)
            .map(|(parent_index, _)| parent_index);
    }

    fn visit(index: usize, children: &[Vec<usize>], visited: &mut [bool], order: &mut Vec<usize>) {
        if std::mem::replace(&mut visited[index], true) {
            return;
        }
        order.push(index);
        for &child in &children[index] {
            visit(child, children, visited, order);
        }
    }

    let mut children = vec![Vec::new(); processes.len()];
    for (child, parent) in parents.iter().enumerate() {
        if let Some(parent) = parent {
            children[*parent].push(child);
        }
    }
    for list in &mut children {
        list.sort_by_key(|&index| (processes[index].start_time_ticks, processes[index].pid));
    }
    let mut roots = (0..processes.len())
        .filter(|&index| parents[index].is_none())
        .collect::<Vec<_>>();
    roots.sort_by_key(|&index| (processes[index].start_time_ticks, processes[index].pid));
    let mut order = Vec::with_capacity(processes.len());
    let mut visited = vec![false; processes.len()];
    for root in roots {
        visit(root, &children, &mut visited, &mut order);
    }
    // Defensive fallback for malformed cyclic ancestry data.
    for index in 0..processes.len() {
        visit(index, &children, &mut visited, &mut order);
    }
    order
}

fn process_depth(index: usize, processes: &[TuiProcess]) -> usize {
    let process = &processes[index];
    let (Some(parent_pid), Some(child_start)) =
        (process.parent_process_id, process.start_time_ticks)
    else {
        return 0;
    };
    processes
        .iter()
        .filter(|parent| {
            parent.pid == parent_pid
                && parent.start_time_ticks.is_some_and(|start| start < child_start)
        })
        .max_by_key(|parent| parent.start_time_ticks)
        .map(|parent| {
            let parent_index = processes.iter().position(|candidate| std::ptr::eq(candidate, parent)).unwrap_or(index);
            1 + process_depth(parent_index, processes)
        })
        .unwrap_or(0)
}

const ACCENT: Color = Color::Cyan;
const MUTED: Color = Color::DarkGray;

fn state_span(text: &str) -> Span<'static> {
    let color = match text {
        "running" | "healthy" | "succeeded" | "done" => Color::Green,
        "failed" | "unhealthy" | "lost" => Color::Red,
        "cancelled" | "exited" | "unknown" => MUTED,
        "blocked" | "waitingForApproval" | "waitingForInput" | "stale" => Color::Yellow,
        _ => Color::Reset,
    };
    Span::styled(sanitize(text, 32), Style::default().fg(color))
}

fn field<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(serde_json::Value::as_str)
}

fn dim(text: String) -> Span<'static> {
    Span::styled(text, Style::default().fg(MUTED))
}

/// Draws the whole screen for `view`.
fn render(frame: &mut Frame, view: &View) {
    let process_rows = view.processes.len().clamp(1, 12) as u16;
    let [header, processes, work, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(process_rows + 3),
        Constraint::Min(4),
        Constraint::Length(2),
    ])
    .areas(frame.area());

    let alive = view.processes.iter().filter(|p| p.alive).count();
    frame.render_widget(
        Line::from(vec![
            " Agent Supervisor ".bold().fg(Color::Black).bg(ACCENT),
            dim(format!(
                "  {} run(s) · {} active process(es) · {} FlowRun(s) · refreshing every second",
                view.runs.len(),
                alive,
                view.flow_runs.len()
            )),
        ]),
        header,
    );

    render_processes(frame, view, processes);
    render_work(frame, view, work);
    render_footer(frame, view, footer);

    if view.detail {
        if let Some(process) = view.selected_process() {
            render_detail(frame, process);
        }
    }
    if let Some(confirmation) = &view.confirmation {
        render_confirmation(frame, view, confirmation);
    }
    if view.help {
        render_help(frame, view.can_control);
    }
}

fn panel(title: &str) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(MUTED))
        .title(Line::from(format!(" {title} ")).bold().fg(ACCENT))
}

fn render_processes(frame: &mut Frame, view: &View, area: Rect) {
    let coordinator = view.coordinator_process.as_ref();
    let root = coordinator
        .map(|p| {
            format!(
                "Coordinator PID {} · {}",
                p.pid,
                sanitize(
                    p.executable
                        .as_deref()
                        .or(p.identity_limitation.as_deref())
                        .unwrap_or("executable unavailable"),
                    42
                )
            )
        })
        .unwrap_or_else(|| "service identity unavailable: protocol did not provide it".into());
    let title = format!("managed OS processes ({}) · {root}", view.processes.len());
    if view.processes.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(dim(
                "  No processes are currently managed.".into()
            )))
            .block(panel(&title)),
            area,
        );
        return;
    }
    let rows = display_order(&view.processes)
        .into_iter()
        .enumerate()
        .map(|(position, index)| {
            let process = &view.processes[index];
            let command = process
                .run_label
                .as_deref()
                .or(process.executable.as_deref())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("PID {}", process.pid));
            let role = if process.worker_id.is_some() || process.task_id.is_some() {
                "worker"
            } else {
                "managed"
            };
            let tree_label = format!("{}{} · {}", "  ".repeat(process_depth(index, &view.processes)), role, command);
            Row::new(vec![
                Cell::from(format!("{:>2}", position + 1)),
                Cell::from(state_span(if process.alive { "running" } else { "exited" })),
                Cell::from(format!("{:>7}", process.pid)),
                Cell::from(sanitize(process.framework.as_deref().unwrap_or("-"), 24)),
                Cell::from(sanitize(process.task_id.as_deref().unwrap_or("-"), 48)),
                Cell::from(sanitize(&tree_label, 80)),
            ])
        });
    let table = Table::new(
        rows,
        [
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Fill(1),
            Constraint::Fill(2),
        ],
    )
    .header(
        Row::new(["#", "STATE", "PID", "FRAMEWORK", "TASK", "PROCESS"])
            .style(Style::default().fg(MUTED).add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
    .highlight_symbol("› ")
    .block(panel(&title));
    let mut state = TableState::default().with_selected(Some(view.selected));
    frame.render_stateful_widget(table, area, &mut state);
}

/// The runs, their activities and attempts, and the Supervisor FlowRuns.
fn work_lines(view: &View) -> Vec<Line<'static>> {
    let heading = |text: &str| Line::from(text.to_owned()).bold().fg(ACCENT);
    let mut lines = vec![
        heading("Coordinated work"),
        Line::from(dim("  FlowRun/task/attempt IDs are context. Tool calls are activities, not OS processes.".into())),
    ];
    if let Some(limitation) = view.coordinator_process.as_ref().and_then(|process| process.identity_limitation.as_deref()) {
        lines.push(Line::from(dim(format!("  Coordinator identity limitation: {}", sanitize(limitation, 120)))));
    } else if view.coordinator_process.is_none() {
        lines.push(Line::from(dim("  Coordinator identity limitation: this protocol connection did not provide the service process identity.".into())));
    }
    if view.tool_requests.is_empty() {
        lines.push(Line::from(dim("  No tool requests are recorded.".into())));
    }
    if view.runs.is_empty() {
        lines.push(Line::from(
            dim("  No runs are currently registered.".into()),
        ));
    }
    for run in &view.runs {
        let linked_framework = view
            .activities
            .iter()
            .filter(|activity| activity.run_id == run.run_id)
            .flat_map(|activity| {
                view.attempts
                    .iter()
                    .filter(move |attempt| attempt.activity_id == activity.activity_id)
            })
            .flat_map(|attempt| attempt.process_ids.iter())
            .find_map(|process_id| {
                view.processes
                    .iter()
                    .find(|process| &process.process_id == process_id)
            })
            .and_then(|process| process.framework.clone());
        let framework = field(&run.context, "framework")
            .map(str::to_owned)
            .or(linked_framework)
            .unwrap_or_else(|| "unknown".to_owned());
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled(sanitize(&framework, 48), Style::default().bold()),
            Span::raw(" · "),
            state_span(&run.status),
            dim(format!(
                " · project {} · FlowRun {} · task {}",
                sanitize(field(&run.context, "projectId").unwrap_or("-"), 64),
                sanitize(field(&run.context, "flowRunId").unwrap_or("-"), 80),
                sanitize(field(&run.context, "taskId").unwrap_or("-"), 80)
            )),
        ]));
        lines.push(Line::from(dim(format!(
            "    run {}",
            sanitize(&run.run_id, 96)
        ))));
        for activity in view.activities.iter().filter(|a| a.run_id == run.run_id) {
            lines.push(Line::from(vec![
                Span::raw(format!("    logical activity {} · ", sanitize(&activity.kind, 48))),
                state_span(&activity.status),
                Span::raw(" · health "),
                state_span(&activity.health),
                dim(format!(
                    " · waiting {} · last contact {}",
                    sanitize(activity.waiting_reason.as_deref().unwrap_or("-"), 80),
                    activity
                        .last_contact_at_ms
                        .map(|at| at.to_string())
                        .unwrap_or_else(|| "unknown".into())
                )),
            ]));
            for attempt in view
                .attempts
                .iter()
                .filter(|attempt| attempt.activity_id == activity.activity_id)
            {
                let report_detail = attempt
                    .reports
                    .last()
                    .and_then(|report| report.get("detail"));
                let evidence = report_detail
                    .and_then(|detail| detail.get("evidence"))
                    .or_else(|| report_detail.and_then(|detail| detail.get("reasonCode")))
                    .map(|value| display_value(value, 96))
                    .unwrap_or_else(|| "-".into());
                let cancel = report_detail
                    .and_then(|detail| detail.get("cancelRequested"))
                    .and_then(serde_json::Value::as_bool)
                    .map(|requested| {
                        if requested {
                            "requested; awaiting outcome"
                        } else {
                            "none"
                        }
                    })
                    .unwrap_or(&attempt.cancellation);
                lines.push(Line::from(vec![
                    Span::raw(format!("      attempt {} · ", attempt.generation)),
                    state_span(&attempt.status),
                    dim(format!(
                        " · cancel {} · processes {} · evidence {}",
                        sanitize(cancel, 32),
                        attempt.process_ids.len(),
                        sanitize(&evidence, 96)
                    )),
                ]));
                for tool in view.tool_requests.iter().filter(|tool| tool.attempt_id == attempt.attempt_id && tool.generation == attempt.generation) {
                    lines.push(Line::from(format!("        tool {} · {} · request {} · FlowRun {} task {}", sanitize(&tool.tool_name, 36), sanitize(&tool.status, 20), sanitize(&tool.request_id, 80), sanitize(&tool.flow_id, 48), sanitize(&tool.task_id, 48))));
                }
                // Processes the Supervisor still reports are listed; those
                // whose details are gone (they exited) become one count line.
                let mut gone = 0usize;
                for process_id in &attempt.process_ids {
                    match view.processes.iter().find(|process| &process.process_id == process_id) {
                        Some(process) => lines.push(Line::from(dim(format!(
                            "        process {} · {}",
                            sanitize(process_id, 96),
                            sanitize(process.framework.as_deref().unwrap_or("framework unknown"), 40)
                        )))),
                        None => gone += 1,
                    }
                }
                if gone > 0 {
                    lines.push(Line::from(dim(format!(
                        "        {gone} process{} exited · details no longer available",
                        if gone == 1 { "" } else { "es" }
                    ))));
                }
            }
        }
    }

    lines.push(Line::default());
    lines.push(heading("Supervisor FlowRuns"));
    if view.flow_runs.is_empty() {
        lines.push(Line::from(dim(
            "  No FlowRuns are currently registered.".into()
        )));
    }
    for tool in view.tool_requests.iter().filter(|tool| !view.attempts.iter().any(|attempt| attempt.attempt_id == tool.attempt_id && attempt.generation == tool.generation && view.activities.iter().any(|activity| activity.activity_id == attempt.activity_id && view.runs.iter().any(|run| run.run_id == activity.run_id)))) {
        lines.push(Line::from(format!("  tool activity · {} · request {} · attempt {} gen {} (attempt record unavailable) · FlowRun {} task {}", sanitize(&tool.tool_name, 36), sanitize(&tool.request_id, 80), sanitize(&tool.attempt_id, 64), tool.generation, sanitize(&tool.flow_id, 48), sanitize(&tool.task_id, 48))));
    }
    for flow in &view.flow_runs {
        let pending_sync = flow
            .get("pendingSync")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        lines.push(Line::from(vec![
            Span::styled(
                format!(
                    "  FlowRun {}",
                    sanitize(field(flow, "flowId").unwrap_or("unknown"), 96)
                ),
                Style::default().bold(),
            ),
            dim(format!(
                " · board {} · revision {} · ",
                sanitize(field(flow, "boardId").unwrap_or("unknown"), 96),
                flow.get("flowRevision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default()
            )),
            if pending_sync {
                Span::styled("Flowboard sync pending", Style::default().fg(Color::Yellow))
            } else {
                Span::styled("Flowboard synchronized", Style::default().fg(Color::Green))
            },
        ]));
        if let Some(tasks) = flow.get("tasks").and_then(serde_json::Value::as_object) {
            for (task_id, task) in tasks {
                let state = field(task, "state").unwrap_or("unknown");
                lines.push(Line::from(vec![
                    Span::raw(format!(
                        "    {} · ",
                        sanitize(field(task, "title").unwrap_or(task_id), 72)
                    )),
                    state_span(state),
                    dim(format!(
                        " · waiting {}",
                        sanitize(field(task, "waitingReason").unwrap_or("-"), 72)
                    )),
                ]));
                lines.push(Line::from(dim(format!(
                    "      task {} · post {}",
                    sanitize(task_id, 96),
                    sanitize(field(task, "postId").unwrap_or("unknown"), 96)
                ))));
                if let Some(attempt_id) = field(task, "attemptId") {
                    lines.push(Line::from(dim(format!(
                        "      attempt {} · generation {}",
                        sanitize(attempt_id, 96),
                        task.get("generation")
                            .and_then(serde_json::Value::as_u64)
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "unknown".into())
                    ))));
                }
                if let Some(process_id) = field(task, "processId") {
                    lines.push(Line::from(dim(format!(
                        "      process {}",
                        sanitize(process_id, 96)
                    ))));
                }
                if let Some(runtime_run_id) = field(task, "runtimeRunId") {
                    lines.push(Line::from(dim(format!(
                        "      runtime {}",
                        sanitize(runtime_run_id, 96)
                    ))));
                }
                if task
                    .get("cancelRequested")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                {
                    lines.push(Line::from(Span::styled(
                        "      cancellation requested · awaiting worker outcome",
                        Style::default().fg(Color::Yellow),
                    )));
                } else if state == "cancelled" {
                    lines.push(Line::from(dim(
                        "      cancellation confirmed by worker outcome".into(),
                    )));
                }
                if let Some(outcome) = task.get("lastOutcome") {
                    let reason = field(outcome, "reason").unwrap_or("");
                    lines.push(Line::from(vec![
                        Span::raw("      outcome "),
                        state_span(field(outcome, "status").unwrap_or("unknown")),
                        dim(if reason.is_empty() {
                            String::new()
                        } else {
                            format!(" · {}", sanitize(reason, 72))
                        }),
                    ]));
                    if let Some(evidence) = outcome.get("evidence") {
                        lines.push(Line::from(dim(format!(
                            "      evidence {}",
                            display_value(evidence, 120)
                        ))));
                    }
                }
            }
        }
        if let Some(error) = field(flow, "syncError") {
            lines.push(Line::from(Span::styled(
                format!("    Flowboard sync error: {}", sanitize(error, 100)),
                Style::default().fg(Color::Red),
            )));
        }
    }
    lines
}

fn render_work(frame: &mut Frame, view: &View, area: Rect) {
    let lines = work_lines(view);
    let visible = area.height.saturating_sub(2);
    let max_scroll = (lines.len() as u16).saturating_sub(visible);
    let scroll = view.scroll.min(max_scroll);
    let title = if max_scroll > 0 {
        format!("Runs and FlowRuns · PgUp/PgDn to scroll ({scroll}/{max_scroll})")
    } else {
        "Runs and FlowRuns".to_owned()
    };
    frame.render_widget(
        Paragraph::new(lines)
            .scroll((scroll, 0))
            .block(panel(&title)),
        area,
    );
}

fn render_footer(frame: &mut Frame, view: &View, area: Rect) {
    let [notice, keys] =
        Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(area);
    frame.render_widget(
        Line::from(format!(" {}", sanitize(&view.notice, 200))),
        notice,
    );
    let mut hints = vec![("j/k", "move"), ("i", "details")];
    if view.can_control {
        hints.extend([("s", "stop"), ("a", "stop all")]);
    }
    hints.extend([("r", "refresh"), ("?", "help"), ("q", "quit")]);
    let mut spans = vec![Span::raw(" ")];
    for (key, label) in hints {
        spans.push(Span::styled(
            format!(" {key} "),
            Style::default().fg(Color::Black).bg(MUTED),
        ));
        spans.push(Span::raw(format!(" {label}  ")));
    }
    frame.render_widget(Line::from(spans), keys);
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let [area] = Layout::horizontal([Constraint::Length(width.min(area.width))])
        .flex(Flex::Center)
        .areas(area);
    let [area] = Layout::vertical([Constraint::Length(height.min(area.height))])
        .flex(Flex::Center)
        .areas(area);
    area
}

fn render_detail(frame: &mut Frame, process: &TuiProcess) {
    let row = |label: &str, value: &str| {
        Line::from(vec![
            Span::styled(format!("{label:<15}"), Style::default().fg(MUTED)),
            Span::raw(sanitize(value, 160)),
        ])
    };
    let missing = "not provided";
    let lines = vec![
        row("supervisor ID", &process.process_id),
        row("OS PID", &process.pid.to_string()),
        row("OS parent PID", &process.parent_process_id.map(|pid| pid.to_string()).unwrap_or_else(|| "unknown".into())),
        row("start time", &process.start_time_ticks.map(|ticks| ticks.to_string()).unwrap_or_else(|| "unknown".into())),
        row("state", if process.alive { "running" } else { "exited" }),
        row(
            "executable",
            process.executable.as_deref().unwrap_or("unknown"),
        ),
        row("framework", process.framework.as_deref().unwrap_or(missing)),
        row("run label", process.run_label.as_deref().unwrap_or(missing)),
        row("project", process.project_id.as_deref().unwrap_or(missing)),
        row("FlowRun", process.flow_run_id.as_deref().unwrap_or(missing)),
        row("task", process.task_id.as_deref().unwrap_or(missing)),
        row("task post", process.post_id.as_deref().unwrap_or(missing)),
        row("worker", process.worker_id.as_deref().unwrap_or(missing)),
        row(
            "runtime run",
            process.runtime_run_id.as_deref().unwrap_or(missing),
        ),
    ];
    let area = centered(frame.area(), 100, lines.len() as u16 + 2);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(panel("Selected process · Esc to close")),
        area,
    );
}

fn render_confirmation(frame: &mut Frame, view: &View, confirmation: &Confirmation) {
    let question = match confirmation {
        Confirmation::Stop(process_id) => {
            let position = display_order(&view.processes)
                .iter()
                .position(|&index| &view.processes[index].process_id == process_id)
                .map(|position| format!("#{}", position + 1))
                .unwrap_or_else(|| sanitize(process_id, 40));
            format!("Stop process {position}?")
        }
        Confirmation::StopAll => "Stop all managed processes?".to_owned(),
    };
    let area = centered(frame.area(), 52, 4);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::from(format!(" {question}")).bold(),
            Line::from(vec![
                Span::raw(" "),
                Span::styled(" y ", Style::default().fg(Color::Black).bg(Color::Red)),
                Span::raw(" confirm    any other key cancels"),
            ]),
        ]))
        .block(panel("Confirm")),
        area,
    );
}

fn render_help(frame: &mut Frame, can_control: bool) {
    let mut lines = vec![
        Line::from(" j / ↓      select the next process"),
        Line::from(" k / ↑      select the previous process"),
        Line::from(" i / Enter  show or hide process details"),
        Line::from(" PgDn/PgUp  scroll runs and FlowRuns"),
        Line::from(" r          refresh the process list"),
    ];
    if can_control {
        lines.push(Line::from(
            " s          stop the selected process (asks first)",
        ));
        lines.push(Line::from(
            " a          stop all managed processes (asks first)",
        ));
    } else {
        lines.push(Line::from(dim(
            " This connection is view-only: it cannot stop processes.".into(),
        )));
    }
    lines.push(Line::from(" q / Esc    quit"));
    let area = centered(frame.area(), 62, lines.len() as u16 + 2);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(panel("Keys · any key to close")),
        area,
    );
}

/// Draws `view` into an in-memory screen of `size` columns by rows.
fn render_to_buffer(view: &View, size: (u16, u16)) -> Buffer {
    let mut terminal = Terminal::new(TestBackend::new(size.0, size.1))
        .expect("an in-memory terminal always starts");
    terminal
        .draw(|frame| render(frame, view))
        .expect("drawing into memory cannot fail");
    terminal.backend().buffer().clone()
}

/// The screen as plain text, one line per row, trailing spaces removed.
fn buffer_to_text(buffer: &Buffer) -> String {
    let area = buffer.area;
    let mut text = String::new();
    for y in area.top()..area.bottom() {
        let mut line = String::new();
        for x in area.left()..area.right() {
            line.push_str(buffer[(x, y)].symbol());
        }
        text.push_str(line.trim_end());
        text.push('\n');
    }
    text
}

fn svg_color(color: Color, default: &str) -> String {
    match color {
        Color::Reset => default.to_owned(),
        Color::Black => "#1e1e2e".into(),
        Color::Red => "#f38ba8".into(),
        Color::Green => "#a6e3a1".into(),
        Color::Yellow => "#f9e2af".into(),
        Color::Blue => "#89b4fa".into(),
        Color::Magenta => "#cba6f7".into(),
        Color::Cyan => "#89dceb".into(),
        Color::Gray => "#bac2de".into(),
        Color::DarkGray => "#7f849c".into(),
        Color::LightRed => "#f5a3b5".into(),
        Color::LightGreen => "#b8ebb3".into(),
        Color::LightYellow => "#fbe8c0".into(),
        Color::LightBlue => "#a5c5fb".into(),
        Color::LightMagenta => "#d9bdf9".into(),
        Color::LightCyan => "#a8e6f0".into(),
        Color::White => "#cdd6f4".into(),
        Color::Rgb(r, g, b) => format!("#{r:02x}{g:02x}{b:02x}"),
        Color::Indexed(_) => default.to_owned(),
    }
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The screen as an SVG image for documentation. Each run of cells with one
/// style gets its background, and each word is placed at its own column, so
/// the image does not depend on how a viewer treats spaces.
fn buffer_to_svg(buffer: &Buffer) -> String {
    const CELL_W: f32 = 8.4;
    const CELL_H: f32 = 18.0;
    const PAD: f32 = 12.0;
    const BACKGROUND: &str = "#1e1e2e";
    const FOREGROUND: &str = "#cdd6f4";
    let area = buffer.area;
    let width = area.width as f32 * CELL_W + PAD * 2.0;
    let height = area.height as f32 * CELL_H + PAD * 2.0;
    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width:.0}\" height=\"{height:.0}\" viewBox=\"0 0 {width:.0} {height:.0}\">\n\
<rect width=\"100%\" height=\"100%\" rx=\"8\" fill=\"{BACKGROUND}\"/>\n\
<g font-family=\"ui-monospace, SFMono-Regular, Menlo, Consolas, monospace\" font-size=\"14\">\n"
    );
    let column = |x: u16| PAD + (x - area.left()) as f32 * CELL_W;
    for y in area.top()..area.bottom() {
        let top = PAD + (y - area.top()) as f32 * CELL_H;
        let baseline = top + CELL_H * 0.75;
        let mut x = area.left();
        while x < area.right() {
            let first = &buffer[(x, y)];
            let style = (first.fg, first.bg, first.modifier);
            let start = x;
            while x < area.right() {
                let cell = &buffer[(x, y)];
                if (cell.fg, cell.bg, cell.modifier) != style {
                    break;
                }
                x += 1;
            }
            let (mut fg, mut bg) = (
                svg_color(style.0, FOREGROUND),
                svg_color(style.1, BACKGROUND),
            );
            if style.2.contains(Modifier::REVERSED) {
                std::mem::swap(&mut fg, &mut bg);
            }
            if bg != BACKGROUND {
                svg.push_str(&format!(
                    "<rect x=\"{:.1}\" y=\"{top:.1}\" width=\"{:.1}\" height=\"{CELL_H:.1}\" fill=\"{bg}\"/>\n",
                    column(start),
                    (x - start) as f32 * CELL_W
                ));
            }
            let weight = if style.2.contains(Modifier::BOLD) {
                " font-weight=\"bold\""
            } else {
                ""
            };
            // One text element per word, at the word's own column.
            let mut word_start = start;
            while word_start < x {
                if buffer[(word_start, y)].symbol().trim().is_empty() {
                    word_start += 1;
                    continue;
                }
                let mut word_end = word_start;
                let mut word = String::new();
                while word_end < x && !buffer[(word_end, y)].symbol().trim().is_empty() {
                    word.push_str(buffer[(word_end, y)].symbol());
                    word_end += 1;
                }
                svg.push_str(&format!(
                    "<text x=\"{:.1}\" y=\"{baseline:.1}\" fill=\"{fg}\"{weight} textLength=\"{:.1}\" lengthAdjust=\"spacingAndGlyphs\">{}</text>\n",
                    column(word_start),
                    (word_end - word_start) as f32 * CELL_W,
                    xml_escape(&word)
                ));
                word_start = word_end;
            }
        }
    }
    svg.push_str("</g>\n</svg>\n");
    svg
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

fn update_tool_activity(activities: &mut Vec<TuiToolActivity>, activity: TuiToolActivity) {
    if let Some(existing) = activities.iter_mut().find(|existing| existing.attempt_id == activity.attempt_id && existing.generation == activity.generation && existing.request_id == activity.request_id) {
        let next = TuiToolActivity { flow_id: if activity.flow_id.is_empty() { existing.flow_id.clone() } else { activity.flow_id }, task_id: if activity.task_id.is_empty() { existing.task_id.clone() } else { activity.task_id }, tool_name: if activity.tool_name.is_empty() { existing.tool_name.clone() } else { activity.tool_name }, ..activity };
        *existing = next;
    } else { activities.push(activity); }
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
        TuiResponse::ToolActivityChanged { .. } => Some(false),
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

fn sanitize(text: &str, max_chars: usize) -> String {
    text.chars()
        .filter(|ch| !ch.is_control())
        .take(max_chars)
        .collect()
}

fn display_value(value: &serde_json::Value, max_chars: usize) -> String {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "<unavailable>".into());
    sanitize(&text, max_chars)
}

#[cfg(unix)]
fn open_terminal_output() -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new().write(true).open("/dev/tty")
}

#[cfg(windows)]
fn open_terminal_output() -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new().write(true).open("CONOUT$")
}

#[cfg(not(any(unix, windows)))]
fn open_terminal_output() -> io::Result<std::fs::File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this platform does not provide a supported controlling-terminal device",
    ))
}

#[cfg(test)]
mod tests;
