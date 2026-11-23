use super::{
    apply_event, buffer_to_svg, buffer_to_text, display_order, handle_key, process_depth,
    read_responses, render_to_buffer, sanitize, view_from_file, Confirmation, KeyOutcome, Options, SnapshotFormat, View,
    DEFAULT_SNAPSHOT_SIZE,
};
use agent_supervisor::tui::{
    read_frame, write_frame, TuiActivity, TuiAttempt, TuiCoordinatorProcess, TuiProcess, TuiRequest, TuiResponse, TuiRun, TuiToolActivity,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde_json::json;
use tokio::io::duplex;

const WIDE: (u16, u16) = (170, 60);

fn screen(view: &View, size: (u16, u16)) -> String {
    buffer_to_text(&render_to_buffer(view, size))
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn process(id: &str, pid: u32, task: &str) -> TuiProcess {
    TuiProcess {
        process_id: id.into(),
        pid,
        alive: true,
        parent_process_id: None,
        start_time_ticks: Some(pid as u64),
        executable: Some("worker".into()),
        framework: Some("claude".into()),
        run_label: Some(format!("worker for {task}")),
        project_id: Some("project-1".into()),
        flow_run_id: Some("flow-1".into()),
        task_id: Some(task.into()),
        post_id: Some(format!("post-{task}")),
        worker_id: Some(format!("worker-{task}")),
        runtime_run_id: None,
    }
}

#[test]
fn process_tree_uses_parent_pid_and_non_reused_start_time() {
    let mut child = process("child", 20, "task");
    child.parent_process_id = Some(10);
    child.start_time_ticks = Some(200);
    let mut reused_parent = process("new-parent", 10, "new");
    reused_parent.start_time_ticks = Some(300);
    let mut parent = process("parent", 10, "parent");
    parent.start_time_ticks = Some(100);
    let entries = vec![child, reused_parent, parent];

    // The later process with the reused PID cannot own the earlier child.
    assert_eq!(display_order(&entries), vec![2, 0, 1]);
    assert_eq!(process_depth(0, &entries), 1);
    assert_eq!(process_depth(2, &entries), 0);
}

#[test]
fn process_rows_use_pid_fallback_after_preferred_labels_and_keep_ancestry() {
    let mut parent = process("parent", 41, "task-parent");
    parent.run_label = None;
    parent.executable = None;
    parent.task_id = None;
    parent.worker_id = None;
    parent.start_time_ticks = Some(100);

    let mut child = process("child", 42, "task-child");
    child.parent_process_id = Some(41);
    child.start_time_ticks = Some(200);
    child.run_label = None;
    child.executable = None;
    child.task_id = None;
    child.worker_id = None;

    let mut run_labeled = process("run-labeled", 43, "task-run");
    run_labeled.run_label = Some("preferred run label".into());
    run_labeled.executable = Some("/bin/worker".into());

    let mut executable_labeled = process("executable-labeled", 44, "task-exec");
    executable_labeled.run_label = None;
    executable_labeled.executable = Some("/bin/executable-only".into());

    let view = View {
        processes: vec![child, parent, run_labeled, executable_labeled],
        ..View::default()
    };
    let text = screen(&view, WIDE);

    assert!(text.contains("managed · PID 41"), "{text}");
    let parent_row = text.lines().find(|line| line.contains("managed · PID 41")).unwrap();
    let child_row = text.lines().find(|line| line.contains("managed · PID 42")).unwrap();
    assert!(
        child_row.find("managed · PID 42").unwrap() > parent_row.find("managed · PID 41").unwrap(),
        "{text}"
    );
    assert!(text.contains("preferred run label"), "{text}");
    assert!(text.contains("/bin/executable-only"), "{text}");
    assert!(!text.contains("unknown"), "{text}");
}

#[test]
fn tool_requests_render_as_attempt_activity_and_do_not_create_process_rows() {
    let view = View {
        runs: vec![TuiRun { run_id: "run-1".into(), manager_id: "manager-1".into(), status: "running".into(), context: json!({"flowRunId":"flow-1","taskId":"task-4"}), revision: 1, updated_at_ms: 10 }],
        activities: vec![TuiActivity { activity_id: "activity-1".into(), run_id: "run-1".into(), kind: "worker".into(), status: "running".into(), health: "healthy".into(), last_contact_at_ms: None, waiting_reason: None, revision: 1, updated_at_ms: 10 }],
        coordinator_process: Some(TuiCoordinatorProcess {
            pid: 314,
            executable: Some("/opt/agent-supervisor-service".into()),
            identity_limitation: None,
        }),
        attempts: vec![TuiAttempt {
            attempt_id: "attempt-2".into(), activity_id: "activity-1".into(), run_id: "run-1".into(),
            generation: 3, status: "running".into(), cancellation: "none".into(), process_ids: vec![],
            reports: vec![], revision: 1, updated_at_ms: 10,
        }],
        tool_requests: vec![TuiToolActivity {
            request_id: "request-7".into(), attempt_id: "attempt-2".into(), generation: 3,
            flow_id: "flow-1".into(), task_id: "task-4".into(), tool_name: "exec".into(),
            status: "pending".into(), supervisor_process_id: Some("stale-or-unobserved".into()), updated_at_ms: 10,
        }],
        ..View::default()
    };
    let text = screen(&view, WIDE);
    assert!(text.contains("Coordinator PID 314"), "{text}");
    assert!(text.contains("attempt 3 · running"), "{text}");
    assert!(text.contains("tool exec · pending · request request-7"), "{text}");
    assert!(text.contains("managed OS processes (0)"), "{text}");
    assert!(!text.contains("stale-or-unobserved"), "{text}");
}

#[test]
fn sanitize_removes_terminal_control_characters_and_bounds_text() {
    assert_eq!(sanitize("ok\x1b[31mBAD\n", 8), "ok[31mBA");
    assert_eq!(sanitize("abcdef", 3), "abc");
}

#[test]
fn view_only_tui_does_not_offer_or_accept_stop_controls() {
    let mut view = View {
        notice: "Connected".into(),
        processes: vec![process("v1:m:1", 10, "a")],
        ..View::default()
    };
    let text = screen(&view, WIDE);
    assert!(text.contains("j/k  move"), "{text}");
    assert!(!text.contains(" s  stop"), "{text}");
    assert!(!text.contains("stop all"), "{text}");
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('s')), false),
        KeyOutcome::None
    );
    assert_eq!(view.confirmation, None);
    assert_eq!(view.notice, "This TUI connection has view-only permission.");
}

#[test]
fn stopping_asks_first_and_names_the_process_by_id_not_by_row() {
    let mut view = View {
        can_control: true,
        // Out of display order on purpose: rows are sorted by task.
        processes: vec![process("v1:m:2", 20, "b"), process("v1:m:1", 10, "a")],
        ..View::default()
    };
    // Row 1 is task a (process v1:m:1); move to row 2, task b.
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Down), false),
        KeyOutcome::None
    );
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('s')), false),
        KeyOutcome::None
    );
    assert_eq!(view.confirmation, Some(Confirmation::Stop("v1:m:2".into())));
    assert!(screen(&view, WIDE).contains("Stop process #2?"));

    // Anything but y cancels.
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('n')), false),
        KeyOutcome::None
    );
    assert_eq!(view.notice, "Cancelled.");
    assert_eq!(view.confirmation, None);

    handle_key(&mut view, key(KeyCode::Char('s')), false);
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('y')), false),
        KeyOutcome::Stop("v1:m:2".into())
    );

    // A process that exits while the question is open is not stopped.
    handle_key(&mut view, key(KeyCode::Char('s')), false);
    view.processes
        .retain(|process| process.process_id != "v1:m:2");
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('y')), false),
        KeyOutcome::None
    );
    assert_eq!(view.notice, "That process has already exited.");

    handle_key(&mut view, key(KeyCode::Char('a')), false);
    assert!(screen(&view, WIDE).contains("Stop all managed processes?"));
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('y')), true),
        KeyOutcome::None
    );
    assert_eq!(view.notice, "A supervisor request is already in progress.");
}

#[test]
fn keys_move_show_details_open_help_and_quit() {
    let mut view = View {
        can_control: true,
        processes: vec![process("v1:m:1", 10, "a"), process("v1:m:2", 20, "b")],
        ..View::default()
    };
    handle_key(&mut view, key(KeyCode::Char('j')), false);
    handle_key(&mut view, key(KeyCode::Char('j')), false);
    assert_eq!(view.selected, 1, "the selection stops at the last row");
    handle_key(&mut view, key(KeyCode::Enter), false);
    let text = screen(&view, WIDE);
    assert!(text.contains("Selected process"), "{text}");
    assert!(text.contains("supervisor ID  v1:m:2"), "{text}");
    handle_key(&mut view, key(KeyCode::Esc), false);
    assert!(!view.detail, "Esc closes the details before it quits");

    handle_key(&mut view, key(KeyCode::Char('?')), false);
    assert!(screen(&view, WIDE).contains("stop all managed processes (asks first)"));
    handle_key(&mut view, key(KeyCode::Char('x')), false);
    assert!(!view.help, "any key closes help");

    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('r')), false),
        KeyOutcome::Refresh
    );
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('r')), true),
        KeyOutcome::None
    );
    assert_eq!(
        handle_key(&mut view, key(KeyCode::Char('q')), false),
        KeyOutcome::Quit
    );
    assert_eq!(
        handle_key(
            &mut view,
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            false
        ),
        KeyOutcome::Quit
    );
}

fn flowrun_view() -> View {
    let run = TuiRun {
        run_id: "run-1".into(),
        manager_id: "manager-1".into(),
        status: "running".into(),
        context: json!({"projectId":"project-1","flowRunId":"flow-1","taskId":"task-1"}),
        revision: 2,
        updated_at_ms: 10_000,
    };
    let activity = TuiActivity {
        activity_id: "activity-1".into(),
        run_id: "run-1".into(),
        kind: "flowrun_task".into(),
        status: "waitingForApproval".into(),
        health: "healthy".into(),
        last_contact_at_ms: Some(9_500),
        waiting_reason: Some("review needed".into()),
        revision: 3,
        updated_at_ms: 10_000,
    };
    let attempt = TuiAttempt {
        attempt_id: "attempt-1".into(),
        activity_id: "activity-1".into(),
        run_id: "run-1".into(),
        generation: 4,
        status: "running".into(),
        cancellation: "requested".into(),
        process_ids: vec!["v1:manager-1:7".into()],
        reports: vec![json!({"detail":{"evidence":{"summary":"checked"}}})],
        revision: 4,
        updated_at_ms: 10_000,
    };
    let process = TuiProcess {
        process_id: "v1:manager-1:7".into(),
        pid: 123,
        alive: true,
        parent_process_id: Some(100),
        start_time_ticks: Some(1234),
        executable: Some("worker".into()),
        framework: Some("LangGraph".into()),
        run_label: Some("task worker".into()),
        project_id: Some("project-1".into()),
        flow_run_id: Some("flow-1".into()),
        task_id: Some("task-1".into()),
        post_id: Some("post-1".into()),
        worker_id: Some("worker-1".into()),
        runtime_run_id: Some("runtime-1".into()),
    };
    let flow = json!({
        "flowId":"flow-1", "boardId":"board-1", "flowRevision":9,
        "pendingSync":true, "syncError":"temporary failure",
        "tasks":{"task-1":{
            "postId":"post-1", "title":"Inspect", "state":"running",
            "waitingReason":"worker approval", "attemptId":"attempt-1", "generation":4,
            "processId":"v1:manager-1:7", "runtimeRunId":"runtime-1",
            "cancelRequested":true,
            "lastOutcome":{"status":"unknown","reason":"awaiting \u{1b}[31mhost","evidence":{"step":"checkpoint\u{1b}[31m"}}
        }}
    });
    let mut view = View {
        notice: "Supervisor connection stale/disconnected".into(),
        ..View::default()
    };
    view.apply_snapshot(
        vec![process],
        vec![run],
        vec![activity],
        vec![attempt],
        vec![flow],
        None,
        vec![],
    );
    view
}

#[test]
fn flowrun_view_joins_task_attempt_process_evidence_and_waiting_state() {
    let text = screen(&flowrun_view(), WIDE);
    for expected in [
        "1 run(s) · 1 active process(es) · 1 FlowRun(s)",
        "LangGraph · running · project project-1 · FlowRun flow-1 · task task-1",
        "flowrun_task · waitingForApproval · health healthy · waiting review needed · last contact 9500",
        "attempt 4 · running · cancel requested",
        "process v1:manager-1:7 · LangGraph",
        "Flowboard sync pending",
        "task task-1 · post post-1",
        "attempt attempt-1 · generation 4",
        "runtime runtime-1",
        "cancellation requested · awaiting worker outcome",
        "outcome unknown · awaiting [31mhost",
        "evidence {\"step\":\"checkpoint\\u001b[31m\"}",
        "temporary failure",
        "Supervisor connection stale/disconnected",
    ] {
        assert!(text.contains(expected), "missing {expected:?} in\n{text}");
    }
    assert!(!text.contains("cancellation confirmed by worker outcome"));
    assert!(
        !text.contains('\u{1b}'),
        "no control characters from Supervisor data reach the screen"
    );
}

#[test]
fn flowrun_view_reports_confirmed_cancellation_from_terminal_worker_state() {
    let view = View {
        flow_runs: vec![json!({
            "flowId":"flow-1", "boardId":"board-1", "pendingSync":false,
            "tasks":{"task-1":{"postId":"post-1","title":"Inspect","state":"cancelled","cancelRequested":false}}
        })],
        ..View::default()
    };
    assert!(screen(&view, WIDE).contains("cancellation confirmed by worker outcome"));
}

#[test]
fn a_long_runs_panel_scrolls_and_says_so() {
    let mut view = flowrun_view();
    let text = screen(&view, (120, 20));
    assert!(text.contains("PgUp/PgDn to scroll (0/"), "{text}");
    handle_key(&mut view, key(KeyCode::PageDown), false);
    let scrolled = screen(&view, (120, 20));
    assert!(!scrolled.contains("PgUp/PgDn to scroll (0/"), "{scrolled}");
}

#[test]
fn snapshot_options_need_a_source_and_valid_size_and_format() {
    let args = |list: &[&str]| list.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
    let options = Options::parse(&args(&[
        "--snapshot",
        "--from",
        "state.json",
        "--size",
        "120x40",
        "--format",
        "svg",
    ]))
    .unwrap();
    let snapshot = options.snapshot.unwrap();
    assert_eq!(snapshot.size, (120, 40));
    assert_eq!(snapshot.format, SnapshotFormat::Svg);
    assert_eq!(
        Options::parse(&args(&[
            "--snapshot",
            "--service-socket",
            "s",
            "--token-file",
            "t"
        ]))
        .unwrap()
        .snapshot
        .unwrap()
        .size,
        DEFAULT_SNAPSHOT_SIZE
    );
    for bad in [
        &["--snapshot"][..],
        &["--from", "state.json"],
        &["--snapshot", "--from", "f", "--size", "10x5"],
        &["--snapshot", "--from", "f", "--format", "png"],
        &["--service-socket", "s"],
        &["--bogus"],
    ] {
        assert!(
            Options::parse(&args(bad)).is_err(),
            "{bad:?} should be refused"
        );
    }
    assert_eq!(
        Options::parse(&[]).unwrap(),
        Options {
            service: None,
            snapshot: None
        }
    );
}

#[test]
fn a_saved_state_renders_as_text_and_as_svg_without_a_terminal() {
    let directory = std::env::temp_dir().join(format!("tui-snapshot-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("state.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&json!({
            "processes": [serde_json::to_value(process("v1:m:1", 4242, "inspect & <report>")).unwrap()],
            "flowRuns": [{"flowId":"flow-1","boardId":"board-1","pendingSync":false,"tasks":{}}]
        }))
        .unwrap(),
    )
    .unwrap();
    let view = view_from_file(&path).unwrap();
    let buffer = render_to_buffer(&view, DEFAULT_SNAPSHOT_SIZE);
    let text = buffer_to_text(&buffer);
    assert!(text.contains("managed OS processes (1)"), "{text}");
    assert!(text.contains("4242"), "{text}");
    assert!(text.contains("FlowRun flow-1"), "{text}");
    assert_eq!(text.lines().count(), DEFAULT_SNAPSHOT_SIZE.1 as usize);

    let svg = buffer_to_svg(&buffer);
    assert!(svg.starts_with("<svg xmlns=\"http://www.w3.org/2000/svg\""));
    assert!(svg.trim_end().ends_with("</svg>"));
    // Words are placed one by one, each escaped for XML.
    assert!(
        svg.contains(">&amp;</text>") && svg.contains(">&lt;report&gt;</text>"),
        "text is escaped for XML"
    );
    assert!(!svg.contains("<report>"));

    std::fs::write(&path, br#"{"processes":[{"pid":"not a number"}]}"#).unwrap();
    let error = view_from_file(&path)
        .err()
        .expect("a malformed state is refused")
        .to_string();
    assert!(error.starts_with("processes:"), "{error}");
    std::fs::remove_dir_all(directory).unwrap();
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
            parent_process_id: Some(7),
            start_time_ticks: Some(900),
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
                runs: vec![],
                activities: vec![],
                attempts: vec![],
                flow_runs: vec![],
                coordinator_process: None,
                tool_requests: vec![],
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

#[test]
fn exited_processes_of_an_attempt_collapse_into_one_count_line() {
    let mut view = flowrun_view();
    view.attempts[0].process_ids.extend(["v1:manager-1:8".into(), "v1:manager-1:9".into()]);
    let text = screen(&view, WIDE);
    assert!(text.contains("process v1:manager-1:7 · LangGraph"), "{text}");
    assert!(text.contains("2 processes exited · details no longer available"), "{text}");
    assert!(!text.contains("v1:manager-1:8"), "exited processes are counted, not listed\n{text}");
}
