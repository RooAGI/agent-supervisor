//! End-to-end coverage for the stdio MCP process wrapper.

use std::io::{BufRead, Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const TEST_ENV: &str = "AGENT_SUPERVISOR_MCP_TEST_VALUE";
const FIXTURE: &str = env!("CARGO_BIN_EXE_mcp-stdio-fixture");

#[test]
fn stdio_is_relayed_without_protocol_changes() {
    let request = b"Content-Length: 19\r\n\r\n{\"jsonrpc\":\"2.0\"}\n";
    let output = run_fixture(&["--value=forwarded"], Some(request), None);
    assert!(
        output.status.success(),
        "fixture failed: {:?}",
        output.stderr
    );
    assert_eq!(
        output.stdout,
        request,
        "unexpected relayed output; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("arg=forwarded"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("env=<unset>"));
}

#[test]
fn arguments_are_forwarded_and_environment_is_cleared_by_default() {
    let output = run_fixture(&["--value=kept"], Some(b""), Some("host-secret"));
    assert!(
        output.status.success(),
        "fixture failed: {:?}",
        output.stderr
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("arg=kept"),
        "argument was not forwarded: {stderr}"
    );
    assert!(
        stderr.contains("env=<unset>"),
        "ambient env leaked: {stderr}"
    );
}

#[test]
fn explicitly_allowed_environment_is_released() {
    let output = run_fixture(
        &["--allow-env", "--value=allowed"],
        Some(b""),
        Some("explicit-value"),
    );
    assert!(
        output.status.success(),
        "fixture failed: {:?}",
        output.stderr
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("env=explicit-value"),
        "allowed env missing: {stderr}"
    );
}

#[test]
fn child_failure_is_reported_to_the_host() {
    let output = run_fixture(&["--fail"], Some(b""), None);
    assert!(!output.status.success(), "server error should propagate");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("fixture server failed"),
        "server stderr was not forwarded: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn client_disconnect_stops_a_persistent_server() {
    let started = Instant::now();
    let output = run_fixture(&["--wait-forever"], Some(b""), None);
    assert!(
        output.status.success(),
        "disconnect cleanup failed: {:?}",
        output.stderr
    );
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "persistent server outlived client disconnect: {:?}",
        started.elapsed()
    );
}

fn run_fixture(
    args: &[&str],
    input: Option<&[u8]>,
    env_value: Option<&str>,
) -> std::process::Output {
    let mut command = Command::new(FIXTURE);
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(value) = env_value {
        command.env(TEST_ENV, value);
    } else {
        command.env_remove(TEST_ENV);
    }

    let mut child = command.spawn().expect("start MCP fixture");
    if let Some(input) = input {
        child
            .stdin
            .as_mut()
            .expect("fixture stdin")
            .write_all(input)
            .expect("write fixture input");
    }

    let mut stderr = std::io::BufReader::new(child.stderr.take().unwrap());
    let mut ready = Vec::new();
    if args.contains(&"--wait-forever") {
        drop(child.stdin.take());
    } else if args.contains(&"--fail") {
        stderr.read_to_end(&mut ready).expect("read failure output");
        drop(child.stdin.take());
    } else {
        let mut line = String::new();
        stderr
            .read_line(&mut line)
            .expect("read startup diagnostic");
        ready.extend(line.into_bytes());
        drop(child.stdin.take());
    }
    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut stdout)
        .unwrap();
    stderr.read_to_end(&mut ready).unwrap();
    let status = child.wait().expect("wait for MCP fixture");
    std::process::Output {
        status,
        stdout,
        stderr: ready,
    }
}
