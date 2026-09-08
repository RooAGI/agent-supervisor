#![cfg(windows)]

use rooagi_sandbox::{
    EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, ProcessGroup, PtyDimensions,
    PtySession, ResourceLimits,
};
use std::path::PathBuf;

fn command(args: &[&str]) -> ExecutionRequest {
    ExecutionRequest {
        executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
        args: std::iter::once("/C")
            .chain(args.iter().copied())
            .map(str::to_owned)
            .collect(),
        environment: EnvironmentPolicy::default(),
        working_directory: None,
        filesystem: None,
        network: rooagi_sandbox::NetworkMode::Host,
        limits: ResourceLimits::default(),
        enforcement: EnforcementRequirement::Required,
    }
}

#[tokio::test]
async fn job_object_tracks_and_reaps_a_real_process() {
    let group = ProcessGroup::new().unwrap();
    let child = group.start(&command(&["exit", "0"])).unwrap();
    let members = group.members().unwrap();
    let process_id = child.id().expect("child must have a process id");
    let member = members
        .iter()
        .find(|member| member.process_id == process_id)
        .expect("Job Object must enumerate the child");
    assert!(member.start_time_ticks.is_some());
    assert!(member
        .executable
        .as_ref()
        .is_some_and(|path| path.is_absolute()));
    assert!(members.iter().all(|member| member.alive));
    let receipt = child.wait().await.unwrap();
    assert!(matches!(
        receipt.termination,
        rooagi_sandbox::TerminationReason::Exited { code: Some(0), .. }
    ));
}

#[test]
fn conpty_session_is_contained_and_reaped() {
    let session_request = command(&["echo", "hello"]);
    let mut session = PtySession::start(&session_request, PtyDimensions::default()).unwrap();
    let mut output = Vec::new();
    let mut buffer = [0_u8; 256];
    for _ in 0..32 {
        let read = session.read(&mut buffer).unwrap_or(0);
        if read == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..read]);
        if output
            .windows(b"hello".len())
            .any(|window| window.eq_ignore_ascii_case(b"hello"))
        {
            break;
        }
    }
    assert!(
        output
            .windows(b"hello".len())
            .any(|window| window.eq_ignore_ascii_case(b"hello")),
        "ConPTY output did not contain the command result: {:?}",
        output
    );
    let receipt = session.wait().unwrap();
    assert!(matches!(
        receipt.termination,
        rooagi_sandbox::TerminationReason::Exited { code: Some(0), .. }
    ));
}

#[tokio::test]
async fn job_object_applies_cpu_quota() {
    let mut request = command(&["exit", "0"]);
    request.limits.cpu_quota_micros = Some(50_000);
    let group = ProcessGroup::new().unwrap();
    let child = group.start(&request).unwrap();
    let receipt = child.wait().await.unwrap();
    assert!(matches!(
        receipt.termination,
        rooagi_sandbox::TerminationReason::Exited { code: Some(0), .. }
    ));
}

#[test]
fn job_object_adopts_an_external_process_with_identity_check() {
    let executable = PathBuf::from(r"C:\Windows\System32\cmd.exe");
    let mut external = std::process::Command::new(&executable)
        .args(["/C", "ping", "-n", "8", "127.0.0.1", ">", "nul"])
        .spawn()
        .unwrap();
    let process_id = external.id();
    let group = ProcessGroup::new().unwrap();
    let adopted = group.adopt(process_id, Some(&executable)).unwrap();
    assert_eq!(adopted.id(), process_id);
    assert!(adopted
        .members()
        .unwrap()
        .iter()
        .any(|member| member.process_id == process_id));

    adopted.terminate().unwrap();
    let status = external.wait().unwrap();
    assert!(!status.success());
}
