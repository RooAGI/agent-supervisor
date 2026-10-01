#![cfg(windows)]

use agent_supervisor::{
    EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, ProcessGroup, PtyDimensions,
    PtySession,
};
use std::collections::BTreeSet;
use std::path::PathBuf;

fn command(args: &[&str]) -> ExecutionRequest {
    ExecutionRequest {
        executable: PathBuf::from(r"C:\Windows\System32\cmd.exe"),
        args: std::iter::once("/C")
            .chain(args.iter().copied())
            .map(str::to_owned)
            .collect(),
        working_directory: None,
        policy: agent_supervisor::SandboxPolicy {
            environment: EnvironmentPolicy {
                inherit: ["SystemRoot", "ComSpec", "PATH", "PATHEXT", "TEMP", "TMP"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>(),
                ..EnvironmentPolicy::default()
            },
            enforcement: EnforcementRequirement::Required,
            ..agent_supervisor::SandboxPolicy::default()
        },
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
        agent_supervisor::TerminationReason::Exited { code: Some(0), .. }
    ));
}

#[test]
fn conpty_session_is_contained_and_reaped() {
    let session_request = command(&["echo hello & exit 0"]);
    let session = PtySession::start(&session_request, PtyDimensions::default()).unwrap();
    // ConPTY's blocking reader may remain open after the child exits. Test
    // lifecycle and containment here; PTY output is covered by the Unix PTY
    // tests, where the reader has EOF semantics.
    let receipt = session
        .wait_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(matches!(
        receipt.termination,
        agent_supervisor::TerminationReason::Exited { code: Some(0), .. }
    ));
}

#[tokio::test]
async fn job_object_applies_cpu_quota() {
    let mut request = command(&["exit", "0"]);
    request.policy.limits.cpu_quota_micros = Some(50_000);
    let group = ProcessGroup::new().unwrap();
    let child = group.start(&request).unwrap();
    let receipt = child.wait().await.unwrap();
    assert!(matches!(
        receipt.termination,
        agent_supervisor::TerminationReason::Exited { code: Some(0), .. }
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
