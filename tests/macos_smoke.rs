#![cfg(target_os = "macos")]

use agent_sandbox::{
    EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, ProcessGroup, ResourceLimits,
};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::time::Duration;

fn command() -> ExecutionRequest {
    ExecutionRequest {
        executable: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), "sleep 2".into()],
        working_directory: None,
        policy: agent_sandbox::SandboxPolicy::default(),
    }
}

#[tokio::test]
async fn process_group_reports_native_identity_and_stats() {
    let group = ProcessGroup::new().unwrap();
    let child = group.start(&command()).unwrap();
    let process_id = child.id().expect("child must have a process id");

    let member = group
        .members()
        .unwrap()
        .into_iter()
        .find(|member| member.process_id == process_id)
        .expect("macOS process-group enumeration must include the leader");
    assert!(member.alive);
    assert!(member.start_time_ticks.is_some());
    assert!(member
        .executable
        .as_ref()
        .is_some_and(|path| path.is_absolute()));

    let stats = group.stats().unwrap();
    assert!(stats.active_processes.unwrap_or_default() >= 1);

    let receipt = child.shutdown(Duration::from_secs(1)).await.unwrap();
    assert!(receipt.finished_at >= receipt.started_at);
}

#[test]
fn adopts_an_external_process_group_with_identity_check() {
    let mut external_command = std::process::Command::new("/bin/sleep");
    external_command.arg("8");
    unsafe {
        external_command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut external = external_command.spawn().unwrap();
    let process_id = external.id();
    let group = ProcessGroup::new().unwrap();
    let adopted = group
        .adopt(process_id, Some(PathBuf::from("/bin/sleep").as_path()))
        .unwrap();
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
