#![cfg(all(not(target_os = "linux"), not(target_os = "macos"), not(windows)))]

use rooagi_sandbox::{
    execute, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, FilesystemPolicy,
    ResourceLimits,
};
use std::path::PathBuf;

#[tokio::test]
async fn requested_filesystem_policy_fails_closed_without_native_backend() {
    let request = ExecutionRequest {
        executable: if cfg!(windows) {
            PathBuf::from(r"C:\Windows\System32\cmd.exe")
        } else {
            PathBuf::from("/bin/sh")
        },
        args: Vec::new(),
        environment: EnvironmentPolicy::default(),
        working_directory: None,
        filesystem: Some(FilesystemPolicy::deny_all()),
        network: rooagi_sandbox::NetworkMode::Host,
        limits: ResourceLimits::default(),
        enforcement: EnforcementRequirement::BestEffort,
    };

    let error = execute(&request, b"").await.unwrap_err();
    assert_eq!(error.code(), "spawn_failed");
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains("filesystem isolation"));
}
