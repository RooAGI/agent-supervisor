#![cfg(target_os = "linux")]

use agent_supervisor::{
    execute, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, FilesystemAccess,
    FilesystemGrant, FilesystemPolicy, ResourceLimits,
};
use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

fn request(script: &str, root: PathBuf) -> ExecutionRequest {
    let working_directory = root.clone();
    let mut grants = vec![FilesystemGrant {
        root,
        access: vec![FilesystemAccess::Read, FilesystemAccess::Write],
    }];
    for path in ["/bin", "/usr/bin", "/lib", "/lib64", "/usr/lib", "/etc"] {
        let path = PathBuf::from(path);
        if path.exists() {
            grants.push(FilesystemGrant {
                root: path,
                access: vec![FilesystemAccess::Read],
            });
        }
    }
    ExecutionRequest {
        executable: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        working_directory: Some(working_directory),
        policy: agent_supervisor::SandboxPolicy {
            filesystem: Some(FilesystemPolicy::new(grants)),
            enforcement: EnforcementRequirement::Required,
            ..agent_supervisor::SandboxPolicy::default()
        },
    }
}

#[tokio::test]
async fn landlock_allows_granted_tree_and_denies_ungranted_tree() {
    let base = std::env::temp_dir().join(format!("agent-supervisor-fs-{}", std::process::id()));
    let allowed = base.join("allowed");
    let denied = base.join("denied");
    fs::create_dir_all(&allowed).unwrap();
    fs::create_dir_all(&denied).unwrap();
    fs::write(allowed.join("value"), b"allowed").unwrap();
    fs::write(denied.join("value"), b"denied").unwrap();

    let allowed_output = execute(
        &request(&format!("cat {}/value", allowed.display()), allowed.clone()),
        b"",
    )
    .await
    .unwrap();
    assert!(allowed_output.success);
    assert_eq!(allowed_output.stdout, b"allowed");

    let denied_output = execute(
        &request(&format!("cat {}/value", denied.display()), allowed),
        b"",
    )
    .await
    .unwrap();
    assert!(!denied_output.success);

    fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn landlock_preserves_host_network_loopback() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("loopback listener");
    let port = listener.local_addr().expect("listener address").port();
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let receiver = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nrooagi",
                        )
                        .expect("network response");
                    return true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return false,
            }
        }
    });

    let output = execute(
        &request(
            &format!(
                "/usr/bin/curl --silent --show-error --connect-timeout 3 http://127.0.0.1:{port}"
            ),
            std::env::current_dir().expect("working directory"),
        ),
        b"",
    )
    .await
    .expect("network execution");
    let received = receiver.join().expect("network receiver");
    assert!(
        output.success,
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(received);
    assert_eq!(output.stdout, b"rooagi");
}
