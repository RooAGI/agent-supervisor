#![cfg(target_os = "macos")]

use agent_sandbox::{
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
    ExecutionRequest {
        executable: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        working_directory: Some(root.clone()),
        policy: agent_sandbox::SandboxPolicy {
            environment: EnvironmentPolicy::default(),
            filesystem: Some(FilesystemPolicy::new(vec![
                FilesystemGrant {
                    root,
                    access: vec![FilesystemAccess::Read, FilesystemAccess::Write],
                },
                // These are read-only runtime locations needed by dynamically
                // linked system tools. The workspace grant above remains the only
                // writable user-controlled tree.
                FilesystemGrant {
                    root: PathBuf::from("/System"),
                    access: vec![FilesystemAccess::Read],
                },
                FilesystemGrant {
                    root: PathBuf::from("/usr"),
                    access: vec![FilesystemAccess::Read],
                },
                FilesystemGrant {
                    root: PathBuf::from("/bin"),
                    access: vec![FilesystemAccess::Read],
                },
                FilesystemGrant {
                    root: PathBuf::from("/dev"),
                    access: vec![FilesystemAccess::Read],
                },
                FilesystemGrant {
                    root: PathBuf::from("/private/var/db"),
                    access: vec![FilesystemAccess::Read],
                },
                FilesystemGrant {
                    root: PathBuf::from("/private/etc/ssl"),
                    access: vec![FilesystemAccess::Read],
                },
            ])),
            ..agent_sandbox::SandboxPolicy::default()
        },
    }
}

#[tokio::test]
async fn seatbelt_allows_granted_tree_and_denies_ungranted_tree() {
    let base = fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!("agent-sandbox-macos-fs-{}", std::process::id()));
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
        &request(&format!("cat {}/value", denied.display()), allowed.clone()),
        b"",
    )
    .await
    .unwrap();
    assert!(!denied_output.success);

    let write_output = execute(
        &request(
            &format!("printf changed > {}/created", allowed.display()),
            allowed.clone(),
        ),
        b"",
    )
    .await
    .unwrap();
    assert!(write_output.success);
    assert_eq!(fs::read(allowed.join("created")).unwrap(), b"changed");

    fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn seatbelt_preserves_host_network_loopback() {
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
            fs::canonicalize(std::env::temp_dir()).unwrap(),
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
