#![cfg(windows)]

use agent_supervisor::{
    execute, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, FilesystemAccess,
    FilesystemGrant, FilesystemPolicy,
};
use std::collections::BTreeMap;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

fn request(
    args: Vec<String>,
    policy: FilesystemPolicy,
    working_directory: Option<PathBuf>,
) -> ExecutionRequest {
    let windir = std::env::var_os("WINDIR").expect("WINDIR");
    ExecutionRequest {
        executable: PathBuf::from(windir).join("System32").join("cmd.exe"),
        args,
        working_directory,
        policy: agent_supervisor::SandboxPolicy {
            environment: EnvironmentPolicy {
                inherit: [
                    "SystemRoot",
                    "WINDIR",
                    "ComSpec",
                    "PATH",
                    "PATHEXT",
                    "TEMP",
                    "TMP",
                    "USERPROFILE",
                    "HOMEDRIVE",
                    "HOMEPATH",
                    "APPDATA",
                    "LOCALAPPDATA",
                    "ProgramData",
                    "ALLUSERSPROFILE",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                variables: BTreeMap::new(),
                executable_search_paths: Vec::new(),
            },
            filesystem: Some(policy),
            enforcement: EnforcementRequirement::Required,
            ..agent_supervisor::SandboxPolicy::default()
        },
    }
}

#[tokio::test]
async fn appcontainer_enforces_read_and_write_grants() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let denied_directory = tempfile::tempdir().expect("denied temporary directory");
    let allowed = directory.path().join("allowed.txt");
    let denied = denied_directory.path().join("denied.txt");
    std::fs::write(&allowed, "allowed\r\n").expect("allowed file");
    std::fs::write(&denied, "secret\r\n").expect("denied file");

    let read = execute(
        &request(
            vec!["/C".into(), "type".into(), allowed.display().to_string()],
            FilesystemPolicy::new(vec![FilesystemGrant {
                root: directory.path().to_path_buf(),
                access: vec![FilesystemAccess::Read],
            }]),
            Some(directory.path().to_path_buf()),
        ),
        b"",
    )
    .await
    .expect("read execution");
    assert!(read.success);
    assert!(String::from_utf8_lossy(&read.stdout).contains("allowed"));

    let created = directory.path().join("created.txt");
    let write = execute(
        &request(
            vec![
                "/C".into(),
                "echo created".into(),
                ">".into(),
                created.display().to_string(),
            ],
            FilesystemPolicy::new(vec![FilesystemGrant {
                root: directory.path().to_path_buf(),
                access: vec![FilesystemAccess::Read, FilesystemAccess::Write],
            }]),
            Some(directory.path().to_path_buf()),
        ),
        b"",
    )
    .await
    .expect("write execution");
    assert!(
        write.success,
        "stderr: {}",
        String::from_utf8_lossy(&write.stderr)
    );
    assert!(std::fs::read_to_string(created)
        .unwrap()
        .contains("created"));

    let denied_read = execute(
        &request(
            vec!["/C".into(), "type".into(), denied.display().to_string()],
            FilesystemPolicy::new(vec![FilesystemGrant {
                root: directory.path().to_path_buf(),
                access: vec![FilesystemAccess::Read],
            }]),
            Some(directory.path().to_path_buf()),
        ),
        b"",
    )
    .await
    .expect("denied execution");
    assert!(!denied_read.success);
}

#[tokio::test]
async fn filesystem_sandbox_preserves_host_network_loopback() {
    let directory = tempfile::tempdir().expect("temporary working directory");
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

    let windir = std::env::var_os("WINDIR").expect("WINDIR");
    let curl = PathBuf::from(windir).join("System32").join("curl.exe");
    let command = format!(
        "\"{}\" --silent --show-error --connect-timeout 3 http://127.0.0.1:{port}",
        curl.display()
    );
    let output = execute(
        &request(
            vec!["/C".into(), command],
            FilesystemPolicy::new(vec![FilesystemGrant {
                root: directory.path().to_path_buf(),
                access: vec![FilesystemAccess::Read],
            }]),
            Some(directory.path().to_path_buf()),
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
