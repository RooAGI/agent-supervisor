#[cfg(unix)]
#[tokio::main]
async fn main() -> std::io::Result<()> {
    use agent_supervisor::service::{create_credentials_dir, read_token_file, SupervisorService};
    use std::path::PathBuf;
    use std::sync::Arc;

    let mut socket = None;
    let mut view_token_file = None;
    let mut control_token_file = None;
    let mut tui_token_file = None;
    let mut init_credentials_dir = None;
    let mut state_dir = None;
    let mut ready_file = None;
    let mut flowboard_mcp_command = None;
    let mut flowboard_mcp_args = Vec::new();
    let mut flowboard_mcp_env = std::collections::BTreeMap::new();
    let mut workflow_worker_command = None;
    let mut workflow_worker_args = Vec::new();
    let mut workflow_worker_cwd = None;
    let mut workflow_worker_env = std::collections::BTreeMap::new();
    let mut status_only = false;
    let mut shutdown_only = false;
    let mut grace_ms = 1500u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => socket = args.next().map(PathBuf::from),
            "--view-token-file" => view_token_file = args.next().map(PathBuf::from),
            "--control-token-file" => control_token_file = args.next().map(PathBuf::from),
            "--tui-token-file" => tui_token_file = args.next().map(PathBuf::from),
            "--init-credentials-dir" => init_credentials_dir = args.next().map(PathBuf::from),
            "--state-dir" => state_dir = args.next().map(PathBuf::from),
            "--ready-file" => ready_file = args.next().map(PathBuf::from),
            "--flowboard-mcp-command" => flowboard_mcp_command = args.next().map(PathBuf::from),
            "--flowboard-mcp-arg" => flowboard_mcp_args.push(args.next().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--flowboard-mcp-arg requires a value",
                )
            })?),
            "--flowboard-mcp-env" => {
                let key = args.next().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "--flowboard-mcp-env requires an environment variable name",
                    )
                })?;
                if key.is_empty() || key.contains('=') || key.contains('\0') {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid Flowboard MCP environment variable name",
                    ));
                }
                let value = std::env::var(&key).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Flowboard MCP environment variable {key} is not set"),
                    )
                })?;
                flowboard_mcp_env.insert(key, value);
            }
            "--workflow-worker-command" => workflow_worker_command = args.next().map(PathBuf::from),
            "--workflow-worker-arg" => workflow_worker_args.push(args.next().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--workflow-worker-arg requires a value",
                )
            })?),
            "--workflow-worker-cwd" => workflow_worker_cwd = args.next().map(PathBuf::from),
            "--workflow-worker-env" => {
                let key = args.next().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "--workflow-worker-env requires an environment variable name",
                    )
                })?;
                if key.is_empty() || key.contains('=') || key.contains('\0') {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid workflow worker environment variable name",
                    ));
                }
                let value = std::env::var(&key).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("workflow worker environment variable {key} is not set"),
                    )
                })?;
                workflow_worker_env.insert(key, value);
            }
            "--status" => status_only = true,
            "--shutdown" => shutdown_only = true,
            "--grace-ms" => {
                grace_ms = args
                    .next()
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "--grace-ms requires a number",
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "invalid --grace-ms value",
                        )
                    })?;
            }
            "--help" | "-h" => {
                println!("agent-supervisor-service --init-credentials-dir DIR");
                println!("agent-supervisor-service --socket PATH --view-token-file PATH --control-token-file PATH --tui-token-file PATH [--state-dir DIR] [--ready-file PATH] [--flowboard-mcp-command PATH [--flowboard-mcp-arg ARG]...] [--workflow-worker-command PATH [--workflow-worker-arg ARG]...]");
                println!("agent-supervisor-service --status --socket PATH --view-token-file PATH");
                println!("agent-supervisor-service --shutdown --socket PATH --control-token-file PATH [--grace-ms N]");
                return Ok(());
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                ))
            }
        }
    }
    if let Some(dir) = init_credentials_dir {
        if socket.is_some()
            || view_token_file.is_some()
            || control_token_file.is_some()
            || tui_token_file.is_some()
            || state_dir.is_some()
            || ready_file.is_some()
            || flowboard_mcp_command.is_some()
            || !flowboard_mcp_args.is_empty()
            || !flowboard_mcp_env.is_empty()
            || workflow_worker_command.is_some()
            || !workflow_worker_args.is_empty()
            || workflow_worker_cwd.is_some()
            || !workflow_worker_env.is_empty()
            || status_only
            || shutdown_only
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--init-credentials-dir cannot be combined with service startup options",
            ));
        }
        let paths = create_credentials_dir(dir)?;
        println!("view-token-file={}", paths.view_token_file.display());
        println!("control-token-file={}", paths.control_token_file.display());
        println!("tui-token-file={}", paths.tui_token_file.display());
        return Ok(());
    }
    let socket = socket.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "--socket is required")
    })?;
    if status_only || shutdown_only {
        if status_only == shutdown_only
            || state_dir.is_some()
            || ready_file.is_some()
            || flowboard_mcp_command.is_some()
            || !flowboard_mcp_args.is_empty()
            || !flowboard_mcp_env.is_empty()
            || workflow_worker_command.is_some()
            || !workflow_worker_args.is_empty()
            || workflow_worker_cwd.is_some()
            || !workflow_worker_env.is_empty()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "choose one of --status or --shutdown; neither accepts service startup options",
            ));
        }
        let (role, token_file, request) = if status_only {
            let token = view_token_file.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--status requires --view-token-file",
                )
            })?;
            (
                "view",
                token,
                serde_json::json!({"type":"status","requestId":"cli-status"}),
            )
        } else {
            let token = control_token_file.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--shutdown requires --control-token-file",
                )
            })?;
            (
                "control",
                token,
                serde_json::json!({"type":"shutdown","requestId":"cli-shutdown","graceMs":grace_ms.min(30_000)}),
            )
        };
        return service_request(socket, role, read_token_file(token_file)?, request).await;
    }
    let default_state_dir = socket
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let state_dir = state_dir.unwrap_or_else(|| default_state_dir.to_owned());
    let view_token_file = view_token_file.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--view-token-file is required",
        )
    })?;
    let control_token_file = control_token_file.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--control-token-file is required",
        )
    })?;
    let tui_token_file = tui_token_file.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--tui-token-file is required",
        )
    })?;
    let view_token = read_token_file(view_token_file)?;
    let control_token = read_token_file(control_token_file)?;
    let tui_token = read_token_file(tui_token_file)?;
    let service = SupervisorService::new(
        Arc::new(agent_supervisor::Supervisor::new()),
        view_token,
        control_token,
        tui_token,
    )?
    .with_state_dir(&state_dir);
    let service = if let Some(command) = flowboard_mcp_command {
        service.with_flowboard_mcp(command, flowboard_mcp_args, flowboard_mcp_env)
    } else {
        if !flowboard_mcp_args.is_empty() || !flowboard_mcp_env.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "--flowboard-mcp-arg/--flowboard-mcp-env require --flowboard-mcp-command",
            ));
        }
        service
    };
    let service = if let Some(command) = workflow_worker_command {
        service.with_workflow_worker(
            command,
            workflow_worker_args,
            workflow_worker_cwd,
            workflow_worker_env,
        )
    } else {
        if !workflow_worker_args.is_empty()
            || workflow_worker_cwd.is_some()
            || !workflow_worker_env.is_empty()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "workflow worker arguments, cwd, and environment require --workflow-worker-command",
            ));
        }
        service
    };
    service
        .serve_unix_with_ready(socket, ready_file.as_deref())
        .await
}

#[cfg(unix)]
async fn service_request(
    socket_path: std::path::PathBuf,
    role: &str,
    token: Vec<u8>,
    request: serde_json::Value,
) -> std::io::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stream = tokio::net::UnixStream::connect(socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let auth = serde_json::json!({
        "type":"auth",
        "role":role,
        "token":String::from_utf8(token).map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData,"token file is not UTF-8"))?,
    });
    writer
        .write_all(
            serde_json::to_string(&auth)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                .as_bytes(),
        )
        .await?;
    writer.write_all(b"\n").await?;
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let ready: serde_json::Value = serde_json::from_str(&line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if ready["type"] != "ready" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "service authentication failed",
        ));
    }
    line.clear();
    writer
        .write_all(
            serde_json::to_string(&request)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                .as_bytes(),
        )
        .await?;
    writer.write_all(b"\n").await?;
    reader.read_line(&mut line).await?;
    let response: serde_json::Value = serde_json::from_str(&line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
    );
    if response["type"] == "error" {
        return Err(std::io::Error::other(
            response["message"]
                .as_str()
                .unwrap_or("service request failed"),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("agent-supervisor-service currently supports Unix sockets only");
    std::process::exit(2);
}
