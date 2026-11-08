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
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => socket = args.next().map(PathBuf::from),
            "--view-token-file" => view_token_file = args.next().map(PathBuf::from),
            "--control-token-file" => control_token_file = args.next().map(PathBuf::from),
            "--tui-token-file" => tui_token_file = args.next().map(PathBuf::from),
            "--init-credentials-dir" => init_credentials_dir = args.next().map(PathBuf::from),
            "--help" | "-h" => {
                println!("agent-supervisor-service --init-credentials-dir DIR");
                println!("agent-supervisor-service --socket PATH --view-token-file PATH --control-token-file PATH --tui-token-file PATH");
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
    SupervisorService::new(
        Arc::new(agent_supervisor::Supervisor::new()),
        view_token,
        control_token,
        tui_token,
    )?
    .serve_unix(socket)
    .await
}

#[cfg(not(unix))]
fn main() {
    eprintln!("agent-supervisor-service currently supports Unix sockets only");
    std::process::exit(2);
}
