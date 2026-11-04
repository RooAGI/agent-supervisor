use agent_supervisor::{mcp::run_stdio_server, SandboxPolicy};
use std::time::Duration;
use tokio::io::{self as tokio_io, AsyncBufReadExt, AsyncWriteExt, BufReader};

const TEST_ENV: &str = "AGENT_SUPERVISOR_MCP_TEST_VALUE";

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut policy = SandboxPolicy::default();
    if args.iter().any(|arg| arg == "--allow-env") {
        let value = std::env::var(TEST_ENV).expect("test environment value in fixture parent");
        policy
            .environment
            .variables
            .insert(TEST_ENV.to_owned(), value);
    }

    let result = run_stdio_server(policy, move || async move {
        if args.iter().any(|arg| arg == "--fail") {
            let mut stderr = tokio_io::stderr();
            stderr.write_all(b"fixture server failed\n").await?;
            stderr.flush().await?;
            return Err(std::io::Error::other("fixture failure"));
        }
        if args.iter().any(|arg| arg == "--wait-forever") {
            tokio::time::sleep(Duration::from_secs(30)).await;
            return Ok::<(), std::io::Error>(());
        }

        let mut stderr = tokio_io::stderr();
        let forwarded = args
            .iter()
            .find_map(|arg| arg.strip_prefix("--value="))
            .unwrap_or("missing");
        let env_value = std::env::var(TEST_ENV).unwrap_or_else(|_| "<unset>".into());
        stderr
            .write_all(format!("arg={forwarded}; env={env_value}\n").as_bytes())
            .await?;
        stderr.flush().await?;

        let mut input = tokio_io::stdin();
        let mut output = tokio_io::stdout();
        if args.iter().any(|arg| arg == "--delay-output") {
            let mut request = Vec::new();
            let mut input = BufReader::new(input);
            input.read_until(b'\n', &mut request).await?;
            let mut stderr = tokio_io::stderr();
            stderr.write_all(b"request-received\n").await?;
            stderr.flush().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            output.write_all(&request).await?;
            output.flush().await?;
            stderr.write_all(b"response-written\n").await?;
            stderr.flush().await?;
        } else {
            tokio_io::copy(&mut input, &mut output).await?;
        }
        output.shutdown().await?;
        Ok::<(), std::io::Error>(())
    })
    .await;

    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
