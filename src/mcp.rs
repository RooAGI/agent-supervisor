//! Helpers for running a local stdio MCP server under process supervision.
//!
//! [`run_stdio_server`] re-executes the current executable as a supervised
//! child, then forwards stdin, stdout, and stderr between the MCP client and
//! that child. The supplied server closure runs only in the child process.

use crate::{spawn, ExecutionRequest, SandboxPolicy, SupervisedChild, TerminationReason};
use std::error::Error;
use std::ffi::OsString;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{self as tokio_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

mod policy;
mod relay;
pub use policy::{McpCallPolicy, McpPolicyState, RequestDecision, ResponseDecision};
pub use relay::relay_mcp;

const CHILD_MARKER: &str = "AGENT_SUPERVISOR_MCP_CHILD";
const CHILD_MARKER_PREFIX: &str = "as-mcp-child:";

/// Copy one session stream while enforcing a total byte limit.
pub async fn copy_bounded(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    limit: usize,
) -> io::Result<()> {
    let mut buffer = [0; 8192];
    let mut total = 0usize;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        total = total
            .checked_add(count)
            .ok_or_else(|| io::Error::other("stdio byte limit exceeded"))?;
        if total > limit {
            return Err(io::Error::other("stdio byte limit exceeded"));
        }
        writer.write_all(&buffer[..count]).await?;
        writer.flush().await?;
    }
}
/// Error returned when server startup, process management, or stdio forwarding fails.
pub type McpServerError = Box<dyn Error + Send + Sync + 'static>;

static NEXT_CHILD_MARKER: AtomicU64 = AtomicU64::new(0);

/// Run an MCP server over stdio with its server process supervised.
///
/// Put the existing server startup and wait logic in `serve`. The same
/// executable is launched again with the given [`SandboxPolicy`], and the
/// closure is invoked in that child. In the parent, this function relays the
/// MCP client's stdio to and from the child until the server exits.
///
/// This is intended for a local stdio MCP server entry point, such as an
/// `rmcp` server. Tool requests pass through the shared MCP policy layer.
/// HTTP servers are not supported by this entry point.
/// The executable's forwarded command-line arguments must be valid UTF-8.
///
/// The parent process uses a two second shutdown grace period after the MCP
/// client's stdin closes. For required server environment variables, set
/// `policy.environment.inherit` or `policy.environment.variables` explicitly.
pub async fn run_stdio_server<F, Fut, E>(
    policy: SandboxPolicy,
    serve: F,
) -> Result<(), McpServerError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Into<McpServerError>,
{
    run_stdio_server_with_config(policy, McpCallPolicy::default(), serve).await
}

/// Run a stdio server with explicit shared tool-call controls.
pub async fn run_stdio_server_with_config<F, Fut, E>(
    mut policy: SandboxPolicy,
    controls: McpCallPolicy,
    serve: F,
) -> Result<(), McpServerError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: Into<McpServerError>,
{
    if controls.call_timeout.is_zero()
        || controls.max_concurrent_calls == 0
        || controls.max_result_bytes == 0
        || controls.max_session_bytes == 0
        || controls.shutdown_grace > std::time::Duration::from_secs(60)
    {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid MCP supervision limits",
        )));
    }
    if child_marker_is_set() {
        return serve().await.map_err(Into::into);
    }

    let executable = std::env::current_exe()?;
    let args = std::env::args_os()
        .skip(1)
        .map(|arg| {
            arg.into_string().map_err(|arg: OsString| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("MCP server argument is not valid UTF-8: {arg:?}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let working_directory = std::env::current_dir()?;
    let marker = next_child_marker();
    policy.environment.inherit.remove(CHILD_MARKER);
    policy
        .environment
        .variables
        .insert(CHILD_MARKER.to_owned(), marker);

    let request = ExecutionRequest {
        executable,
        args,
        working_directory: Some(working_directory),
        policy,
    };
    let child = spawn(&request)?;
    proxy_stdio(child, controls).await
}

fn child_marker_is_set() -> bool {
    std::env::var(CHILD_MARKER)
        .map(|value| value.starts_with(CHILD_MARKER_PREFIX))
        .unwrap_or(false)
}

fn next_child_marker() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = NEXT_CHILD_MARKER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{CHILD_MARKER_PREFIX}{}:{timestamp}:{counter}",
        std::process::id()
    )
}

async fn proxy_stdio(
    mut child: SupervisedChild,
    controls: McpCallPolicy,
) -> Result<(), McpServerError> {
    let session_limit = controls.max_session_bytes;
    let shutdown_grace = controls.shutdown_grace;
    let child_stdin = child
        .take_stdin()
        .ok_or_else(|| io::Error::other("missing child stdin"))?;
    let child_stdout = child
        .take_stdout()
        .ok_or_else(|| io::Error::other("missing child stdout"))?;
    let child_stderr = child
        .take_stderr()
        .ok_or_else(|| io::Error::other("missing child stderr"))?;
    let client_closed = CancellationToken::new();
    let relay_closed = client_closed.clone();
    let mut relay = tokio::spawn(relay_mcp(
        tokio_io::stdin(),
        tokio_io::stdout(),
        child_stdout,
        child_stdin,
        controls,
        session_limit,
        relay_closed,
    ));
    let mut logs = tokio::spawn(copy_bounded(
        child_stderr,
        tokio_io::stderr(),
        session_limit,
    ));
    let cancellation = CancellationToken::new();
    let wait_cancellation = cancellation.clone();
    let mut wait = tokio::spawn(async move {
        child
            .wait_with_cancellation(wait_cancellation, shutdown_grace)
            .await
    });
    let mut input_closed = false;
    let mut relay_result = None;
    let mut log_result = None;
    let receipt = loop {
        tokio::select! {
            result = &mut wait => break result??,
            _ = client_closed.cancelled(), if !input_closed => {
                input_closed = true;
                cancellation.cancel();
            }
            result = &mut relay, if relay_result.is_none() => {
                let result = join_copy_result(result);
                if result.is_err() { cancellation.cancel(); }
                relay_result = Some(result);
            }
            result = &mut logs, if log_result.is_none() => {
                let result = join_copy_result(result);
                if result.is_err() { cancellation.cancel(); }
                log_result = Some(result);
            }
        }
    };
    if relay_result.is_none() {
        relay_result = Some(
            match tokio::time::timeout(shutdown_grace, &mut relay).await {
                Ok(result) => join_copy_result(result),
                Err(_) => {
                    relay.abort();
                    Ok(())
                }
            },
        );
    }
    if log_result.is_none() {
        log_result = Some(
            match tokio::time::timeout(shutdown_grace, &mut logs).await {
                Ok(result) => join_copy_result(result),
                Err(_) => {
                    logs.abort();
                    Ok(())
                }
            },
        );
    }
    relay_result.unwrap()?;
    log_result.unwrap()?;
    match receipt.termination {
        TerminationReason::Exited { code: Some(0), .. } => Ok(()),
        TerminationReason::Cancelled if input_closed => Ok(()),
        termination => Err(Box::new(io::Error::other(format!(
            "supervised MCP server exited with {termination:?}"
        )))),
    }
}

fn join_copy_result(
    result: Result<Result<(), io::Error>, tokio::task::JoinError>,
) -> Result<(), McpServerError> {
    match result {
        Ok(result) => result.map_err(|error| Box::new(error) as McpServerError),
        Err(error) => Err(Box::new(error)),
    }
}
