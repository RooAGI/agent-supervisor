use super::{McpCallPolicy, McpPolicyState, RequestDecision, ResponseDecision};
use serde_json::Value;
use std::io;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

/// Relay one MCP stdio session with shared tool-call controls.
pub async fn relay_mcp<CR, CW, SR, SW>(
    client_read: CR,
    mut client_write: CW,
    server_read: SR,
    mut server_write: SW,
    policy: McpCallPolicy,
    max_session_bytes: usize,
    client_closed: CancellationToken,
) -> io::Result<()>
where
    CR: AsyncRead + Unpin,
    CW: AsyncWrite + Unpin,
    SR: AsyncRead + Unpin,
    SW: AsyncWrite + Unpin,
{
    if policy.call_timeout.is_zero()
        || policy.max_concurrent_calls == 0
        || policy.max_result_bytes == 0
        || max_session_bytes == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MCP limits must be positive",
        ));
    }
    let mut client = BufReader::new(client_read);
    let mut server = BufReader::new(server_read);
    let mut client_line = Vec::new();
    let mut server_line = Vec::new();
    let mut client_bytes = 0usize;
    let mut server_bytes = 0usize;
    let mut input_open = true;
    let mut state = McpPolicyState::new(policy);
    let mut tick = tokio::time::interval(Duration::from_millis(10));

    loop {
        tokio::select! {
            read = async {
                let remaining = max_session_bytes.saturating_sub(client_bytes.saturating_add(client_line.len()));
                (&mut client).take(remaining.saturating_add(1) as u64).read_until(b'\n', &mut client_line).await
            }, if input_open => {
                let count = read?;
                if count == 0 && client_line.is_empty() {
                    input_open = false;
                    server_write.shutdown().await?;
                    client_closed.cancel();
                    continue;
                }
                client_bytes = client_bytes.checked_add(client_line.len()).ok_or_else(limit_error)?;
                if client_bytes > max_session_bytes { return Err(limit_error()); }
                let action = serde_json::from_slice::<Value>(&client_line)
                    .ok().map(|value| state.request(&value, Instant::now()));
                match action {
                    Some(RequestDecision::Reply(reply)) => write_json(&mut client_write, &reply).await?,
                    Some(RequestDecision::Drop) => {},
                    _ => { server_write.write_all(&client_line).await?; server_write.flush().await?; }
                }
                client_line.clear();
            }
            read = async {
                let remaining = max_session_bytes.saturating_sub(server_bytes.saturating_add(server_line.len()));
                (&mut server).take(remaining.saturating_add(1) as u64).read_until(b'\n', &mut server_line).await
            } => {
                let count = read?;
                if count == 0 && server_line.is_empty() { return Ok(()); }
                server_bytes = server_bytes.checked_add(server_line.len()).ok_or_else(limit_error)?;
                if server_bytes > max_session_bytes { return Err(limit_error()); }
                match serde_json::from_slice::<Value>(&server_line) {
                    Ok(value) => match state.response(value) {
                        ResponseDecision::Forward(value) => write_json(&mut client_write, &value).await?,
                        ResponseDecision::Suppress => {}
                    },
                    Err(_) => { client_write.write_all(&server_line).await?; client_write.flush().await?; }
                }
                server_line.clear();
            }
            _ = tick.tick(), if input_open => {
                for (reply, cancellation) in state.expire(Instant::now()) {
                    write_json(&mut client_write, &reply).await?;
                    write_json(&mut server_write, &cancellation).await?;
                }
            }
        }
    }
}

fn limit_error() -> io::Error {
    io::Error::other("stdio byte limit exceeded")
}

async fn write_json(writer: &mut (impl AsyncWrite + Unpin), value: &Value) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn relay_filters_discovery_and_rejects_denied_calls() {
        let (client, proxy_client) = tokio::io::duplex(4096);
        let (server, proxy_server) = tokio::io::duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (proxy_read, proxy_write) = tokio::io::split(proxy_client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let (proxy_server_read, proxy_server_write) = tokio::io::split(proxy_server);
        let mut policy = McpCallPolicy::default();
        policy.denied_tools.insert("delete".into());
        let relay = tokio::spawn(relay_mcp(
            proxy_read,
            proxy_write,
            proxy_server_read,
            proxy_server_write,
            policy,
            4096,
            CancellationToken::new(),
        ));
        let mut client_read = BufReader::new(client_read);
        let mut server_read = BufReader::new(server_read);
        client_write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n")
            .await
            .unwrap();
        let mut line = String::new();
        server_read.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["method"],
            "tools/list"
        );
        server_write.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"delete\"},{\"name\":\"read\"}]}}\n").await.unwrap();
        line.clear();
        client_read.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["result"]["tools"],
            json!([{"name":"read"}])
        );
        client_write.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"delete\"}}\n").await.unwrap();
        line.clear();
        client_read.read_line(&mut line).await.unwrap();
        assert!(serde_json::from_str::<Value>(&line).unwrap()["error"].is_object());
        relay.abort();
    }

    #[tokio::test]
    async fn timeout_sends_error_and_cancellation_and_suppresses_late_result() {
        let (client, proxy_client) = tokio::io::duplex(4096);
        let (server, proxy_server) = tokio::io::duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client);
        let (proxy_read, proxy_write) = tokio::io::split(proxy_client);
        let (server_read, mut server_write) = tokio::io::split(server);
        let (proxy_server_read, proxy_server_write) = tokio::io::split(proxy_server);
        let policy = McpCallPolicy {
            call_timeout: Duration::from_millis(20),
            ..Default::default()
        };
        let relay = tokio::spawn(relay_mcp(
            proxy_read,
            proxy_write,
            proxy_server_read,
            proxy_server_write,
            policy,
            4096,
            CancellationToken::new(),
        ));
        let mut client_read = BufReader::new(client_read);
        let mut server_read = BufReader::new(server_read);
        client_write.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"slow\"}}\n").await.unwrap();
        let mut line = String::new();
        server_read.read_line(&mut line).await.unwrap();
        line.clear();
        tokio::time::timeout(Duration::from_secs(1), client_read.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(serde_json::from_str::<Value>(&line).unwrap()["error"].is_object());
        line.clear();
        server_read.read_line(&mut line).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["method"],
            "notifications/cancelled"
        );
        server_write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"content\":[]}}\n")
            .await
            .unwrap();
        line.clear();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), client_read.read_line(&mut line))
                .await
                .is_err()
        );
        relay.abort();
    }
}
