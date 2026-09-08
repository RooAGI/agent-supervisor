use std::fmt;
use std::net::ToSocketAddrs;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
pub enum ProbeError {
    InvalidAddress(String),
    InvalidUrl(String),
    TimedOut { address: String, attempts: u32 },
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAddress(address) => write!(formatter, "invalid probe address: {address}"),
            Self::InvalidUrl(url) => write!(formatter, "invalid HTTP probe URL: {url}"),
            Self::TimedOut { address, attempts } => write!(
                formatter,
                "TCP probe timed out for {address} after {attempts} attempts"
            ),
        }
    }
}

/// Wait for an HTTP endpoint and require a successful or redirect status.
/// This intentionally supports plain HTTP only; callers needing TLS should
/// provide a TLS-capable probe at the integration boundary.
pub async fn wait_for_http(url: impl AsRef<str>, timeout: Duration) -> Result<(), ProbeError> {
    let url = url.as_ref().to_owned();
    let remainder = url
        .strip_prefix("http://")
        .ok_or_else(|| ProbeError::InvalidUrl(url.clone()))?;
    let (address, path) = match remainder.split_once('/') {
        Some((address, path)) if !address.is_empty() => (address, format!("/{path}")),
        Some(_) | None => (remainder, "/".to_owned()),
    };
    if address.is_empty() || address.contains('@') {
        return Err(ProbeError::InvalidUrl(url));
    }
    let socket_addresses = address
        .to_socket_addrs()
        .map_err(|_| ProbeError::InvalidUrl(url.clone()))?
        .collect::<Vec<_>>();
    if socket_addresses.is_empty() {
        return Err(ProbeError::InvalidUrl(url));
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ProbeError::TimedOut {
                address: url,
                attempts: 1,
            });
        }
        for socket_address in &socket_addresses {
            let result = tokio::time::timeout(remaining, async {
                let mut stream = TcpStream::connect(socket_address).await?;
                let request =
                    format!("HEAD {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
                stream.write_all(request.as_bytes()).await?;
                let mut response = Vec::with_capacity(128);
                let mut buffer = [0_u8; 128];
                loop {
                    let read = stream.read(&mut buffer).await?;
                    if read == 0 || response.len() >= 4096 {
                        break;
                    }
                    response.extend_from_slice(&buffer[..read.min(4096 - response.len())]);
                    if response.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                Ok::<bool, std::io::Error>(
                    String::from_utf8_lossy(&response)
                        .split_whitespace()
                        .nth(1)
                        .and_then(|status| status.parse::<u16>().ok())
                        .is_some_and(|status| (200..400).contains(&status)),
                )
            })
            .await;
            if matches!(result, Ok(Ok(true))) {
                return Ok(());
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ProbeError::TimedOut {
                address: url,
                attempts: 1,
            });
        }
        tokio::time::sleep(Duration::from_millis(25).min(remaining)).await;
    }
}

impl std::error::Error for ProbeError {}

/// Wait until a TCP endpoint accepts a connection or the deadline expires.
pub async fn wait_for_tcp(address: impl AsRef<str>, timeout: Duration) -> Result<(), ProbeError> {
    let address = address.as_ref().to_owned();
    let socket_addresses = address
        .to_socket_addrs()
        .map_err(|_| ProbeError::InvalidAddress(address.clone()))?
        .collect::<Vec<_>>();
    if socket_addresses.is_empty() {
        return Err(ProbeError::InvalidAddress(address));
    }

    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0;
    loop {
        attempts += 1;
        for socket_address in &socket_addresses {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(ProbeError::TimedOut { address, attempts });
            }
            if tokio::time::timeout(remaining, TcpStream::connect(socket_address))
                .await
                .is_ok_and(|result| result.is_ok())
            {
                return Ok(());
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ProbeError::TimedOut { address, attempts });
        }
        tokio::time::sleep(Duration::from_millis(25).min(remaining)).await;
    }
}

/// Compatibility alias for callers that describe the endpoint as a port probe.
pub async fn wait_for_port(address: impl AsRef<str>, timeout: Duration) -> Result<(), ProbeError> {
    wait_for_tcp(address, timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn detects_listening_endpoint() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        wait_for_tcp(address, Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn reports_timeout() {
        // TEST-NET-1 is reserved for documentation and cannot have a local
        // listener, so this test does not depend on an ephemeral port being
        // free when the connect loop runs.
        let address = "192.0.2.1:9";
        let error = wait_for_port(address, Duration::from_millis(30))
            .await
            .unwrap_err();
        assert!(matches!(error, ProbeError::TimedOut { .. }));
    }

    #[tokio::test]
    async fn detects_http_success() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        wait_for_http(format!("http://{address}/ready"), Duration::from_secs(1))
            .await
            .unwrap();
        server.await.unwrap();
    }
}
