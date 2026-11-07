//! Internal byte relay for the Python MCP launcher. MCP framing stays in the SDK.
use super::*;
use agent_supervisor::mcp::{copy_bounded, relay_mcp, McpCallPolicy};
use std::time::Duration;

#[pyfunction]
pub(super) fn mcp_defaults() -> (f64, usize, usize, usize, f64) {
    let defaults = McpCallPolicy::default();
    (
        defaults.call_timeout.as_secs_f64(),
        defaults.max_concurrent_calls,
        defaults.max_result_bytes,
        defaults.max_session_bytes,
        defaults.shutdown_grace.as_secs_f64(),
    )
}

// Mirrors the keyword-only Python function, so the arguments cannot be grouped.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (command, *, policy=None, cancellation=None, shutdown_grace=2.0, call_timeout=30.0, max_concurrent_calls=4, max_result_bytes=1048576, denied_tools=None))]
pub(super) fn relay_stdio<'py>(
    py: Python<'py>,
    command: PyRef<'py, Command>,
    policy: Option<PyRef<'py, Policy>>,
    cancellation: Option<PyRef<'py, CancellationToken>>,
    shutdown_grace: f64,
    call_timeout: f64,
    max_concurrent_calls: usize,
    max_result_bytes: usize,
    denied_tools: Option<Vec<String>>,
) -> PyResult<Bound<'py, PyAny>> {
    if !shutdown_grace.is_finite() || !(0.0..=60.0).contains(&shutdown_grace) {
        return Err(PyValueError::new_err(
            "shutdown_grace must be between 0 and 60 seconds",
        ));
    }
    if !call_timeout.is_finite()
        || call_timeout <= 0.0
        || max_concurrent_calls == 0
        || max_result_bytes == 0
    {
        return Err(PyValueError::new_err("MCP call limits must be positive"));
    }
    let request = request_from(&command, policy.as_deref());
    let call_duration = Duration::try_from_secs_f64(call_timeout)
        .map_err(|_| PyValueError::new_err("call_timeout is too large"))?;
    let controls = McpCallPolicy {
        call_timeout: call_duration,
        max_concurrent_calls,
        max_result_bytes,
        denied_tools: denied_tools.unwrap_or_default().into_iter().collect(),
        max_session_bytes: request.policy.limits.input_bytes,
        shutdown_grace: Duration::from_secs_f64(shutdown_grace),
    };
    let token = cancellation.map(|t| t.token.clone()).unwrap_or_default();
    let grace = Duration::from_secs_f64(shutdown_grace);
    future_into_py(py, async move {
        let worker_token = token.clone();
        let worker = tokio::spawn(async move {
            // The group outlives the root and cleans up descendants even on normal exit.
            let group = agent_supervisor::ProcessGroup::new()?;
            let mut child = group.start(&request)?;
            let stdin = child.take_stdin().expect("piped stdin");
            let stdout = child.take_stdout().expect("piped stdout");
            let stderr = child.take_stderr().expect("piped stderr");
            let client_closed = tokio_util::sync::CancellationToken::new();
            let mut relay = tokio::spawn(relay_mcp(
                tokio::io::stdin(),
                tokio::io::stdout(),
                stdout,
                stdin,
                controls,
                request.policy.limits.input_bytes,
                client_closed.clone(),
            ));
            let mut logs = tokio::spawn(copy_bounded(
                stderr,
                tokio::io::stderr(),
                request.policy.limits.stderr_bytes,
            ));
            let wait = child.wait_with_cancellation(worker_token.clone(), grace);
            tokio::pin!(wait);
            let mut relay_done = false;
            let mut logs_done = false;
            let mut io_error = None;
            let mut input_closed = false;
            let receipt = loop {
                tokio::select! {
                    receipt = &mut wait => break receipt,
                    _ = client_closed.cancelled(), if !input_closed => {
                        input_closed = true;
                        worker_token.cancel();
                    }
                    result = &mut relay, if !relay_done => {
                        relay_done = true;
                        if let Err(error) = result.map_err(std::io::Error::other).and_then(|v| v) {
                            io_error = Some(error);
                            worker_token.cancel();
                        }
                    }
                    result = &mut logs, if !logs_done => {
                        logs_done = true;
                        if let Err(error) = result.map_err(std::io::Error::other).and_then(|v| v) {
                            io_error = Some(error);
                            worker_token.cancel();
                        }
                    }
                }
            };
            group.terminate();
            if !relay_done {
                match tokio::time::timeout(grace, &mut relay).await {
                    Ok(result) => {
                        if let Err(error) = result.map_err(std::io::Error::other).and_then(|v| v) {
                            io_error.get_or_insert(error);
                        }
                    }
                    Err(_) => relay.abort(),
                }
            }
            if !logs_done {
                match tokio::time::timeout(grace, &mut logs).await {
                    Ok(result) => {
                        if let Err(error) = result.map_err(std::io::Error::other).and_then(|v| v) {
                            io_error.get_or_insert(error);
                        }
                    }
                    Err(_) => logs.abort(),
                }
            }
            if let Some(error) = io_error {
                return Err(SandboxError::execution(error));
            }
            receipt
        });
        let mut guard = CancelOperationOnDrop { token, armed: true };
        let receipt = worker
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("stdio worker failed: {e}")))?
            .map_err(to_py_error)?;
        guard.armed = false;
        Python::attach(|py| Py::new(py, RunResult::from_receipt(receipt, Vec::new(), Vec::new())))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create test runtime")
    }

    #[test]
    fn bounded_copy_preserves_stdio_bytes() {
        runtime().block_on(async {
            let (mut source, reader) = tokio::io::duplex(3);
            let (writer, mut target) = tokio::io::duplex(3);
            let relay = tokio::spawn(copy_bounded(reader, writer, 32));
            let send = tokio::spawn(async move {
                source.write_all(b"MCP bytes\r\n").await.unwrap();
            });

            let mut received = Vec::new();
            target.read_to_end(&mut received).await.unwrap();
            send.await.unwrap();
            relay.await.unwrap().unwrap();
            assert_eq!(received, b"MCP bytes\r\n");
        });
    }

    #[test]
    fn bounded_copy_rejects_traffic_over_the_session_limit() {
        runtime().block_on(async {
            let (mut source, reader) = tokio::io::duplex(8);
            let (writer, mut target) = tokio::io::duplex(8);
            source.write_all(b"four").await.unwrap();
            drop(source);

            let error = copy_bounded(reader, writer, 3).await.unwrap_err();
            assert_eq!(error.to_string(), "stdio byte limit exceeded");
            let mut received = Vec::new();
            target.read_to_end(&mut received).await.unwrap();
            assert!(received.len() <= 3);
        });
    }
}
