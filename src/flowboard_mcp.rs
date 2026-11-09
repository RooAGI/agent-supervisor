//! Service-owned stdio MCP connection to a Flowboard server.

use crate::workflow::{
    FlowboardAppendResult, FlowboardCurrent, FlowboardOutboxEvent, FlowboardSink, SinkFuture,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

const MCP_FRAME_LIMIT: usize = 1024 * 1024;
const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

#[derive(Clone, Debug)]
pub(crate) struct FlowboardMcpConfig {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<PathBuf>,
}

/// Persistent MCP stdio client owned by the Supervisor service. Configure a
/// local Flowboard MCP executable; its process is started lazily and is killed
/// when the sink is shut down or dropped.
pub(crate) struct McpStdioFlowboardSink {
    config: FlowboardMcpConfig,
    process: Mutex<Option<McpProcess>>,
}

struct McpProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpStdioFlowboardSink {
    pub(crate) fn new(config: FlowboardMcpConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            process: Mutex::new(None),
        })
    }

    pub(crate) async fn shutdown(&self) {
        if let Some(mut process) = self.process.lock().await.take() {
            let _ = process.child.kill().await;
            let _ = process.child.wait().await;
        }
    }

    async fn call_tool(&self, name: &str, arguments: Value) -> Result<Value, String> {
        let mut guard = self.process.lock().await;
        if guard.is_none() {
            *guard = Some(self.spawn().await?);
        }
        let process = guard.as_mut().expect("process just initialized");
        let result = process
            .call(json!({"method":"tools/call","params":{"name":name,"arguments":arguments}}));
        let result = tokio::time::timeout(std::time::Duration::from_secs(30), result)
            .await
            .map_err(|_| "Flowboard MCP request timed out".to_owned())
            .and_then(|result| result);
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if let Some(mut process) = guard.take() {
                    let _ = process.child.kill().await;
                    let _ = process.child.wait().await;
                }
                Err(error)
            }
        }
    }

    async fn spawn(&self) -> Result<McpProcess, String> {
        let mut command = Command::new(&self.config.command);
        command
            .args(&self.config.args)
            .envs(&self.config.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        if let Some(cwd) = &self.config.cwd {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().map_err(|error| error.to_string())?;
        let stdin = child.stdin.take().ok_or("MCP child stdin unavailable")?;
        let stdout = child.stdout.take().ok_or("MCP child stdout unavailable")?;
        let mut process = McpProcess {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        };
        let initialize_call = process.call(json!({
            "method":"initialize",
            "params":{
                "protocolVersion":MCP_PROTOCOL_VERSION,
                "capabilities":{},
                "clientInfo":{"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}
            }
        }));
        let initialize = tokio::time::timeout(std::time::Duration::from_secs(30), initialize_call)
            .await
            .map_err(|_| "Flowboard MCP initialize timed out".to_owned())??;
        if initialize.get("protocolVersion").is_none() {
            return Err("Flowboard MCP initialize response omitted protocolVersion".to_owned());
        }
        process
            .notify(json!({"method":"notifications/initialized"}))
            .await?;
        Ok(process)
    }
}

impl FlowboardSink for McpStdioFlowboardSink {
    fn append<'a>(
        &'a self,
        event: &'a FlowboardOutboxEvent,
    ) -> SinkFuture<'a, FlowboardAppendResult> {
        Box::pin(async move {
            let arguments = json!({
                "actor":event.actor,
                "coordinator_fence":event.coordinator_fence,
                "data":event.data,
                "event_id":event.event_id,
                "expected_revision":event.expected_revision,
                "flow_id":event.flow_id,
                "kind":event.kind,
                "post_id":event.post_id,
                "request_id":event.request_id,
                "task_id":event.task_id,
                "attempt_id":event.attempt_id,
            });
            match self.call_tool("flow_event_append", arguments).await {
                Ok(result) => match find_u64(&result, &["revision", "flowRevision"]) {
                    Some(revision) => FlowboardAppendResult::Appended { revision },
                    None => FlowboardAppendResult::Transient(
                        "Flowboard append response omitted the committed revision".to_owned(),
                    ),
                },
                Err(error) if looks_like_revision_conflict(&error) => {
                    FlowboardAppendResult::RevisionConflict
                }
                Err(error) => FlowboardAppendResult::Rejected(error),
            }
        })
    }

    fn current<'a>(&'a self, flow_id: &'a str) -> SinkFuture<'a, Result<FlowboardCurrent, String>> {
        Box::pin(async move {
            let result = self
                .call_tool("flow_get", json!({"flow_id":flow_id}))
                .await?;
            let revision = find_u64(&result, &["revision", "flowRevision"])
                .ok_or_else(|| "Flowboard flow_get returned no revision".to_owned())?;
            let mut event_ids = Vec::new();
            collect_event_ids(&result, &mut event_ids);
            Ok(FlowboardCurrent {
                revision,
                event_ids,
            })
        })
    }
}

impl McpProcess {
    async fn notify(&mut self, request: Value) -> Result<(), String> {
        let mut message = json!({"jsonrpc":"2.0","method":request["method"]});
        if let Some(params) = request.get("params") {
            message["params"] = params.clone();
        }
        self.write_message(message).await
    }

    async fn call(&mut self, request: Value) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let mut message = json!({"jsonrpc":"2.0","id":id,"method":request["method"]});
        if let Some(params) = request.get("params") {
            message["params"] = params.clone();
        }
        self.write_message(message).await?;
        loop {
            let mut line = Vec::new();
            let bytes = (&mut self.stdout)
                .take((MCP_FRAME_LIMIT + 1) as u64)
                .read_until(b'\n', &mut line)
                .await
                .map_err(|error| error.to_string())?;
            if bytes == 0 {
                return Err("Flowboard MCP stdio disconnected".to_owned());
            }
            if bytes > MCP_FRAME_LIMIT {
                return Err("Flowboard MCP frame exceeded limit".to_owned());
            }
            let response: Value =
                serde_json::from_slice(&line).map_err(|error| error.to_string())?;
            if response.get("method").and_then(Value::as_str) == Some("ping") {
                let id = response.get("id").cloned().unwrap_or(Value::Null);
                self.write_message(json!({"jsonrpc":"2.0","id":id,"result":{}}))
                    .await?;
                continue;
            }
            if response.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = response.get("error") {
                return Err(error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Flowboard MCP request failed")
                    .to_owned());
            }
            let result = response
                .get("result")
                .cloned()
                .ok_or_else(|| "Flowboard MCP response omitted result".to_owned())?;
            if result.get("isError").and_then(Value::as_bool) == Some(true) {
                return Err(extract_text(&result)
                    .unwrap_or_else(|| "Flowboard tool returned an error".to_owned()));
            }
            return Ok(unwrap_tool_result(result));
        }
    }

    async fn write_message(&mut self, message: Value) -> Result<(), String> {
        let mut bytes = serde_json::to_vec(&message).map_err(|error| error.to_string())?;
        if bytes.len() > MCP_FRAME_LIMIT {
            return Err("Flowboard MCP request exceeded frame limit".to_owned());
        }
        bytes.push(b'\n');
        self.stdin
            .write_all(&bytes)
            .await
            .map_err(|error| error.to_string())?;
        self.stdin.flush().await.map_err(|error| error.to_string())
    }
}

fn unwrap_tool_result(result: Value) -> Value {
    if let Some(structured) = result.get("structuredContent") {
        return structured.clone();
    }
    if let Some(text) = extract_text(&result) {
        if let Ok(value) = serde_json::from_str(&text) {
            return value;
        }
        return json!({"text":text});
    }
    result
}

fn extract_text(result: &Value) -> Option<String> {
    result
        .get("content")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find_map(|item| {
                (item.get("type").and_then(Value::as_str) == Some("text"))
                    .then(|| {
                        item.get("text")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned)
                    })
                    .flatten()
            })
        })
}

fn find_u64(value: &Value, fields: &[&str]) -> Option<u64> {
    if let Some(found) = fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_u64))
    {
        return Some(found);
    }
    value
        .as_object()?
        .values()
        .find_map(|child| find_u64(child, fields))
}

fn collect_event_ids(value: &Value, output: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(id) = object
                .get("eventId")
                .or_else(|| object.get("event_id"))
                .and_then(Value::as_str)
            {
                output.push(id.to_owned());
            }
            for child in object.values() {
                collect_event_ids(child, output);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_event_ids(child, output);
            }
        }
        _ => {}
    }
}

fn looks_like_revision_conflict(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("revision") && (lower.contains("stale") || lower.contains("conflict"))
}

impl From<io::Error> for FlowboardAppendResult {
    fn from(error: io::Error) -> Self {
        Self::Transient(error.to_string())
    }
}
