//! Shared MCP tool-call policy and request tracking.
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Controls applied to MCP tools in a single stdio session.
#[derive(Clone, Debug)]
pub struct McpCallPolicy {
    pub call_timeout: Duration,
    pub max_concurrent_calls: usize,
    pub max_result_bytes: usize,
    pub denied_tools: HashSet<String>,
    pub max_session_bytes: usize,
    pub shutdown_grace: Duration,
}

impl Default for McpCallPolicy {
    fn default() -> Self {
        Self {
            call_timeout: Duration::from_secs(30),
            max_concurrent_calls: 4,
            max_result_bytes: 1024 * 1024,
            denied_tools: HashSet::new(),
            max_session_bytes: 1024 * 1024 * 1024,
            shutdown_grace: Duration::from_secs(2),
        }
    }
}

#[derive(Debug)]
struct Pending {
    deadline: Instant,
    expired: bool,
}

/// Decision for a client-to-server message.
#[derive(Debug, PartialEq)]
pub enum RequestDecision {
    Forward,
    Reply(Value),
    Drop,
}

/// Decision for a server-to-client message.
#[derive(Debug, PartialEq)]
pub enum ResponseDecision {
    Forward(Value),
    Suppress,
}

/// Tracks tool requests and discovery requests for one MCP session.
#[derive(Debug)]
pub struct McpPolicyState {
    pub policy: McpCallPolicy,
    calls: HashMap<String, Pending>,
    discovery: HashSet<String>,
}

fn key(id: &Value) -> Option<String> {
    if id.is_string() || id.is_i64() || id.is_u64() {
        Some(id.to_string())
    } else {
        None
    }
}

fn error(id: &Value, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":message}})
}

impl McpPolicyState {
    pub fn new(policy: McpCallPolicy) -> Self {
        Self {
            policy,
            calls: HashMap::new(),
            discovery: HashSet::new(),
        }
    }

    /// Inspect a message sent by the client.
    pub fn request(&mut self, message: &Value, now: Instant) -> RequestDecision {
        if message.is_array() {
            return RequestDecision::Drop;
        }
        if message.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
            if let Some(id_key) = message.pointer("/params/requestId").and_then(key) {
                if let Some(call) = self.calls.get_mut(&id_key) {
                    call.expired = true;
                }
            }
            return RequestDecision::Forward;
        }
        if message.get("method").and_then(Value::as_str) == Some("tools/call")
            && message.get("id").and_then(key).is_none()
        {
            return RequestDecision::Drop;
        }
        let Some(id) = message.get("id") else {
            return RequestDecision::Forward;
        };
        let Some(id_key) = key(id) else {
            return RequestDecision::Forward;
        };
        match message.get("method").and_then(Value::as_str) {
            Some("tools/list") => {
                self.discovery.insert(id_key);
            }
            Some("tools/call") => {
                if message.pointer("/params/task").is_some() {
                    return RequestDecision::Reply(error(
                        id,
                        "Task-augmented tool calls are not supported by supervisor policy",
                    ));
                }
                let name = message
                    .pointer("/params/name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if self.policy.denied_tools.contains(name) {
                    return RequestDecision::Reply(error(id, "Tool denied by supervisor policy"));
                }
                if self.calls.contains_key(&id_key) {
                    return RequestDecision::Reply(error(id, "Duplicate active request ID"));
                }
                if self.calls.len() >= self.policy.max_concurrent_calls {
                    return RequestDecision::Reply(error(
                        id,
                        "Supervisor concurrency limit reached",
                    ));
                }
                self.calls.insert(
                    id_key,
                    Pending {
                        deadline: now.checked_add(self.policy.call_timeout).unwrap_or(now),
                        expired: false,
                    },
                );
            }
            _ => {}
        }
        RequestDecision::Forward
    }

    /// Inspect a message sent by the server.
    pub fn response(&mut self, mut message: Value) -> ResponseDecision {
        if message.get("method").is_some() {
            return ResponseDecision::Forward(message);
        }
        let Some(id_key) = message.get("id").and_then(key) else {
            return ResponseDecision::Forward(message);
        };
        if self.discovery.remove(&id_key) {
            if let Some(tools) = message
                .pointer_mut("/result/tools")
                .and_then(Value::as_array_mut)
            {
                tools.retain(|tool| {
                    !tool
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| self.policy.denied_tools.contains(name))
                });
            }
        }
        if let Some(call) = self.calls.remove(&id_key) {
            if call.expired {
                return ResponseDecision::Suppress;
            }
            if let Some(result) = message.get("result") {
                if serde_json::to_vec(result)
                    .map_or(true, |v| v.len() > self.policy.max_result_bytes)
                {
                    let id = message.get("id").cloned().unwrap_or(Value::Null);
                    return ResponseDecision::Forward(error(
                        &id,
                        "Tool result exceeds supervisor max_result_bytes",
                    ));
                }
            }
        }
        ResponseDecision::Forward(message)
    }

    /// Expire calls, returning an error for the client and a cancellation for the server.
    /// Expired calls retain their slot until a response or session termination.
    pub fn expire(&mut self, now: Instant) -> Vec<(Value, Value)> {
        let mut expired = Vec::new();
        for (id_key, call) in &mut self.calls {
            if !call.expired && now >= call.deadline {
                call.expired = true;
                if let Ok(id) = serde_json::from_str::<Value>(id_key) {
                    expired.push((
                        error(&id, "Tool deadline exceeded; cancellation requested"),
                        json!({
                            "jsonrpc":"2.0","method":"notifications/cancelled",
                            "params":{"requestId":id,"reason":"Supervisor deadline exceeded"}
                        }),
                    ));
                }
            }
        }
        expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn denied_tools_are_hidden_and_calls_rejected() {
        let mut config = McpCallPolicy::default();
        config.denied_tools.insert("delete".into());
        let mut state = McpPolicyState::new(config);
        let now = Instant::now();
        assert!(matches!(
            state.request(
                &json!({"id":1,"method":"tools/call","params":{"name":"delete"}}),
                now
            ),
            RequestDecision::Reply(_)
        ));
        assert_eq!(
            state.request(&json!({"id":2,"method":"tools/list"}), now),
            RequestDecision::Forward
        );
        let response =
            state.response(json!({"id":2,"result":{"tools":[{"name":"delete"},{"name":"read"}]}}));
        assert_eq!(
            response,
            ResponseDecision::Forward(json!({"id":2,"result":{"tools":[{"name":"read"}]}}))
        );
    }

    #[test]
    fn limit_timeout_and_late_reply_are_tracked() {
        let mut state = McpPolicyState::new(McpCallPolicy {
            max_concurrent_calls: 1,
            call_timeout: Duration::from_millis(1),
            ..Default::default()
        });
        let now = Instant::now();
        assert_eq!(
            state.request(
                &json!({"id":"a","method":"tools/call","params":{"name":"read"}}),
                now
            ),
            RequestDecision::Forward
        );
        assert!(matches!(
            state.request(
                &json!({"id":"b","method":"tools/call","params":{"name":"read"}}),
                now
            ),
            RequestDecision::Reply(_)
        ));
        assert_eq!(state.expire(now + Duration::from_millis(2)).len(), 1);
        assert_eq!(
            state.response(json!({"id":"a","result":{}})),
            ResponseDecision::Suppress
        );
        assert_eq!(
            state.request(
                &json!({"id":"b","method":"tools/call","params":{"name":"read"}}),
                now
            ),
            RequestDecision::Forward
        );
    }
}
