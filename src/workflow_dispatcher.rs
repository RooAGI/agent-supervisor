//! Supervisor-owned process adapter for the Roo Runtime JSONL worker host.

use crate::service_client::{ServiceProcessRequest, ServiceRole, SupervisorServiceClient};
use crate::workflow::{
    FlowRunWorkflowState, SinkFuture, WorkflowDispatchInput, WorkflowDispatchReceipt,
    WorkflowDispatchRequest, WorkflowDispatcher, WorkflowTask, WorkflowWorkerEvent,
};
use crate::{EnvironmentPolicy, NetworkMode, Policy, ResourceLimits};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const MAX_WORKER_EVENT_BYTES: usize = 1024 * 1024;
const MAX_EVIDENCE_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub(crate) struct RuntimeWorkerConfig {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub environment: EnvironmentPolicy,
}

#[derive(Clone)]
pub(crate) struct RuntimeWorkerDispatcher {
    socket_path: PathBuf,
    control_token: String,
    config: RuntimeWorkerConfig,
    invocations: Arc<Mutex<HashMap<(String, String), Value>>>,
    active_processes: Arc<Mutex<HashMap<String, String>>>,
    cancel_requests: Arc<Mutex<HashSet<String>>>,
}

impl RuntimeWorkerDispatcher {
    pub(crate) fn new(
        socket_path: PathBuf,
        control_token: String,
        config: RuntimeWorkerConfig,
        invocations: Arc<Mutex<HashMap<(String, String), Value>>>,
    ) -> Self {
        Self {
            socket_path,
            control_token,
            config,
            invocations,
            active_processes: Arc::new(Mutex::new(HashMap::new())),
            cancel_requests: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    async fn client(&self) -> Result<SupervisorServiceClient, String> {
        SupervisorServiceClient::connect(
            &self.socket_path,
            self.control_token.clone(),
            ServiceRole::Control,
        )
        .await
        .map_err(|error| error.to_string())
    }

    fn process_for(&self, attempt_id: &str) -> Option<String> {
        self.active_processes
            .lock()
            .unwrap()
            .get(attempt_id)
            .cloned()
    }

    async fn process_for_request(
        &self,
        request: &WorkflowDispatchRequest,
    ) -> Result<String, String> {
        let snapshot = self
            .client()
            .await?
            .snapshot()
            .await
            .map_err(|error| error.to_string())?;
        snapshot
            .processes
            .into_iter()
            .find(|process| {
                process.attempt_id.as_deref() == Some(request.attempt_id.as_str())
                    && process.alive
                    && snapshot.attempts.iter().any(|attempt| {
                        attempt.attempt_id == request.attempt_id
                            && attempt.generation == request.generation
                    })
            })
            .map(|process| process.process_id)
            .ok_or_else(|| {
                "no Supervisor process is associated with this workflow attempt".to_owned()
            })
    }
}

impl WorkflowDispatcher for RuntimeWorkerDispatcher {
    fn resolve_invocation<'a>(
        &'a self,
        flow: &'a FlowRunWorkflowState,
        task: &'a WorkflowTask,
    ) -> SinkFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            let invocation = self
                .invocations
                .lock()
                .unwrap()
                .get(&(flow.flow_id.clone(), task.task_id.clone()))
                .cloned()
                .ok_or_else(|| {
                    "no transient Roo Runtime invocation is registered for this task".to_owned()
                })?;
            let object = invocation
                .as_object()
                .ok_or_else(|| "Roo Runtime invocation must be a JSON object".to_owned())?;
            match object.get("workerKind").and_then(Value::as_str) {
                Some("codex" | "claude") => {
                    if object.get("protocolVersion").and_then(Value::as_u64) != Some(1)
                        || !object
                            .get("providerInvocation")
                            .is_some_and(Value::is_object)
                    {
                        return Err(
                            "provider invocation requires protocolVersion 1 and an object providerInvocation"
                                .to_owned(),
                        );
                    }
                }
                Some(other) => return Err(format!("unsupported workflow worker kind {other}")),
                None => {
                    for key in ["runtimeOptions", "graph", "input"] {
                        if !object.contains_key(key) {
                            return Err(format!("Roo Runtime invocation is missing {key}"));
                        }
                    }
                }
            }
            Ok(invocation)
        })
    }

    fn dispatch<'a>(
        &'a self,
        input: WorkflowDispatchInput,
    ) -> SinkFuture<'a, Result<WorkflowDispatchReceipt, String>> {
        Box::pin(async move {
            let request = input.request;
            let workflow_context = input.workflow_context;
            let mut invocation = input
                .invocation
                .as_object()
                .cloned()
                .ok_or_else(|| "Roo Runtime invocation must be a JSON object".to_owned())?;
            invocation.insert("workflowContext".into(), workflow_context.clone());
            if let Some(input) = invocation.get_mut("input").and_then(Value::as_object_mut) {
                let context = input.entry("context").or_insert_with(|| json!({}));
                if let Some(context) = context.as_object_mut() {
                    let context_text = serde_json::to_string(&workflow_context)
                        .map_err(|error| error.to_string())?;
                    match context.entry("user_input").or_insert_with(|| json!("")) {
                        Value::String(existing) => {
                            if !existing.is_empty() {
                                existing.push_str("\n\n");
                            }
                            existing
                                .push_str("Current FlowRun task and dependency evidence (JSON):\n");
                            existing.push_str(&context_text);
                        }
                        value => {
                            *value =
                                json!({"priorUserInput":value,"flowRunContext":workflow_context});
                        }
                    }
                }
            }
            let runtime_run_id = uuid::Uuid::new_v4().to_string();
            invocation.insert("flowId".into(), json!(request.flow_id));
            invocation.insert("flowRunId".into(), json!(request.flow_id));
            invocation.insert("boardId".into(), json!(request.board_id));
            invocation.insert("taskId".into(), json!(request.task_id));
            invocation.insert("postId".into(), json!(request.post_id));
            invocation.insert(
                "workerId".into(),
                json!(format!("supervisor:{}", request.attempt_id)),
            );
            invocation.insert("idempotencyKey".into(), json!(request.idempotency_key));
            invocation.insert("runtimeRunId".into(), json!(runtime_run_id));
            if let Some(host_context) = invocation
                .get_mut("runtimeOptions")
                .and_then(|options| options.get_mut("localExecution"))
                .and_then(|local| local.get_mut("host"))
                .and_then(|host| host.get_mut("context"))
                .and_then(Value::as_object_mut)
            {
                host_context.insert("runId".into(), json!(runtime_run_id));
                host_context.insert("runtimeRunId".into(), json!(runtime_run_id));
                host_context.insert(
                    "workerId".into(),
                    json!(format!("supervisor:{}", request.attempt_id)),
                );
                host_context.insert("flowRunId".into(), json!(request.flow_id));
                host_context.insert("taskId".into(), json!(request.task_id));
                host_context.insert("postId".into(), json!(request.post_id));
            }
            let project_id = invocation.get("projectId").cloned().unwrap_or(Value::Null);
            let metadata = json!({
                "framework":"rooagiRuntime",
                "runLabel":request.title,
                "projectId":project_id,
                "flowRunId":request.flow_id,
                "taskId":request.task_id,
                "postId":request.post_id,
                "workerId":format!("supervisor:{}", request.attempt_id),
                "runtimeRunId":runtime_run_id,
            });
            let policy = Policy {
                environment: self.config.environment.clone(),
                network: NetworkMode::Host,
                limits: ResourceLimits {
                    timeout_ms: Duration::from_secs(24 * 60 * 60).as_millis() as u64,
                    input_bytes: MAX_WORKER_EVENT_BYTES,
                    output_bytes: MAX_WORKER_EVENT_BYTES,
                    stderr_bytes: 256 * 1024,
                    ..ResourceLimits::default()
                },
                ..Policy::default()
            };
            let client = self.client().await?;
            let process = client
                .start_process(ServiceProcessRequest {
                    executable: self.config.executable.to_string_lossy().into_owned(),
                    args: self.config.args.clone(),
                    cwd: self.config.cwd.clone(),
                    metadata,
                    policy,
                    attempt_id: Some(request.attempt_id.clone()),
                    generation: Some(request.generation),
                })
                .await
                .map_err(|error| error.to_string())?;
            let process_id = process.process_id().to_owned();
            self.active_processes
                .lock()
                .unwrap()
                .insert(request.attempt_id.clone(), process_id.clone());
            let envelope = json!({
                "type":"roo.runtime.worker.start",
                "protocolVersion":1,
                "attemptId":request.attempt_id,
                "generation":request.generation,
                "invocation":invocation,
            });
            let mut bytes = serde_json::to_vec(&envelope).map_err(|error| error.to_string())?;
            bytes.push(b'\n');
            if let Err(error) = process.write_stdin(&bytes).await {
                let _ = process.stop(0).await;
                self.active_processes
                    .lock()
                    .unwrap()
                    .remove(&request.attempt_id);
                return Err(error.to_string());
            }
            Ok(WorkflowDispatchReceipt {
                process_id,
                runtime_run_id,
            })
        })
    }

    fn attach<'a>(
        &'a self,
        request: &'a WorkflowDispatchRequest,
        receipt: Option<&'a WorkflowDispatchReceipt>,
    ) -> SinkFuture<'a, Result<mpsc::Receiver<WorkflowWorkerEvent>, String>> {
        Box::pin(async move {
            let client = self.client().await?;
            let snapshot = client.snapshot().await.map_err(|error| error.to_string())?;
            let expected_process_id = receipt
                .map(|value| value.process_id.clone())
                .or_else(|| self.process_for(&request.attempt_id));
            let process_id = snapshot
                .processes
                .iter()
                .find(|process| {
                    process.attempt_id.as_deref() == Some(request.attempt_id.as_str())
                        && process.alive
                        && expected_process_id
                            .as_deref()
                            .is_none_or(|expected| process.process_id == expected)
                        && snapshot.attempts.iter().any(|attempt| {
                            attempt.attempt_id == request.attempt_id
                                && attempt.generation == request.generation
                        })
                })
                .map(|process| process.process_id.clone())
                .ok_or_else(|| {
                    "worker process is no longer alive or has no persisted attempt association"
                        .to_owned()
                })?;
            let process = client
                .attach_process(&process_id)
                .await
                .map_err(|error| error.to_string())?;
            let (sender, receiver) = mpsc::channel(64);
            let request = request.clone();
            let active = Arc::clone(&self.active_processes);
            let cancel_requests = Arc::clone(&self.cancel_requests);
            let event_client = client.clone();
            let event_process_id = process_id.clone();
            tokio::spawn(async move {
                read_worker_events(
                    process,
                    request,
                    sender,
                    active,
                    cancel_requests,
                    event_client,
                    event_process_id,
                )
                .await;
            });
            Ok(receiver)
        })
    }

    fn resume<'a>(
        &'a self,
        request: &'a WorkflowDispatchRequest,
        input: Value,
    ) -> SinkFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let process_id = self.process_for_request(request).await?;
            let client = self.client().await?;
            let mut bytes = serde_json::to_vec(&json!({"type":"resume","input":input}))
                .map_err(|error| error.to_string())?;
            bytes.push(b'\n');
            client
                .write_stdin(&process_id, &bytes)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn cancel<'a>(
        &'a self,
        request: &'a WorkflowDispatchRequest,
    ) -> SinkFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let process_id = self.process_for_request(request).await?;
            let client = self.client().await?;
            let mut bytes =
                serde_json::to_vec(&json!({"type":"cancel"})).map_err(|error| error.to_string())?;
            bytes.push(b'\n');
            self.cancel_requests
                .lock()
                .unwrap()
                .insert(request.attempt_id.clone());
            if let Err(error) = client.write_stdin(&process_id, &bytes).await {
                self.cancel_requests
                    .lock()
                    .unwrap()
                    .remove(&request.attempt_id);
                return Err(error.to_string());
            }
            Ok(())
        })
    }
}

async fn read_worker_events(
    mut process: crate::service_client::ServiceProcessHandle,
    request: WorkflowDispatchRequest,
    sender: mpsc::Sender<WorkflowWorkerEvent>,
    active_processes: Arc<Mutex<HashMap<String, String>>>,
    cancel_requests: Arc<Mutex<HashSet<String>>>,
    client: SupervisorServiceClient,
    process_id: String,
) {
    let mut pending = Vec::new();
    let mut last_cursor = 0;
    let mut saw_terminal = false;
    loop {
        match process.next_event().await {
            Ok(Some(event)) if event.kind == "stdout" => {
                if let Some(data) = event.data {
                    pending.extend_from_slice(&data);
                    while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                        let line = pending.drain(..=end).collect::<Vec<_>>();
                        if line.len() > MAX_WORKER_EVENT_BYTES {
                            pending.clear();
                            continue;
                        }
                        let Ok(value) = serde_json::from_slice::<Value>(&line[..line.len() - 1])
                        else {
                            continue;
                        };
                        if value.get("type").and_then(Value::as_str)
                            == Some("roo.runtime.worker.approval.requested")
                        {
                            if value.get("protocolVersion").and_then(Value::as_u64) != Some(1)
                                || value.get("attemptId").and_then(Value::as_str)
                                    != Some(request.attempt_id.as_str())
                                || value.get("generation").and_then(Value::as_u64)
                                    != Some(request.generation)
                            {
                                continue;
                            }
                            let Some(approval_request_id) = value
                                .get("requestId")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                            else {
                                continue;
                            };
                            let kind = value
                                .get("kind")
                                .and_then(Value::as_str)
                                .unwrap_or("localExecution")
                                .to_owned();
                            let reason = value
                                .get("reason")
                                .and_then(Value::as_str)
                                .unwrap_or("worker requested approval")
                                .to_owned();
                            let payload = value.get("payload").cloned().unwrap_or(Value::Null);
                            let timeout_ms = value
                                .get("timeoutMs")
                                .and_then(Value::as_u64)
                                .unwrap_or(300_000);
                            let current_attempt = client.snapshot().await.is_ok_and(|snapshot| {
                                snapshot.flow_runs.iter().any(|flow| {
                                    flow.flow_id == request.flow_id
                                        && flow.coordinator_fence == request.coordinator_fence
                                        && flow.tasks.get(&request.task_id).is_some_and(|task| {
                                            task.attempt_id.as_deref()
                                                == Some(request.attempt_id.as_str())
                                                && task.generation == Some(request.generation)
                                                && !task.cancel_requested
                                        })
                                })
                            });
                            if !current_attempt {
                                write_approval_result(
                                    &client,
                                    &process_id,
                                    &approval_request_id,
                                    json!({"decision":"interrupted","reason":"worker attempt is no longer current"}),
                                )
                                .await;
                                continue;
                            }
                            // Persist the request before draining another worker event. The
                            // wait itself runs in the background so stdout and cancellation
                            // handling remain live while Desktop considers the request.
                            if let Err(error) = client
                                .request_worker_approval(
                                    &request.attempt_id,
                                    request.generation,
                                    request.coordinator_fence,
                                    &approval_request_id,
                                    &kind,
                                    &reason,
                                    payload,
                                    timeout_ms,
                                )
                                .await
                            {
                                write_approval_result(
                                    &client,
                                    &process_id,
                                    &approval_request_id,
                                    json!({"decision":"denied","reason":error.to_string()}),
                                )
                                .await;
                                continue;
                            }
                            let client = client.clone();
                            let attempt_id = request.attempt_id.clone();
                            let generation = request.generation;
                            let process_id = process_id.clone();
                            tokio::spawn(async move {
                                let decision = match client
                                    .wait_approval(&attempt_id, generation, &approval_request_id)
                                    .await
                                {
                                    Ok(approval) => json!({
                                        "decision": match approval.state.as_str() {
                                            "approved" => "approved",
                                            "denied" => "denied",
                                            "timedOut" => "timedOut",
                                            "cancelled" => "cancelled",
                                            "interrupted" => "interrupted",
                                            _ => "denied",
                                        },
                                        "grant": approval.grant,
                                        "reason": approval.resolution_reason,
                                        "state": approval.state,
                                    }),
                                    Err(error) => {
                                        json!({"decision":"interrupted","reason":error.to_string()})
                                    }
                                };
                                write_approval_result(
                                    &client,
                                    &process_id,
                                    &approval_request_id,
                                    decision,
                                )
                                .await;
                            });
                            continue;
                        }
                        if value.get("type").and_then(Value::as_str)
                            == Some("roo.runtime.worker.tool.requested")
                        {
                            if value.get("protocolVersion").and_then(Value::as_u64) != Some(1)
                                || value.get("attemptId").and_then(Value::as_str)
                                    != Some(request.attempt_id.as_str())
                                || value.get("generation").and_then(Value::as_u64)
                                    != Some(request.generation)
                            {
                                continue;
                            }
                            let Some(tool_request_id) = value
                                .get("requestId")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                            else {
                                continue;
                            };
                            let tool_name = value
                                .get("toolName")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_owned();
                            let arguments = value.get("arguments").cloned().unwrap_or(Value::Null);
                            let timeout_ms = value
                                .get("timeoutMs")
                                .and_then(Value::as_u64)
                                .unwrap_or(300_000);
                            let current_attempt = client.snapshot().await.is_ok_and(|snapshot| {
                                snapshot.flow_runs.iter().any(|flow| {
                                    flow.flow_id == request.flow_id
                                        && flow.coordinator_fence == request.coordinator_fence
                                        && flow.tasks.get(&request.task_id).is_some_and(|task| {
                                            task.attempt_id.as_deref()
                                                == Some(request.attempt_id.as_str())
                                                && task.generation == Some(request.generation)
                                                && !task.cancel_requested
                                        })
                                })
                            });
                            if !current_attempt {
                                write_tool_result(
                                    &client,
                                    &process_id,
                                    &tool_request_id,
                                    "interrupted",
                                    None,
                                    Some("worker attempt is no longer current"),
                                )
                                .await;
                                continue;
                            }
                            if let Err(error) = client
                                .request_worker_tool(
                                    &request.attempt_id,
                                    request.generation,
                                    request.coordinator_fence,
                                    &tool_request_id,
                                    &tool_name,
                                    arguments,
                                    timeout_ms,
                                )
                                .await
                            {
                                write_tool_result(
                                    &client,
                                    &process_id,
                                    &tool_request_id,
                                    "failed",
                                    None,
                                    Some(&error.to_string()),
                                )
                                .await;
                                continue;
                            }
                            let client = client.clone();
                            let attempt_id = request.attempt_id.clone();
                            let generation = request.generation;
                            let process_id = process_id.clone();
                            tokio::spawn(async move {
                                let response = client
                                    .wait_tool_request(&attempt_id, generation, &tool_request_id)
                                    .await;
                                match response {
                                    Ok(tool_request) => {
                                        write_tool_result(
                                            &client,
                                            &process_id,
                                            &tool_request_id,
                                            &tool_request.state,
                                            tool_request.result,
                                            tool_request.error.as_deref(),
                                        )
                                        .await
                                    }
                                    Err(error) => {
                                        write_tool_result(
                                            &client,
                                            &process_id,
                                            &tool_request_id,
                                            "interrupted",
                                            None,
                                            Some(&error.to_string()),
                                        )
                                        .await
                                    }
                                }
                            });
                            continue;
                        }
                        let Some(status) = value.get("status").and_then(Value::as_str) else {
                            continue;
                        };
                        if !matches!(
                            status,
                            "queued"
                                | "running"
                                | "succeeded"
                                | "failed"
                                | "cancelled"
                                | "blocked"
                                | "unknown"
                        ) {
                            continue;
                        }
                        let Some(source_cursor) = value.get("sourceCursor").and_then(Value::as_u64)
                        else {
                            continue;
                        };
                        if source_cursor <= last_cursor
                            || value.get("attemptId").and_then(Value::as_str)
                                != Some(request.attempt_id.as_str())
                            || value.get("generation").and_then(Value::as_u64)
                                != Some(request.generation)
                        {
                            continue;
                        }
                        last_cursor = source_cursor;
                        saw_terminal =
                            matches!(status, "succeeded" | "failed" | "cancelled" | "blocked");
                        let event = WorkflowWorkerEvent {
                            attempt_id: request.attempt_id.clone(),
                            generation: request.generation,
                            source_cursor,
                            status: status.to_owned(),
                            reason: value
                                .get("reasonCode")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned),
                            retryable: value
                                .get("retryable")
                                .and_then(Value::as_bool)
                                .unwrap_or(false),
                            evidence: value.get("output").map(bounded_evidence),
                        };
                        if sender.send(event).await.is_err() {
                            return;
                        }
                        if saw_terminal {
                            settle_pending_approvals(&client, &request, status == "cancelled")
                                .await;
                            settle_pending_tool_requests(&client, &request, status == "cancelled")
                                .await;
                        }
                    }
                    if pending.len() > MAX_WORKER_EVENT_BYTES {
                        pending.clear();
                    }
                }
            }
            Ok(Some(event)) if event.kind == "exited" => {
                active_processes.lock().unwrap().remove(&request.attempt_id);
                let cancel_requested = cancel_requests.lock().unwrap().remove(&request.attempt_id);
                settle_pending_approvals(&client, &request, cancel_requested).await;
                settle_pending_tool_requests(&client, &request, cancel_requested).await;
                if !saw_terminal {
                    let _ = sender
                        .send(WorkflowWorkerEvent {
                            attempt_id: request.attempt_id.clone(),
                            generation: request.generation,
                            source_cursor: last_cursor.saturating_add(1),
                            status: if cancel_requested {
                                "cancelled"
                            } else {
                                "unknown"
                            }
                            .into(),
                            reason: Some(if cancel_requested {
                                "worker process exited after a cooperative cancellation request"
                                    .into()
                            } else {
                                "worker process exited without a terminal Runtime event".into()
                            }),
                            retryable: false,
                            evidence: None,
                        })
                        .await;
                }
                return;
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return,
        }
    }
}

async fn write_approval_result(
    client: &SupervisorServiceClient,
    process_id: &str,
    request_id: &str,
    decision: Value,
) {
    let response = json!({
        "type":"approval.result",
        "requestId":request_id,
        "decision":decision["decision"],
        "grant":decision.get("grant"),
        "reason":decision.get("reason"),
        "state":decision.get("state"),
    });
    if let Ok(mut bytes) = serde_json::to_vec(&response) {
        bytes.push(b'\n');
        let _ = client.write_stdin(process_id, &bytes).await;
    }
}

async fn settle_pending_approvals(
    client: &SupervisorServiceClient,
    request: &WorkflowDispatchRequest,
    cancel_requested: bool,
) {
    let Ok(snapshot) = client.snapshot().await else {
        return;
    };
    for approval in snapshot.approvals.into_iter().filter(|approval| {
        approval.attempt_id == request.attempt_id
            && approval.generation == request.generation
            && approval.state == "pending"
    }) {
        let _ = client
            .resolve_approval(
                &request.attempt_id,
                request.generation,
                &approval.request_id,
                &format!(
                    "worker-exit:{}:{}:{}",
                    request.attempt_id, request.generation, approval.request_id
                ),
                if cancel_requested {
                    "cancelled"
                } else {
                    "interrupted"
                },
                None,
                request.coordinator_fence,
            )
            .await;
    }
}

async fn settle_pending_tool_requests(
    client: &SupervisorServiceClient,
    request: &WorkflowDispatchRequest,
    cancel_requested: bool,
) {
    let Ok(snapshot) = client.snapshot().await else {
        return;
    };
    for tool in snapshot.tool_requests.into_iter().filter(|tool| {
        tool.attempt_id == request.attempt_id
            && tool.generation == request.generation
            && matches!(tool.state.as_str(), "pending" | "claimed")
    }) {
        let state = if cancel_requested {
            "cancelled"
        } else if tool.state == "claimed" {
            "unknown"
        } else {
            "interrupted"
        };
        let reason = if state == "unknown" {
            "worker exited after a side-effecting tool request was claimed; result is unknown"
        } else if state == "cancelled" {
            "worker attempt was cancelled"
        } else {
            "worker exited before the tool request was completed"
        };
        let _ = client
            .finish_tool_request(
                &request.attempt_id,
                request.generation,
                &tool.request_id,
                state,
                reason,
                request.coordinator_fence,
            )
            .await;
    }
}

async fn write_tool_result(
    client: &SupervisorServiceClient,
    process_id: &str,
    request_id: &str,
    state: &str,
    result: Option<Value>,
    error: Option<&str>,
) {
    let response = json!({"type":"tool.result","requestId":request_id,"result":result,"error":error,"state":state});
    if let Ok(mut bytes) = serde_json::to_vec(&response) {
        bytes.push(b'\n');
        let _ = client.write_stdin(process_id, &bytes).await;
    }
}

fn bounded_evidence(value: &Value) -> Value {
    let redacted = redact_evidence(value);
    let encoded = serde_json::to_vec(&redacted).unwrap_or_default();
    if encoded.len() <= MAX_EVIDENCE_BYTES {
        return redacted;
    }
    let text = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| String::from_utf8_lossy(&encoded).into_owned());
    let mut end = text.len().min(MAX_EVIDENCE_BYTES.saturating_sub(48));
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    json!({"truncated":true,"preview":&text[..end]})
}

fn redact_evidence(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let key_lower = key.to_ascii_lowercase();
                    let sensitive = ["secret", "token", "password", "api_key", "apikey"]
                        .iter()
                        .any(|marker| key_lower.contains(marker));
                    (
                        key.clone(),
                        if sensitive {
                            json!("[REDACTED]")
                        } else {
                            redact_evidence(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_evidence).collect()),
        Value::String(text) => {
            let mut sanitized = text.clone();
            for marker in [
                "Bearer ",
                "api_key=",
                "apiKey=",
                "token=",
                "password=",
                "secret=",
            ] {
                let mut offset = 0;
                while let Some(index) = sanitized[offset..].find(marker) {
                    let start = offset + index + marker.len();
                    let end = sanitized[start..]
                        .find(|character: char| {
                            character.is_whitespace() || matches!(character, '&' | ',' | ';')
                        })
                        .map(|length| start + length)
                        .unwrap_or(sanitized.len());
                    sanitized.replace_range(start..end, "[REDACTED]");
                    offset = start + "[REDACTED]".len();
                    if offset >= sanitized.len() {
                        break;
                    }
                }
            }
            Value::String(sanitized)
        }
        _ => value.clone(),
    }
}
