//! Client for the authenticated Unix Supervisor process service.
//!
//! Process, run, activity, attempt, snapshot, and coordinator event APIs for
//! the Supervisor service. Snapshots and replay cursors are in-memory service
//! state; clients must resnapshot after epoch changes or replay gaps.

// These methods serialize protocol records with distinct fencing and timeout
// fields; keeping the fields explicit makes the wire contract easy to audit.
#![allow(clippy::too_many_arguments)]

use crate::Policy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        unix::{OwnedReadHalf, OwnedWriteHalf},
        UnixStream,
    },
};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ServiceClientError {
    #[error("Supervisor service I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid Supervisor service JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Supervisor service frame exceeds the 16 MiB limit")]
    FrameTooLarge,
    #[error("Supervisor service rejected authentication for role {0}")]
    Unauthorized(String),
    #[error("Supervisor service returned {code}: {message}")]
    Rejected { code: String, message: String },
    #[error("unexpected Supervisor service response: {0}")]
    Unexpected(String),
    #[error("the {role} role cannot {action}")]
    Role { role: String, action: &'static str },
    #[error("Supervisor process event stream disconnected")]
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceRole {
    View,
    Control,
    Tui,
    Reporter,
    Canceller,
    Output,
}

impl ServiceRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::Control => "control",
            Self::Tui => "tui",
            Self::Reporter => "reporter",
            Self::Canceller => "canceller",
            Self::Output => "output",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SupervisorServiceClient {
    socket_path: PathBuf,
    token: String,
    role: ServiceRole,
}

#[derive(Debug, Clone)]
pub struct ServiceProcessRequest {
    pub executable: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    /// Informational process labels such as framework and FlowRun context.
    pub metadata: Value,
    pub policy: Policy,
    pub attempt_id: Option<String>,
    pub generation: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct ServiceSubscriptionFilter {
    pub epoch: Option<String>,
    pub cursor: Option<u64>,
    pub run_id: Option<String>,
    pub activity_id: Option<String>,
    pub attempt_id: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSnapshot {
    pub manager_id: String,
    pub epoch: String,
    pub cursor: u64,
    pub counts: ServiceCounts,
    pub runs: Vec<ServiceRunRecord>,
    pub activities: Vec<ServiceActivityRecord>,
    pub attempts: Vec<ServiceAttemptRecord>,
    pub processes: Vec<ServiceProcessRecord>,
    #[serde(default)]
    pub flow_runs: Vec<ServiceFlowRunRecord>,
    #[serde(default)]
    pub approvals: Vec<ServiceApprovalRequestRecord>,
    #[serde(default)]
    pub tool_requests: Vec<ServiceWorkerToolRequestRecord>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceWorkerToolRequestRecord {
    pub attempt_id: String,
    pub generation: u64,
    pub request_id: String,
    pub flow_id: String,
    pub task_id: String,
    pub post_id: String,
    pub coordinator_fence: u64,
    pub tool_name: String,
    pub arguments: Option<Value>,
    pub state: String,
    pub requested_at_ms: u64,
    pub deadline_at_ms: u64,
    pub claim_id: Option<String>,
    pub resolution_id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceApprovalRequestRecord {
    pub attempt_id: String,
    pub generation: u64,
    pub request_id: String,
    pub flow_id: String,
    pub task_id: String,
    pub post_id: String,
    pub kind: String,
    pub reason: String,
    pub payload: Option<Value>,
    pub state: String,
    pub requested_at_ms: u64,
    pub deadline_at_ms: u64,
    pub resolution_id: Option<String>,
    pub grant: Option<String>,
    pub resolution_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceFlowTaskProjection {
    pub task_id: String,
    pub post_id: String,
    pub title: String,
    #[serde(default)]
    pub stage_id: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub assignee: Option<String>,
    pub state: String,
    pub waiting_reason: Option<String>,
    pub attempt_id: Option<String>,
    pub generation: Option<u64>,
    #[serde(default)]
    pub last_source_cursor: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceFlowRunProjection {
    pub flow_id: String,
    pub board_id: String,
    pub flow_revision: u64,
    #[serde(default)]
    pub current_stage: Option<String>,
    #[serde(default)]
    pub stage_transitions: BTreeMap<String, Vec<String>>,
    #[serde(default = "default_flow_max_attempts")]
    pub max_attempts: u32,
    pub tasks: Vec<ServiceFlowTaskProjection>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceFlowTaskRecord {
    pub task_id: String,
    pub post_id: String,
    pub title: String,
    pub state: String,
    pub waiting_reason: Option<String>,
    pub attempt_id: Option<String>,
    pub generation: Option<u64>,
    pub last_source_cursor: u64,
    pub attempts_started: u32,
    pub dispatch_id: Option<String>,
    pub dispatch_pending: bool,
    #[serde(default)]
    pub cancel_requested: bool,
    pub runtime_run_id: Option<String>,
    pub process_id: Option<String>,
    #[serde(default)]
    pub last_outcome: Option<ServiceFlowAttemptOutcome>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceFlowRunRecord {
    pub flow_id: String,
    pub board_id: String,
    pub coordinator_id: String,
    pub coordinator_fence: u64,
    pub flow_revision: u64,
    #[serde(default)]
    pub current_stage: Option<String>,
    #[serde(default)]
    pub stage_transitions: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub waiting_reason: Option<String>,
    #[serde(default)]
    pub completed: bool,
    pub max_attempts: u32,
    pub source_cursor: u64,
    pub tasks: BTreeMap<String, ServiceFlowTaskRecord>,
    pub pending_sync: bool,
    pub sync_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceFlowAttemptOutcome {
    pub attempt_id: String,
    pub generation: u64,
    pub source_cursor: u64,
    pub status: ExecutionStatus,
    pub reason: Option<String>,
    #[serde(default)]
    pub workflow_cancel_requested: bool,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub evidence: Option<serde_json::Value>,
}

fn default_flow_max_attempts() -> u32 {
    3
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceCounts {
    pub runs: usize,
    pub active_runs: usize,
    pub activities: usize,
    pub active_activities: usize,
    pub attempts: usize,
    pub active_attempts: usize,
    pub processes: usize,
    pub active_processes: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRunRecord {
    pub run_id: String,
    pub manager_id: String,
    pub status: ExecutionStatus,
    pub context: Value,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceActivityRecord {
    pub activity_id: String,
    pub run_id: String,
    pub kind: String,
    pub status: ActivityStatus,
    pub health: ServiceHealth,
    pub last_contact_at_ms: Option<u64>,
    pub waiting_reason: Option<String>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAttemptReport {
    pub status: ExecutionStatus,
    pub detail: Value,
    pub revision: u64,
    pub reported_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAttemptRecord {
    pub attempt_id: String,
    pub activity_id: String,
    pub run_id: String,
    pub generation: u64,
    pub status: ExecutionStatus,
    pub cancellation: CancellationState,
    pub process_ids: Vec<String>,
    pub reports: Vec<ServiceAttemptReport>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceProcessRecord {
    pub process_id: String,
    pub run_id: Option<String>,
    pub attempt_id: Option<String>,
    pub alive: bool,
    pub exit_status: Option<String>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ActivityStatus {
    Queued,
    Running,
    WaitingForInput,
    WaitingForApproval,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ServiceHealth {
    Unknown,
    Healthy,
    Stale,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CancellationState {
    None,
    Requested,
    Delivered,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct RegisteredAttempt {
    pub attempt_id: String,
    pub generation: u64,
    pub reporter_token: String,
    pub cancel_token: String,
}

impl std::fmt::Debug for RegisteredAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredAttempt")
            .field("attempt_id", &self.attempt_id)
            .field("generation", &self.generation)
            .field("reporter_token", &"[REDACTED]")
            .field("cancel_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceCoordinatorEvent {
    pub sequence: u64,
    pub cursor: u64,
    pub epoch: String,
    pub kind: String,
    pub run_id: Option<String>,
    pub activity_id: Option<String>,
    pub attempt_id: Option<String>,
    pub process_id: Option<String>,
    pub revision: u64,
    pub occurred_at_ms: u64,
    pub detail: Value,
}

#[derive(Debug, Clone)]
pub enum ServiceSubscriptionMessage {
    Event(ServiceCoordinatorEvent),
    ResyncRequired {
        epoch: String,
        cursor: u64,
        snapshot: Option<ServiceSnapshot>,
    },
}

#[derive(Debug, Clone)]
pub struct ServiceSubscriptionStart {
    pub epoch: String,
    pub cursor: u64,
    pub resync_required: bool,
    pub events: Vec<ServiceCoordinatorEvent>,
    pub snapshot: Option<ServiceSnapshot>,
}

#[derive(Debug, Clone)]
pub struct ServiceProcessEvent {
    pub kind: String,
    pub process_id: String,
    /// Decoded bytes for stdout/stderr events, otherwise `None`.
    pub data: Option<Vec<u8>>,
    /// Original response frame for fields such as pid, receipt, metadata or actor.
    pub payload: Value,
}

struct Connection {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl SupervisorServiceClient {
    /// Authenticate to the Unix service with one scoped role token.
    pub async fn connect(
        socket_path: impl AsRef<Path>,
        token: impl Into<String>,
        role: ServiceRole,
    ) -> Result<Self, ServiceClientError> {
        let client = Self {
            socket_path: socket_path.as_ref().to_owned(),
            token: token.into(),
            role,
        };
        client.open().await?;
        Ok(client)
    }

    /// Read a role token from a private file and connect to the service.
    #[cfg(unix)]
    pub async fn connect_from_token_file(
        socket_path: impl AsRef<Path>,
        token_path: impl AsRef<Path>,
        role: ServiceRole,
    ) -> Result<Self, ServiceClientError> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let path = token_path.as_ref();
        let metadata = std::fs::symlink_metadata(path)?;
        // SAFETY: geteuid has no preconditions and returns the effective UID.
        let uid = unsafe { libc::geteuid() };
        if !metadata.file_type().is_file()
            || metadata.uid() != uid
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.len() > 4096
        {
            return Err(ServiceClientError::Unexpected(
                "role token file must be a regular file owned by the current user with mode 0600"
                    .into(),
            ));
        }
        let token = std::fs::read_to_string(path)?.trim().to_owned();
        Self::connect(socket_path, token, role).await
    }

    pub fn role(&self) -> ServiceRole {
        self.role
    }

    /// Return one typed coordinator/process snapshot and its replay cursor.
    pub async fn snapshot(&self) -> Result<ServiceSnapshot, ServiceClientError> {
        if !matches!(
            self.role,
            ServiceRole::View | ServiceRole::Control | ServiceRole::Tui
        ) {
            return Err(self.role_error("read metadata snapshots"));
        }
        let response = self.request(json!({ "type": "snapshot" })).await?;
        serde_json::from_value(response).map_err(ServiceClientError::from)
    }

    pub async fn register_run(&self, context: Value) -> Result<String, ServiceClientError> {
        self.require(ServiceRole::Control, "register runs")?;
        let response = self
            .request(json!({ "type": "registerRun", "context": context }))
            .await?;
        response_string_or_record(&response, "runId", "run")
    }

    pub async fn register_activity(
        &self,
        run_id: &str,
        kind: &str,
    ) -> Result<String, ServiceClientError> {
        self.require(ServiceRole::Control, "register activities")?;
        let response = self
            .request(json!({
                "type": "registerActivity", "runId": run_id, "kind": kind
            }))
            .await?;
        response_string_or_record(&response, "activityId", "activity")
    }

    pub async fn register_attempt(
        &self,
        activity_id: &str,
    ) -> Result<RegisteredAttempt, ServiceClientError> {
        self.require(ServiceRole::Control, "register attempts")?;
        let response = self
            .request(json!({ "type": "registerAttempt", "activityId": activity_id }))
            .await?;
        let record = response.get("attempt").unwrap_or(&response);
        Ok(RegisteredAttempt {
            attempt_id: response_string_or_record(&response, "attemptId", "attempt")?,
            generation: response
                .get("generation")
                .or_else(|| record.get("generation"))
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    ServiceClientError::Unexpected("response omitted generation".into())
                })?,
            reporter_token: response_string(&response, "reporterToken")?,
            cancel_token: response_string(&response, "cancelToken")?,
        })
    }

    pub async fn set_activity_status(
        &self,
        activity_id: &str,
        status: ActivityStatus,
        reason: Option<&str>,
    ) -> Result<ServiceActivityRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "report activity status")?;
        let mut request = json!({
            "type":"setActivityStatus", "activityId":activity_id, "status":status
        });
        if let Some(reason) = reason {
            request["reason"] = json!(reason);
        }
        let response = self.request(request).await?;
        serde_json::from_value(response["activity"].clone()).map_err(ServiceClientError::from)
    }

    pub async fn set_waiting(
        &self,
        activity_id: &str,
        reason: Option<&str>,
        status: ActivityStatus,
    ) -> Result<ServiceActivityRecord, ServiceClientError> {
        if !matches!(
            status,
            ActivityStatus::WaitingForInput | ActivityStatus::WaitingForApproval
        ) {
            return Err(ServiceClientError::Unexpected(
                "waiting status must be WaitingForInput or WaitingForApproval".into(),
            ));
        }
        self.set_activity_status(activity_id, status, reason).await
    }

    pub async fn set_run_status(
        &self,
        run_id: &str,
        status: ExecutionStatus,
    ) -> Result<ServiceRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "report run status")?;
        let response = self
            .request(json!({"type":"setRunStatus","runId":run_id,"status":status}))
            .await?;
        serde_json::from_value(response["run"].clone()).map_err(ServiceClientError::from)
    }

    pub async fn set_activity_timeout(
        &self,
        activity_id: &str,
        timeout_ms: u64,
    ) -> Result<(), ServiceClientError> {
        self.require(ServiceRole::Control, "set activity timeout")?;
        if !(1..=86_400_000).contains(&timeout_ms) {
            return Err(ServiceClientError::Unexpected(
                "timeout must be between 1 and 86400000 milliseconds".into(),
            ));
        }
        self.request(json!({
            "type":"setActivityTimeout","activityId":activity_id,"timeoutMs":timeout_ms
        }))
        .await?;
        Ok(())
    }

    pub async fn register_flow_run(
        &self,
        projection: ServiceFlowRunProjection,
        coordinator_id: &str,
        invocations: Option<Value>,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "register FlowRuns")?;
        let mut frame = json!({
            "type":"registerFlowRun",
            "projection":projection,
            "coordinatorId":coordinator_id,
        });
        if let Some(invocations) = invocations {
            frame["invocations"] = invocations;
        }
        flow_run_response(self.request(frame).await?)
    }

    pub async fn update_flow_invocations(
        &self,
        flow_id: &str,
        invocations: Value,
        fence: u64,
    ) -> Result<usize, ServiceClientError> {
        self.require(ServiceRole::Control, "update FlowRun invocations")?;
        let response = self
            .request(
                json!({"type":"updateFlowInvocations","flowId":flow_id,"invocations":invocations,"fence":fence}),
            )
            .await?;
        response
            .get("count")
            .and_then(Value::as_u64)
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                ServiceClientError::Unexpected("response omitted invocation count".into())
            })
    }

    pub async fn claim_flow_run(
        &self,
        flow_id: &str,
        coordinator_id: &str,
        expected_fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "claim FlowRuns")?;
        flow_run_response(
            self.request(json!({
                "type":"claimFlowRun","flowId":flow_id,
                "coordinatorId":coordinator_id,"expectedFence":expected_fence,
            }))
            .await?,
        )
    }

    pub async fn advance_flow_stage(
        &self,
        flow_id: &str,
        expected_stage: &str,
        next_stage: &str,
        expected_flow_revision: u64,
        fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "advance FlowRun stages")?;
        flow_run_response(
            self.request(json!({
                "type":"advanceFlowStage","flowId":flow_id,
                "expectedStage":expected_stage,"nextStage":next_stage,
                "expectedFlowRevision":expected_flow_revision,"fence":fence,
            }))
            .await?,
        )
    }

    pub async fn complete_flow_run(
        &self,
        flow_id: &str,
        expected_stage: &str,
        expected_flow_revision: u64,
        evidence: Value,
        fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "complete FlowRuns")?;
        flow_run_response(
            self.request(json!({
                "type":"completeFlowRun","flowId":flow_id,
                "expectedStage":expected_stage,"expectedFlowRevision":expected_flow_revision,
                "evidence":evidence,"fence":fence,
            }))
            .await?,
        )
    }

    pub async fn request_worker_approval(
        &self,
        attempt_id: &str,
        generation: u64,
        fence: u64,
        approval_request_id: &str,
        kind: &str,
        reason: &str,
        payload: Value,
        timeout_ms: u64,
    ) -> Result<ServiceApprovalRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "submit worker approval requests")?;
        let response = self
            .request(json!({
                "type":"requestWorkerApproval","attemptId":attempt_id,
                "generation":generation,"fence":fence,"approvalRequestId":approval_request_id,
                "kind":kind,"reason":reason,"payload":payload,"timeoutMs":timeout_ms,
            }))
            .await?;
        serde_json::from_value(response.get("approval").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn wait_approval(
        &self,
        attempt_id: &str,
        generation: u64,
        approval_request_id: &str,
    ) -> Result<ServiceApprovalRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "wait for worker approval")?;
        let response = self
            .request(json!({
                "type":"waitApproval","attemptId":attempt_id,
                "generation":generation,"approvalRequestId":approval_request_id,
            }))
            .await?;
        serde_json::from_value(response.get("approval").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn resolve_approval(
        &self,
        attempt_id: &str,
        generation: u64,
        approval_request_id: &str,
        resolution_id: &str,
        decision: &str,
        grant: Option<&str>,
        fence: u64,
    ) -> Result<ServiceApprovalRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "resolve worker approvals")?;
        let response = self
            .request(json!({
                "type":"resolveApproval","attemptId":attempt_id,"generation":generation,
                "approvalRequestId":approval_request_id,"resolutionId":resolution_id,
                "decision":decision,"grant":grant,"fence":fence,
            }))
            .await?;
        serde_json::from_value(response.get("approval").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn request_worker_tool(
        &self,
        attempt_id: &str,
        generation: u64,
        fence: u64,
        request_id: &str,
        tool_name: &str,
        arguments: Value,
        timeout_ms: u64,
    ) -> Result<ServiceWorkerToolRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "submit worker tool requests")?;
        let response = self
            .request(json!({
                "type":"requestWorkerTool","attemptId":attempt_id,"generation":generation,
                "fence":fence,"toolRequestId":request_id,"toolName":tool_name,
                "arguments":arguments,"timeoutMs":timeout_ms,
            }))
            .await?;
        serde_json::from_value(response.get("toolRequest").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn list_pending_tool_requests(
        &self,
    ) -> Result<Vec<ServiceWorkerToolRequestRecord>, ServiceClientError> {
        self.require(ServiceRole::Control, "list worker tool requests")?;
        let response = self
            .request(json!({"type":"listPendingToolRequests"}))
            .await?;
        serde_json::from_value(response.get("toolRequests").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn claim_tool_request(
        &self,
        attempt_id: &str,
        generation: u64,
        request_id: &str,
        claim_id: &str,
        fence: u64,
    ) -> Result<ServiceWorkerToolRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "claim worker tool requests")?;
        let response = self
            .request(json!({
                "type":"claimToolRequest","attemptId":attempt_id,"generation":generation,
                "toolRequestId":request_id,"claimId":claim_id,"fence":fence,
            }))
            .await?;
        serde_json::from_value(response.get("toolRequest").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn wait_tool_request(
        &self,
        attempt_id: &str,
        generation: u64,
        request_id: &str,
    ) -> Result<ServiceWorkerToolRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "wait for worker tool requests")?;
        let response = self.request(json!({
            "type":"waitToolRequest","attemptId":attempt_id,"generation":generation,"toolRequestId":request_id,
        })).await?;
        serde_json::from_value(response.get("toolRequest").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn resolve_tool_request(
        &self,
        attempt_id: &str,
        generation: u64,
        request_id: &str,
        claim_id: &str,
        resolution_id: &str,
        result: Option<Value>,
        error: Option<&str>,
        fence: u64,
    ) -> Result<ServiceWorkerToolRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "resolve worker tool requests")?;
        let response = self
            .request(json!({
                "type":"resolveToolRequest","attemptId":attempt_id,"generation":generation,
                "toolRequestId":request_id,"claimId":claim_id,"resolutionId":resolution_id,
                "result":result,"error":error,"fence":fence,
            }))
            .await?;
        serde_json::from_value(response.get("toolRequest").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn finish_tool_request(
        &self,
        attempt_id: &str,
        generation: u64,
        request_id: &str,
        state: &str,
        error: &str,
        fence: u64,
    ) -> Result<ServiceWorkerToolRequestRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "finish worker tool requests")?;
        let response = self
            .request(json!({
                "type":"finishToolRequest","attemptId":attempt_id,"generation":generation,
                "toolRequestId":request_id,"state":state,"error":error,"fence":fence,
            }))
            .await?;
        serde_json::from_value(response.get("toolRequest").cloned().unwrap_or(Value::Null))
            .map_err(ServiceClientError::from)
    }

    pub async fn bind_flow_attempt(
        &self,
        flow_id: &str,
        task_id: &str,
        attempt_id: &str,
        generation: u64,
        source_cursor: u64,
        fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "bind FlowRun attempts")?;
        flow_run_response(
            self.request(json!({
                "type":"bindFlowAttempt","flowId":flow_id,"taskId":task_id,
                "attemptId":attempt_id,"generation":generation,
                "sourceCursor":source_cursor,"fence":fence,
            }))
            .await?,
        )
    }

    pub async fn reconcile_flow_attempt(
        &self,
        flow_id: &str,
        task_id: &str,
        outcome: ServiceFlowAttemptOutcome,
        fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "reconcile FlowRun attempts")?;
        flow_run_response(
            self.request(json!({
                "type":"reconcileFlowAttempt","flowId":flow_id,
                "taskId":task_id,"outcome":outcome,"fence":fence,
            }))
            .await?,
        )
    }

    pub async fn apply_flow_review(
        &self,
        flow_id: &str,
        task_id: &str,
        flow_revision: u64,
        evidence: Value,
        fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "review FlowRun tasks")?;
        flow_run_response(
            self.request(json!({
                "type":"applyFlowReview","flowId":flow_id,"taskId":task_id,
                "flowRevision":flow_revision,"evidence":evidence,"fence":fence,
            }))
            .await?,
        )
    }

    pub async fn cancel_flow_task(
        &self,
        flow_id: &str,
        task_id: &str,
        attempt_id: &str,
        generation: u64,
        fence: u64,
    ) -> Result<ServiceFlowRunRecord, ServiceClientError> {
        self.require(ServiceRole::Control, "cancel FlowRun tasks")?;
        flow_run_response(
            self.request(json!({
                "type":"cancelFlowTask","flowId":flow_id,"taskId":task_id,
                "attemptId":attempt_id,"generation":generation,"fence":fence,
            }))
            .await?,
        )
    }

    pub async fn report_attempt(
        &self,
        attempt: &RegisteredAttempt,
        status: ExecutionStatus,
        detail: Value,
    ) -> Result<u64, ServiceClientError> {
        let reporter = Self::connect(
            &self.socket_path,
            attempt.reporter_token.clone(),
            ServiceRole::Reporter,
        )
        .await?;
        let response = reporter
            .request(json!({
                "type": "report", "attemptId": attempt.attempt_id,
                "generation": attempt.generation, "status": status, "detail": detail
            }))
            .await?;
        response
            .get("revision")
            .or_else(|| {
                response
                    .get("attempt")
                    .and_then(|attempt| attempt.get("revision"))
            })
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                ServiceClientError::Unexpected("report response omitted revision".into())
            })
    }

    pub async fn poll_attempt(
        &self,
        attempt: &RegisteredAttempt,
    ) -> Result<ServiceAttemptRecord, ServiceClientError> {
        let reporter = Self::connect(
            &self.socket_path,
            attempt.reporter_token.clone(),
            ServiceRole::Reporter,
        )
        .await?;
        let response = reporter.request(json!({
            "type": "pollAttempt", "attemptId": attempt.attempt_id, "generation": attempt.generation
        })).await?;
        serde_json::from_value(response.get("attempt").cloned().ok_or_else(|| {
            ServiceClientError::Unexpected("poll response omitted attempt".into())
        })?)
        .map_err(ServiceClientError::from)
    }

    pub async fn cancel_attempt(
        &self,
        attempt: &RegisteredAttempt,
    ) -> Result<CancellationState, ServiceClientError> {
        let canceller = Self::connect(
            &self.socket_path,
            attempt.cancel_token.clone(),
            ServiceRole::Canceller,
        )
        .await?;
        let response = canceller.request(json!({
            "type": "cancelAttempt", "attemptId": attempt.attempt_id, "generation": attempt.generation
        })).await?;
        let value = response
            .get("cancellation")
            .or_else(|| response.get("cancelState"))
            .or_else(|| {
                response
                    .get("attempt")
                    .and_then(|attempt| attempt.get("cancellation"))
            })
            .or_else(|| {
                response
                    .get("attempt")
                    .and_then(|attempt| attempt.get("cancelState"))
            })
            .cloned()
            .ok_or_else(|| {
                ServiceClientError::Unexpected("cancel response omitted cancellation state".into())
            })?;
        serde_json::from_value(value).map_err(ServiceClientError::from)
    }

    pub async fn subscribe(
        &self,
        filter: ServiceSubscriptionFilter,
    ) -> Result<ServiceSubscription, ServiceClientError> {
        if !matches!(
            self.role,
            ServiceRole::View | ServiceRole::Control | ServiceRole::Tui
        ) {
            return Err(self.role_error("subscribe to coordinator events"));
        }
        let mut connection = self.open().await?;
        let id = request_id();
        let mut request = json!({ "type": "subscribe", "requestId": id });
        for (key, value) in [
            ("epoch", filter.epoch.map(Value::from)),
            ("cursor", filter.cursor.map(Value::from)),
            ("runId", filter.run_id.map(Value::from)),
            ("activityId", filter.activity_id.map(Value::from)),
            ("attemptId", filter.attempt_id.map(Value::from)),
        ] {
            if let Some(value) = value {
                request[key] = value;
            }
        }
        write_json(&mut connection.writer, &request).await?;
        let response = read_response(&mut connection.reader, &id).await?;
        let kind = response.get("type").and_then(Value::as_str).unwrap_or("");
        if kind != "subscriptionReady" && kind != "subscribed" && kind != "resyncRequired" {
            return Err(ServiceClientError::Unexpected(format!(
                "unexpected subscription response {kind}"
            )));
        }
        let start = ServiceSubscriptionStart {
            epoch: response_string(&response, "epoch")?,
            cursor: response.get("cursor").and_then(Value::as_u64).unwrap_or(0),
            resync_required: kind == "resyncRequired"
                || response.get("resyncRequired").and_then(Value::as_bool) == Some(true),
            events: response
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(serde_json::from_value)
                .collect::<Result<_, _>>()?,
            snapshot: response
                .get("snapshot")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
        };
        Ok(ServiceSubscription { start, connection })
    }

    /// Start a service-owned process and return its live event stream.
    pub async fn start_process(
        &self,
        request: ServiceProcessRequest,
    ) -> Result<ServiceProcessHandle, ServiceClientError> {
        self.require(ServiceRole::Control, "start processes")?;
        let mut connection = self.open().await?;
        let request_id = request_id();
        let mut frame = json!({
            "type": "start",
            "requestId": request_id,
            "command": {
                "executable": request.executable,
                "args": request.args,
                "cwd": request.cwd.map(|p| p.to_string_lossy().into_owned()),
            },
            "metadata": request.metadata,
            "policy": serde_json::to_value(request.policy)?,
        });
        match (request.attempt_id, request.generation) {
            (Some(attempt_id), Some(generation)) => {
                frame["attemptId"] = json!(attempt_id);
                frame["generation"] = json!(generation);
            }
            (None, None) => {}
            _ => {
                return Err(ServiceClientError::Unexpected(
                    "attemptId and generation must be provided together".into(),
                ))
            }
        }
        write_json(&mut connection.writer, &frame).await?;
        let response = read_response(&mut connection.reader, &request_id).await?;
        let process_id = response_string(&response, "processId")?;
        let output_token = response_string(&response, "outputToken")?;
        Ok(ServiceProcessHandle {
            client: self.clone(),
            process_id,
            output_token,
            connection,
        })
    }

    /// Attach to a managed process. The service replays its retained process
    /// event history, then streams new events. If this connection drops, call
    /// `attach_process` again to resynchronize from retained history.
    pub async fn attach_process(
        &self,
        process_id: &str,
    ) -> Result<ServiceProcessHandle, ServiceClientError> {
        if !matches!(
            self.role,
            ServiceRole::Control | ServiceRole::Tui | ServiceRole::Output
        ) {
            return Err(self.role_error("attach to process output"));
        }
        let mut connection = self.open().await?;
        let request_id = request_id();
        write_json(
            &mut connection.writer,
            &json!({
                "type": "attach", "requestId": request_id, "processId": process_id
            }),
        )
        .await?;
        let response = read_response(&mut connection.reader, &request_id).await?;
        let attached_id = response_string(&response, "processId")?;
        if attached_id != process_id {
            return Err(ServiceClientError::Unexpected(
                "attached process ID does not match the requested ID".into(),
            ));
        }
        Ok(ServiceProcessHandle {
            client: self.clone(),
            process_id: attached_id,
            output_token: String::new(),
            connection,
        })
    }

    /// Request a targeted stop. A successful response means the stop request
    /// was accepted; observe the process stream for its final exit receipt.
    pub async fn stop_process(
        &self,
        process_id: &str,
        grace_ms: u64,
    ) -> Result<Value, ServiceClientError> {
        if self.role != ServiceRole::Control && self.role != ServiceRole::Tui {
            return Err(self.role_error("stop processes"));
        }
        self.request(json!({ "type": "stop", "processId": process_id, "graceMs": grace_ms }))
            .await
    }

    pub async fn write_stdin(
        &self,
        process_id: &str,
        bytes: &[u8],
    ) -> Result<(), ServiceClientError> {
        self.require(ServiceRole::Control, "write process stdin")?;
        let response = self
            .request(json!({
                "type": "stdin", "processId": process_id, "data": base64_encode(bytes)
            }))
            .await?;
        if response.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(ServiceClientError::Unexpected(
                "service rejected process stdin".into(),
            ))
        }
    }

    pub async fn close_stdin(&self, process_id: &str) -> Result<(), ServiceClientError> {
        self.require(ServiceRole::Control, "close process stdin")?;
        let response = self
            .request(json!({ "type": "closeStdin", "processId": process_id }))
            .await?;
        if response.get("ok").and_then(Value::as_bool) == Some(true) {
            Ok(())
        } else {
            Err(ServiceClientError::Unexpected(
                "service rejected stdin close".into(),
            ))
        }
    }

    /// Attach and wait until the service emits the process exit receipt.
    pub async fn wait_process(&self, process_id: &str) -> Result<Value, ServiceClientError> {
        let mut stream = self.attach_process(process_id).await?;
        while let Some(event) = stream.next_event().await? {
            if event.kind == "exited" {
                return Ok(event.payload.get("receipt").cloned().unwrap_or(Value::Null));
            }
        }
        Err(ServiceClientError::Disconnected)
    }

    async fn request(&self, mut frame: Value) -> Result<Value, ServiceClientError> {
        let mut connection = self.open().await?;
        let id = request_id();
        frame["requestId"] = json!(id);
        write_json(&mut connection.writer, &frame).await?;
        read_response(&mut connection.reader, &id).await
    }

    async fn open(&self) -> Result<Connection, ServiceClientError> {
        if self.token.len() < 32 {
            return Err(ServiceClientError::Unauthorized(self.role.as_str().into()));
        }
        let stream = UnixStream::connect(&self.socket_path).await?;
        let (read, mut writer) = stream.into_split();
        let mut reader = BufReader::new(read);
        write_json(
            &mut writer,
            &json!({
                "type": "auth", "role": self.role.as_str(), "token": self.token
            }),
        )
        .await?;
        let ready = read_json_line(&mut reader)
            .await?
            .ok_or(ServiceClientError::Disconnected)?;
        if ready.get("type").and_then(Value::as_str) == Some("error") {
            return Err(ServiceClientError::Unauthorized(self.role.as_str().into()));
        }
        if ready.get("type").and_then(Value::as_str) != Some("ready")
            || ready.get("role").and_then(Value::as_str) != Some(self.role.as_str())
        {
            return Err(ServiceClientError::Unauthorized(self.role.as_str().into()));
        }
        Ok(Connection { reader, writer })
    }

    fn require(&self, role: ServiceRole, action: &'static str) -> Result<(), ServiceClientError> {
        if self.role == role {
            Ok(())
        } else {
            Err(self.role_error(action))
        }
    }

    fn role_error(&self, action: &'static str) -> ServiceClientError {
        ServiceClientError::Role {
            role: self.role.as_str().into(),
            action,
        }
    }
}

pub struct ServiceProcessHandle {
    client: SupervisorServiceClient,
    process_id: String,
    output_token: String,
    connection: Connection,
}

pub struct ServiceSubscription {
    pub start: ServiceSubscriptionStart,
    connection: Connection,
}

impl ServiceSubscription {
    /// Receive the next coordinator event or an explicit resync instruction.
    /// On `ResyncRequired`, read its snapshot and reconnect from the returned
    /// epoch/cursor after processing it.
    pub async fn next_message(
        &mut self,
    ) -> Result<Option<ServiceSubscriptionMessage>, ServiceClientError> {
        let Some(value) = read_json_line(&mut self.connection.reader).await? else {
            return Err(ServiceClientError::Disconnected);
        };
        match value.get("type").and_then(Value::as_str) {
            Some("coordinatorEvent") => {
                let event = value.get("event").cloned().unwrap_or(value);
                Ok(Some(ServiceSubscriptionMessage::Event(
                    serde_json::from_value(event)?,
                )))
            }
            Some("resyncRequired") => Ok(Some(ServiceSubscriptionMessage::ResyncRequired {
                epoch: response_string(&value, "epoch")?,
                cursor: value.get("cursor").and_then(Value::as_u64).unwrap_or(0),
                snapshot: value
                    .get("snapshot")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?,
            })),
            Some("error") => Err(service_error(value)),
            Some(kind) => Err(ServiceClientError::Unexpected(format!(
                "unexpected coordinator subscription frame {kind}"
            ))),
            None => Err(ServiceClientError::Unexpected(
                "coordinator subscription frame omitted type".into(),
            )),
        }
    }
}

impl ServiceProcessHandle {
    pub fn process_id(&self) -> &str {
        &self.process_id
    }

    /// Per-process output capability returned by `start_process`. Attached
    /// handles do not receive a new capability.
    pub fn output_token(&self) -> Option<&str> {
        (!self.output_token.is_empty()).then_some(self.output_token.as_str())
    }

    pub async fn next_event(&mut self) -> Result<Option<ServiceProcessEvent>, ServiceClientError> {
        let Some(payload) = read_json_line(&mut self.connection.reader).await? else {
            return Err(ServiceClientError::Disconnected);
        };
        if payload.get("type").and_then(Value::as_str) == Some("error") {
            return Err(service_error(payload));
        }
        let kind = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let process_id = payload
            .get("processId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        if process_id != self.process_id {
            return Err(ServiceClientError::Unexpected(
                "event belongs to another process".into(),
            ));
        }
        let data = if matches!(kind.as_str(), "stdout" | "stderr") {
            Some(
                base64_decode(payload.get("data").and_then(Value::as_str).unwrap_or(""))
                    .map_err(ServiceClientError::Unexpected)?,
            )
        } else {
            None
        };
        Ok(Some(ServiceProcessEvent {
            kind,
            process_id,
            data,
            payload,
        }))
    }

    pub async fn stop(&self, grace_ms: u64) -> Result<Value, ServiceClientError> {
        self.client.stop_process(&self.process_id, grace_ms).await
    }

    pub async fn write_stdin(&self, bytes: &[u8]) -> Result<(), ServiceClientError> {
        self.client.write_stdin(&self.process_id, bytes).await
    }

    pub async fn close_stdin(&self) -> Result<(), ServiceClientError> {
        self.client.close_stdin(&self.process_id).await
    }
}

fn request_id() -> String {
    format!("rust-client-{}", uuid::Uuid::new_v4())
}

async fn read_response<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    expected_id: &str,
) -> Result<Value, ServiceClientError> {
    loop {
        let value = read_json_line(reader)
            .await?
            .ok_or(ServiceClientError::Disconnected)?;
        if value.get("requestId").and_then(Value::as_str) != Some(expected_id) {
            continue;
        }
        if value.get("type").and_then(Value::as_str) == Some("error") {
            return Err(service_error(value));
        }
        return Ok(value);
    }
}

fn response_string(value: &Value, key: &str) -> Result<String, ServiceClientError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ServiceClientError::Unexpected(format!("response omitted {key}")))
}

fn flow_run_response(value: Value) -> Result<ServiceFlowRunRecord, ServiceClientError> {
    let flow = value.get("flowRun").cloned().ok_or_else(|| {
        ServiceClientError::Unexpected("response omitted FlowRun projection".into())
    })?;
    serde_json::from_value(flow).map_err(ServiceClientError::from)
}

fn response_string_or_record(
    value: &Value,
    key: &str,
    record_key: &str,
) -> Result<String, ServiceClientError> {
    value
        .get(key)
        .or_else(|| value.get(record_key).and_then(|record| record.get(key)))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ServiceClientError::Unexpected(format!("response omitted {key}")))
}

fn service_error(value: Value) -> ServiceClientError {
    ServiceClientError::Rejected {
        code: value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .into(),
        message: value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("service request failed")
            .into(),
    }
}

async fn read_json_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Value>, ServiceClientError> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(ServiceClientError::Disconnected)
            };
        }
        if let Some(end) = available.iter().position(|byte| *byte == b'\n') {
            if frame.len() + end + 1 > MAX_FRAME_BYTES {
                return Err(ServiceClientError::FrameTooLarge);
            }
            frame.extend_from_slice(&available[..end]);
            reader.consume(end + 1);
            return Ok(Some(serde_json::from_slice(&frame)?));
        }
        let count = available.len();
        if frame.len() + count > MAX_FRAME_BYTES {
            return Err(ServiceClientError::FrameTooLarge);
        }
        frame.extend_from_slice(available);
        reader.consume(count);
    }
}

async fn write_json<W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: &Value,
) -> Result<(), ServiceClientError> {
    let mut frame = serde_json::to_vec(value)?;
    frame.push(b'\n');
    if frame.len() > MAX_FRAME_BYTES {
        return Err(ServiceClientError::FrameTooLarge);
    }
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = *chunk.get(1).unwrap_or(&0);
        let c = *chunk.get(2).unwrap_or(&0);
        out.push(BASE64[(a >> 2) as usize] as char);
        out.push(BASE64[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            BASE64[(((b & 15) << 2) | (c >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64[(c & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let mut bits = 0_u32;
    let mut count = 0_u8;
    for byte in input.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if byte == b'=' {
            break;
        }
        let value = BASE64
            .iter()
            .position(|candidate| *candidate == byte)
            .ok_or_else(|| "invalid base64 in process event".to_owned())?
            as u32;
        bits = (bits << 6) | value;
        count += 6;
        if count >= 8 {
            count -= 8;
            out.push((bits >> count) as u8);
        }
    }
    Ok(out)
}
