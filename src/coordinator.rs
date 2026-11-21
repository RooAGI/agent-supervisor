//! Event-driven in-memory coordination state for the long-lived Supervisor
//! service. The service transport authenticates callers and translates its
//! wire messages into these typed commands; this module owns state ordering,
//! attempt generations, deadlines, and non-blocking event publication.

// These internal command handlers mirror the service protocol's field sets;
// some recovery and coordinator operations are intentionally reserved for
// service routes that are not enabled in every build configuration.
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]

use crate::coordinator_store::{
    CommitOutcome, CoordinatorStore, CoordinatorStoreError, RecoveryDiagnostics, RecoveryState,
    ReservationOutcome, RestartClassification,
};
use crate::workflow::{
    AttemptOutcome, FlowRunProjection, FlowRunWorkflowState, WorkflowDispatchReceipt,
    WorkflowDispatchRequest, WorkflowError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{sleep_until, Instant};

const COMMAND_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 256;
const MAX_RETAINED_REPORTS: usize = 64;
const MAX_RETAINED_COORDINATOR_EVENTS: usize = 256;
const MAX_REPORT_DETAIL_BYTES: usize = 32 * 1024;
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ExecutionStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Blocked,
    Unknown,
}

impl ExecutionStatus {
    fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ActivityStatus {
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

impl ActivityStatus {
    fn terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Health {
    Unknown,
    Healthy,
    Stale,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum CancellationState {
    None,
    Requested,
    Delivered,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunRecord {
    pub run_id: String,
    pub manager_id: String,
    pub status: ExecutionStatus,
    pub context: Value,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActivityRecord {
    pub activity_id: String,
    pub run_id: String,
    pub kind: String,
    pub status: ActivityStatus,
    pub health: Health,
    pub last_contact_at_ms: Option<u64>,
    pub waiting_reason: Option<String>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttemptReport {
    pub status: ExecutionStatus,
    pub detail: Value,
    pub revision: u64,
    pub reported_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttemptRecord {
    pub attempt_id: String,
    pub activity_id: String,
    pub run_id: String,
    pub generation: u64,
    pub status: ExecutionStatus,
    pub cancellation: CancellationState,
    pub process_ids: Vec<String>,
    pub reports: Vec<AttemptReport>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProcessRecord {
    pub process_id: String,
    pub run_id: Option<String>,
    pub attempt_id: Option<String>,
    pub alive: bool,
    pub exit_status: Option<String>,
    pub revision: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ApprovalRequestState {
    Pending,
    Approved,
    Denied,
    TimedOut,
    Cancelled,
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApprovalRequestRecord {
    pub attempt_id: String,
    pub generation: u64,
    pub request_id: String,
    pub flow_id: String,
    pub task_id: String,
    pub post_id: String,
    pub kind: String,
    pub reason: String,
    pub payload: Value,
    pub state: ApprovalRequestState,
    pub requested_at_ms: u64,
    pub deadline_at_ms: u64,
    #[serde(default)]
    pub resolution_id: Option<String>,
    #[serde(default)]
    pub grant: Option<String>,
    #[serde(default)]
    pub resolution_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum WorkerToolRequestState {
    Pending,
    Claimed,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
    Unknown,
}

impl WorkerToolRequestState {
    fn terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Claimed)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkerToolRequestRecord {
    pub attempt_id: String,
    pub generation: u64,
    pub request_id: String,
    pub flow_id: String,
    pub task_id: String,
    pub post_id: String,
    pub coordinator_fence: u64,
    pub tool_name: String,
    pub arguments: Value,
    pub state: WorkerToolRequestState,
    pub requested_at_ms: u64,
    pub deadline_at_ms: u64,
    #[serde(default)]
    pub claim_id: Option<String>,
    #[serde(default)]
    pub resolution_id: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoordinatorSnapshot {
    pub manager_id: String,
    pub epoch: String,
    pub cursor: u64,
    pub revision: u64,
    pub runs: Vec<RunRecord>,
    pub activities: Vec<ActivityRecord>,
    pub attempts: Vec<AttemptRecord>,
    pub processes: Vec<ProcessRecord>,
    #[serde(default)]
    pub flow_runs: Vec<FlowRunWorkflowState>,
    #[serde(default)]
    pub approvals: Vec<ApprovalRequestRecord>,
    #[serde(default)]
    pub tool_requests: Vec<WorkerToolRequestRecord>,
    pub counts: CoordinatorCounts,
    #[serde(default)]
    pub recovery: Option<CoordinatorRecoveryInfo>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoordinatorRecoveryInfo {
    pub previous_epoch: Option<String>,
    pub attempts: Vec<RecoveredAttemptInfo>,
    pub diagnostics: RecoveryDiagnostics,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveredAttemptInfo {
    pub attempt_id: String,
    pub generation: u64,
    pub classification: RestartClassification,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoordinatorCounts {
    pub runs: usize,
    pub active_runs: usize,
    pub activities: usize,
    pub active_activities: usize,
    pub attempts: usize,
    pub active_attempts: usize,
    pub processes: usize,
    pub active_processes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoordinatorEvent {
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoordinatorReplay {
    pub epoch: String,
    pub cursor: u64,
    pub resync_required: bool,
    pub events: Vec<CoordinatorEvent>,
}

#[derive(Debug)]
pub(crate) enum CoordinatorError {
    Closed,
    NotFound(&'static str),
    InvalidParent(&'static str),
    StaleGeneration,
    TerminalTransition,
    InvalidReport,
    Persistence,
    IdempotencyConflict,
    Workflow(String),
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("coordinator loop is closed"),
            Self::NotFound(kind) => write!(f, "unknown {kind}"),
            Self::InvalidParent(kind) => write!(f, "invalid parent for {kind}"),
            Self::StaleGeneration => f.write_str("attempt generation is stale"),
            Self::TerminalTransition => f.write_str("terminal state cannot be replaced"),
            Self::InvalidReport => f.write_str("report detail exceeds the size limit"),
            Self::Persistence => {
                f.write_str("coordinator persistence failed; reopen the service to recover")
            }
            Self::IdempotencyConflict => {
                f.write_str("request ID was already committed with a different command")
            }
            Self::Workflow(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for CoordinatorError {}

/// A command handle for the event loop. The service keeps one handle for its
/// lifetime and uses `snapshot()` after reconnect; broadcast receivers are
/// deliberately lossy and must resync from the watched snapshot on lag.
#[derive(Clone)]
pub(crate) struct CoordinatorHandle {
    commands: mpsc::Sender<Command>,
    snapshot: watch::Receiver<CoordinatorSnapshot>,
    events: broadcast::Sender<CoordinatorEvent>,
    startup: watch::Receiver<i8>,
}

impl CoordinatorHandle {
    pub(crate) fn spawn(manager_id: impl Into<String>) -> Self {
        Self::spawn_inner(manager_id.into(), None, None)
    }

    pub(crate) fn spawn_persistent(
        manager_id: impl Into<String>,
        store: CoordinatorStore,
        recovered: RecoveryState,
    ) -> Self {
        Self::spawn_inner(manager_id.into(), Some(store), Some(recovered))
    }

    fn spawn_inner(
        manager_id: String,
        store: Option<CoordinatorStore>,
        recovered: Option<RecoveryState>,
    ) -> Self {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let (snapshot_tx, snapshot) = watch::channel(CoordinatorSnapshot {
            manager_id: manager_id.clone(),
            epoch: manager_id.clone(),
            ..CoordinatorSnapshot::default()
        });
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let (startup_tx, startup) = watch::channel(0i8);
        let actor = CoordinatorActor::new(
            manager_id,
            snapshot_tx,
            events.clone(),
            store.map(|store| Arc::new(Mutex::new(store))),
            recovered,
        );
        tokio::spawn(actor.run(receiver, startup_tx));
        Self {
            commands,
            snapshot,
            events,
            startup,
        }
    }

    pub(crate) async fn wait_ready(&self) -> Result<(), CoordinatorError> {
        let mut startup = self.startup.clone();
        loop {
            match *startup.borrow() {
                1 => return Ok(()),
                -1 => return Err(CoordinatorError::Persistence),
                _ => {}
            }
            startup
                .changed()
                .await
                .map_err(|_| CoordinatorError::Closed)?;
        }
    }

    pub(crate) fn snapshot(&self) -> CoordinatorSnapshot {
        self.snapshot.borrow().clone()
    }

    pub(crate) fn subscribe_snapshots(&self) -> watch::Receiver<CoordinatorSnapshot> {
        self.snapshot.clone()
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<CoordinatorEvent> {
        self.events.subscribe()
    }

    /// Subscribe before reading replay so changes between those operations
    /// remain observable. Consumers discard live events at or below the
    /// replay cursor; a lag or explicit resync result requires a fresh
    /// snapshot.
    pub(crate) async fn subscribe_from(
        &self,
        epoch: String,
        cursor: u64,
    ) -> (broadcast::Receiver<CoordinatorEvent>, CoordinatorReplay) {
        let receiver = self.subscribe();
        let replay = self
            .request(|reply| Command::Replay {
                epoch,
                cursor,
                reply,
            })
            .await
            .unwrap_or_else(|_| CoordinatorReplay {
                epoch: String::new(),
                cursor: 0,
                resync_required: true,
                events: Vec::new(),
            });
        (receiver, replay)
    }

    pub(crate) async fn register_run(&self, context: Value) -> Result<RunRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterRun {
            request_id: new_id(),
            context,
            reply,
        })
        .await
    }

    pub(crate) async fn register_run_with_id(
        &self,
        request_id: String,
        context: Value,
    ) -> Result<RunRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterRun {
            request_id,
            context,
            reply,
        })
        .await
    }

    pub(crate) async fn register_activity(
        &self,
        run_id: String,
        kind: String,
    ) -> Result<ActivityRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterActivity {
            request_id: new_id(),
            run_id,
            kind,
            reply,
        })
        .await
    }

    pub(crate) async fn register_activity_with_id(
        &self,
        request_id: String,
        run_id: String,
        kind: String,
    ) -> Result<ActivityRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterActivity {
            request_id,
            run_id,
            kind,
            reply,
        })
        .await
    }

    pub(crate) async fn register_attempt(
        &self,
        activity_id: String,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterAttempt {
            request_id: new_id(),
            activity_id,
            reply,
        })
        .await
    }

    pub(crate) async fn register_attempt_with_id(
        &self,
        request_id: String,
        activity_id: String,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterAttempt {
            request_id,
            activity_id,
            reply,
        })
        .await
    }

    pub(crate) async fn report_attempt(
        &self,
        attempt_id: String,
        generation: u64,
        status: ExecutionStatus,
        detail: Value,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::ReportAttempt {
            request_id: new_id(),
            attempt_id,
            generation,
            status,
            detail,
            reply,
        })
        .await
    }

    pub(crate) async fn report_attempt_with_id(
        &self,
        request_id: String,
        attempt_id: String,
        generation: u64,
        status: ExecutionStatus,
        detail: Value,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::ReportAttempt {
            request_id,
            attempt_id,
            generation,
            status,
            detail,
            reply,
        })
        .await
    }

    pub(crate) async fn request_cancel(
        &self,
        attempt_id: String,
        generation: u64,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::RequestCancel {
            request_id: new_id(),
            attempt_id,
            generation,
            reply,
        })
        .await
    }

    pub(crate) async fn request_cancel_with_id(
        &self,
        request_id: String,
        attempt_id: String,
        generation: u64,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::RequestCancel {
            request_id,
            attempt_id,
            generation,
            reply,
        })
        .await
    }

    pub(crate) async fn mark_cancel_delivered(
        &self,
        attempt_id: String,
        generation: u64,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::MarkCancelDelivered {
            request_id: new_id(),
            attempt_id,
            generation,
            reply,
        })
        .await
    }

    pub(crate) async fn mark_cancel_delivered_with_id(
        &self,
        request_id: String,
        attempt_id: String,
        generation: u64,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::MarkCancelDelivered {
            request_id,
            attempt_id,
            generation,
            reply,
        })
        .await
    }

    pub(crate) async fn associate_process(
        &self,
        process_id: String,
        attempt_id: Option<String>,
        generation: Option<u64>,
    ) -> Result<ProcessRecord, CoordinatorError> {
        self.request(|reply| Command::AssociateProcess {
            request_id: new_id(),
            process_id,
            attempt_id,
            generation,
            reply,
        })
        .await
    }

    pub(crate) async fn associate_process_with_id(
        &self,
        request_id: String,
        process_id: String,
        attempt_id: Option<String>,
        generation: Option<u64>,
    ) -> Result<ProcessRecord, CoordinatorError> {
        self.request(|reply| Command::AssociateProcess {
            request_id,
            process_id,
            attempt_id,
            generation,
            reply,
        })
        .await
    }

    /// Call after a process stream has finished draining and the child has
    /// been reaped. It can wait for coordinator queue capacity without
    /// stalling stdout/stderr draining.
    pub(crate) async fn process_exited(
        &self,
        process_id: String,
        exit_status: String,
    ) -> Result<ProcessRecord, CoordinatorError> {
        self.request(|reply| Command::ProcessExited {
            request_id: new_id(),
            process_id,
            exit_status,
            reply,
        })
        .await
    }

    pub(crate) async fn process_exited_with_id(
        &self,
        request_id: String,
        process_id: String,
        exit_status: String,
    ) -> Result<ProcessRecord, CoordinatorError> {
        self.request(|reply| Command::ProcessExited {
            request_id,
            process_id,
            exit_status,
            reply,
        })
        .await
    }

    pub(crate) async fn set_waiting(
        &self,
        request_id: String,
        activity_id: String,
        reason: Option<String>,
        status: ActivityStatus,
    ) -> Result<ActivityRecord, CoordinatorError> {
        self.request(|reply| Command::SetActivityStatus {
            request_id,
            activity_id,
            reason: reason.clone(),
            status,
            reply,
        })
        .await
    }

    pub(crate) async fn set_run_status(
        &self,
        request_id: String,
        run_id: String,
        status: ExecutionStatus,
    ) -> Result<RunRecord, CoordinatorError> {
        self.request(|reply| Command::SetRunStatus {
            request_id,
            run_id,
            status,
            reply,
        })
        .await
    }

    pub(crate) async fn set_activity_timeout(
        &self,
        request_id: String,
        activity_id: String,
        timeout: Duration,
    ) -> Result<(), CoordinatorError> {
        self.request(|reply| Command::SetActivityTimeout {
            request_id,
            activity_id,
            timeout_ms: timeout.as_millis().min(u64::MAX as u128) as u64,
            deadline: Instant::now() + timeout,
            reply,
        })
        .await
    }

    pub(crate) async fn reserve_launch_with_id(
        &self,
        request_id: String,
        command_fingerprint: Value,
    ) -> Result<ReservationOutcome, CoordinatorError> {
        self.request(|reply| Command::ReserveLaunch {
            request_id,
            command_fingerprint,
            reply,
        })
        .await
    }

    pub(crate) async fn complete_launch_reservation_with_id(
        &self,
        request_id: String,
        result: Value,
    ) -> Result<CommitOutcome, CoordinatorError> {
        self.request(|reply| Command::CompleteLaunchReservation {
            request_id,
            result,
            reply,
        })
        .await
    }

    pub(crate) async fn register_flowrun_with_id(
        &self,
        request_id: String,
        projection: FlowRunProjection,
        coordinator_id: String,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::RegisterFlowRun {
            request_id,
            projection,
            coordinator_id,
            reply,
        })
        .await
    }

    pub(crate) async fn claim_flowrun_with_id(
        &self,
        request_id: String,
        flow_id: String,
        coordinator_id: String,
        expected_fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::ClaimFlowRun {
            request_id,
            flow_id,
            coordinator_id,
            expected_fence,
            reply,
        })
        .await
    }

    pub(crate) async fn advance_flow_stage_with_id(
        &self,
        request_id: String,
        flow_id: String,
        expected_stage: String,
        next_stage: String,
        expected_flow_revision: u64,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::AdvanceFlowStage {
            request_id,
            flow_id,
            expected_stage,
            next_stage,
            expected_flow_revision,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn complete_flowrun_with_id(
        &self,
        request_id: String,
        flow_id: String,
        expected_stage: String,
        expected_flow_revision: u64,
        evidence: Value,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::CompleteFlowRun {
            request_id,
            flow_id,
            expected_stage,
            expected_flow_revision,
            evidence,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn bind_flow_attempt_with_id(
        &self,
        request_id: String,
        flow_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        source_cursor: u64,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::BindFlowAttempt {
            request_id,
            flow_id,
            task_id,
            attempt_id,
            generation,
            source_cursor,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn reconcile_flow_attempt_with_id(
        &self,
        request_id: String,
        flow_id: String,
        task_id: String,
        outcome: AttemptOutcome,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::ReconcileFlowAttempt {
            request_id,
            flow_id,
            task_id,
            outcome,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn apply_flow_review_with_id(
        &self,
        request_id: String,
        flow_id: String,
        task_id: String,
        flow_revision: u64,
        evidence: Value,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::ApplyFlowReview {
            request_id,
            flow_id,
            task_id,
            flow_revision,
            evidence,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn request_flow_task_cancel_with_id(
        &self,
        request_id: String,
        flow_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::RequestFlowTaskCancel {
            request_id,
            flow_id,
            task_id,
            attempt_id,
            generation,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn acknowledge_flowboard_event_with_id(
        &self,
        request_id: String,
        flow_id: String,
        event_id: String,
        new_flow_revision: u64,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::AcknowledgeFlowboardEvent {
            request_id,
            flow_id,
            event_id,
            new_flow_revision,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn rebase_flowboard_outbox_with_id(
        &self,
        request_id: String,
        flow_id: String,
        flow_revision: u64,
        existing_event_ids: Vec<String>,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::RebaseFlowboardOutbox {
            request_id,
            flow_id,
            flow_revision,
            existing_event_ids,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn reserve_flow_dispatch_with_id(
        &self,
        request_id: String,
        flow_id: String,
        task_id: String,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::ReserveFlowDispatch {
            request_id,
            flow_id,
            task_id,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn register_flow_worker_attempt_with_id(
        &self,
        request_id: String,
        flow_id: String,
        task_id: String,
        fence: u64,
    ) -> Result<AttemptRecord, CoordinatorError> {
        self.request(|reply| Command::RegisterFlowWorkerAttempt {
            request_id,
            flow_id,
            task_id,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn request_worker_approval_with_id(
        &self,
        command_id: String,
        attempt_id: String,
        generation: u64,
        fence: u64,
        request_id: String,
        kind: String,
        reason: String,
        payload: Value,
        deadline_at_ms: u64,
    ) -> Result<ApprovalRequestRecord, CoordinatorError> {
        self.request(|reply| Command::RequestWorkerApproval {
            command_id,
            attempt_id,
            generation,
            fence,
            request_id,
            kind,
            reason,
            payload,
            deadline_at_ms,
            reply,
        })
        .await
    }

    pub(crate) async fn resolve_worker_approval_with_id(
        &self,
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        resolution_id: String,
        decision: ApprovalRequestState,
        grant: Option<String>,
        fence: u64,
    ) -> Result<ApprovalRequestRecord, CoordinatorError> {
        self.request(|reply| Command::ResolveWorkerApproval {
            command_id,
            attempt_id,
            generation,
            request_id,
            resolution_id,
            decision,
            grant,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn wait_worker_approval(
        &self,
        attempt_id: &str,
        generation: u64,
        request_id: &str,
    ) -> Result<ApprovalRequestRecord, CoordinatorError> {
        let key = approval_key(attempt_id, generation, request_id);
        let mut snapshot = self.subscribe_snapshots();
        loop {
            if let Some(record) =
                snapshot.borrow().approvals.iter().find(|row| {
                    approval_key(&row.attempt_id, row.generation, &row.request_id) == key
                })
            {
                if record.state != ApprovalRequestState::Pending {
                    return Ok(record.clone());
                }
            } else {
                return Err(CoordinatorError::NotFound("approval request"));
            }
            snapshot
                .changed()
                .await
                .map_err(|_| CoordinatorError::Closed)?;
        }
    }

    pub(crate) async fn request_worker_tool_with_id(
        &self,
        command_id: String,
        attempt_id: String,
        generation: u64,
        fence: u64,
        request_id: String,
        tool_name: String,
        arguments: Value,
        deadline_at_ms: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        self.request(|reply| Command::RequestWorkerTool {
            command_id,
            attempt_id,
            generation,
            fence,
            request_id,
            tool_name,
            arguments,
            deadline_at_ms,
            reply,
        })
        .await
    }

    pub(crate) async fn claim_worker_tool_with_id(
        &self,
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        claim_id: String,
        fence: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        self.request(|reply| Command::ClaimWorkerTool {
            command_id,
            attempt_id,
            generation,
            request_id,
            claim_id,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn resolve_worker_tool_with_id(
        &self,
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        claim_id: String,
        resolution_id: String,
        result: Option<Value>,
        error: Option<String>,
        fence: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        self.request(|reply| Command::ResolveWorkerTool {
            command_id,
            attempt_id,
            generation,
            request_id,
            claim_id,
            resolution_id,
            result,
            error,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn finish_worker_tool_with_id(
        &self,
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        state: WorkerToolRequestState,
        error: String,
        fence: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        self.request(|reply| Command::FinishWorkerTool {
            command_id,
            attempt_id,
            generation,
            request_id,
            state,
            error,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn wait_worker_tool(
        &self,
        attempt_id: &str,
        generation: u64,
        request_id: &str,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        let key = approval_key(attempt_id, generation, request_id);
        let mut snapshot = self.subscribe_snapshots();
        loop {
            if let Some(record) =
                snapshot.borrow().tool_requests.iter().find(|row| {
                    approval_key(&row.attempt_id, row.generation, &row.request_id) == key
                })
            {
                if record.state.terminal() {
                    return Ok(record.clone());
                }
            } else {
                return Err(CoordinatorError::NotFound("worker tool request"));
            }
            snapshot
                .changed()
                .await
                .map_err(|_| CoordinatorError::Closed)?;
        }
    }

    pub(crate) async fn complete_flow_dispatch_with_id(
        &self,
        request_id: String,
        flow_id: String,
        dispatch: WorkflowDispatchRequest,
        receipt: WorkflowDispatchReceipt,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::CompleteFlowDispatch {
            request_id,
            flow_id,
            dispatch,
            receipt,
            fence,
            reply,
        })
        .await
    }

    pub(crate) async fn release_flow_dispatch_with_id(
        &self,
        request_id: String,
        flow_id: String,
        dispatch: WorkflowDispatchRequest,
        fence: u64,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        self.request(|reply| Command::ReleaseFlowDispatch {
            request_id,
            flow_id,
            dispatch,
            fence,
            reply,
        })
        .await
    }

    async fn request<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, CoordinatorError>>) -> Command,
    ) -> Result<T, CoordinatorError> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(make(reply))
            .await
            .map_err(|_| CoordinatorError::Closed)?;
        receive.await.map_err(|_| CoordinatorError::Closed)?
    }
}

enum Command {
    RegisterRun {
        request_id: String,
        context: Value,
        reply: oneshot::Sender<Result<RunRecord, CoordinatorError>>,
    },
    RegisterActivity {
        request_id: String,
        run_id: String,
        kind: String,
        reply: oneshot::Sender<Result<ActivityRecord, CoordinatorError>>,
    },
    RegisterAttempt {
        request_id: String,
        activity_id: String,
        reply: oneshot::Sender<Result<AttemptRecord, CoordinatorError>>,
    },
    ReportAttempt {
        request_id: String,
        attempt_id: String,
        generation: u64,
        status: ExecutionStatus,
        detail: Value,
        reply: oneshot::Sender<Result<AttemptRecord, CoordinatorError>>,
    },
    RequestCancel {
        request_id: String,
        attempt_id: String,
        generation: u64,
        reply: oneshot::Sender<Result<AttemptRecord, CoordinatorError>>,
    },
    MarkCancelDelivered {
        request_id: String,
        attempt_id: String,
        generation: u64,
        reply: oneshot::Sender<Result<AttemptRecord, CoordinatorError>>,
    },
    AssociateProcess {
        request_id: String,
        process_id: String,
        attempt_id: Option<String>,
        generation: Option<u64>,
        reply: oneshot::Sender<Result<ProcessRecord, CoordinatorError>>,
    },
    ProcessExited {
        request_id: String,
        process_id: String,
        exit_status: String,
        reply: oneshot::Sender<Result<ProcessRecord, CoordinatorError>>,
    },
    SetActivityStatus {
        request_id: String,
        activity_id: String,
        reason: Option<String>,
        status: ActivityStatus,
        reply: oneshot::Sender<Result<ActivityRecord, CoordinatorError>>,
    },
    SetRunStatus {
        request_id: String,
        run_id: String,
        status: ExecutionStatus,
        reply: oneshot::Sender<Result<RunRecord, CoordinatorError>>,
    },
    Replay {
        epoch: String,
        cursor: u64,
        reply: oneshot::Sender<Result<CoordinatorReplay, CoordinatorError>>,
    },
    SetActivityTimeout {
        request_id: String,
        activity_id: String,
        timeout_ms: u64,
        deadline: Instant,
        reply: oneshot::Sender<Result<(), CoordinatorError>>,
    },
    ReserveLaunch {
        request_id: String,
        command_fingerprint: Value,
        reply: oneshot::Sender<Result<ReservationOutcome, CoordinatorError>>,
    },
    CompleteLaunchReservation {
        request_id: String,
        result: Value,
        reply: oneshot::Sender<Result<CommitOutcome, CoordinatorError>>,
    },
    RegisterFlowRun {
        request_id: String,
        projection: FlowRunProjection,
        coordinator_id: String,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    ClaimFlowRun {
        request_id: String,
        flow_id: String,
        coordinator_id: String,
        expected_fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    AdvanceFlowStage {
        request_id: String,
        flow_id: String,
        expected_stage: String,
        next_stage: String,
        expected_flow_revision: u64,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    CompleteFlowRun {
        request_id: String,
        flow_id: String,
        expected_stage: String,
        expected_flow_revision: u64,
        evidence: Value,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    BindFlowAttempt {
        request_id: String,
        flow_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        source_cursor: u64,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    ReconcileFlowAttempt {
        request_id: String,
        flow_id: String,
        task_id: String,
        outcome: AttemptOutcome,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    ApplyFlowReview {
        request_id: String,
        flow_id: String,
        task_id: String,
        flow_revision: u64,
        evidence: Value,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    RequestFlowTaskCancel {
        request_id: String,
        flow_id: String,
        task_id: String,
        attempt_id: String,
        generation: u64,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    ReserveFlowDispatch {
        request_id: String,
        flow_id: String,
        task_id: String,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    RegisterFlowWorkerAttempt {
        request_id: String,
        flow_id: String,
        task_id: String,
        fence: u64,
        reply: oneshot::Sender<Result<AttemptRecord, CoordinatorError>>,
    },
    RequestWorkerApproval {
        command_id: String,
        attempt_id: String,
        generation: u64,
        fence: u64,
        request_id: String,
        kind: String,
        reason: String,
        payload: Value,
        deadline_at_ms: u64,
        reply: oneshot::Sender<Result<ApprovalRequestRecord, CoordinatorError>>,
    },
    ResolveWorkerApproval {
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        resolution_id: String,
        decision: ApprovalRequestState,
        grant: Option<String>,
        fence: u64,
        reply: oneshot::Sender<Result<ApprovalRequestRecord, CoordinatorError>>,
    },
    RequestWorkerTool {
        command_id: String,
        attempt_id: String,
        generation: u64,
        fence: u64,
        request_id: String,
        tool_name: String,
        arguments: Value,
        deadline_at_ms: u64,
        reply: oneshot::Sender<Result<WorkerToolRequestRecord, CoordinatorError>>,
    },
    ClaimWorkerTool {
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        claim_id: String,
        fence: u64,
        reply: oneshot::Sender<Result<WorkerToolRequestRecord, CoordinatorError>>,
    },
    ResolveWorkerTool {
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        claim_id: String,
        resolution_id: String,
        result: Option<Value>,
        error: Option<String>,
        fence: u64,
        reply: oneshot::Sender<Result<WorkerToolRequestRecord, CoordinatorError>>,
    },
    FinishWorkerTool {
        command_id: String,
        attempt_id: String,
        generation: u64,
        request_id: String,
        state: WorkerToolRequestState,
        error: String,
        fence: u64,
        reply: oneshot::Sender<Result<WorkerToolRequestRecord, CoordinatorError>>,
    },
    CompleteFlowDispatch {
        request_id: String,
        flow_id: String,
        dispatch: WorkflowDispatchRequest,
        receipt: WorkflowDispatchReceipt,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    ReleaseFlowDispatch {
        request_id: String,
        flow_id: String,
        dispatch: WorkflowDispatchRequest,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    AcknowledgeFlowboardEvent {
        request_id: String,
        flow_id: String,
        event_id: String,
        new_flow_revision: u64,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
    RebaseFlowboardOutbox {
        request_id: String,
        flow_id: String,
        flow_revision: u64,
        existing_event_ids: Vec<String>,
        fence: u64,
        reply: oneshot::Sender<Result<FlowRunWorkflowState, CoordinatorError>>,
    },
}

struct CoordinatorActor {
    manager_id: String,
    revision: u64,
    sequence: u64,
    retained_events: std::collections::VecDeque<CoordinatorEvent>,
    runs: BTreeMap<String, RunRecord>,
    activities: BTreeMap<String, ActivityRecord>,
    attempts: BTreeMap<String, AttemptRecord>,
    processes: BTreeMap<String, ProcessRecord>,
    flow_runs: BTreeMap<String, FlowRunWorkflowState>,
    approvals: BTreeMap<String, ApprovalRequestRecord>,
    tool_requests: BTreeMap<String, WorkerToolRequestRecord>,
    latest_generation: HashMap<String, u64>,
    activity_contact_deadlines: HashMap<String, Instant>,
    activity_timeouts: HashMap<String, (Instant, Option<u64>)>,
    approval_deadlines: HashMap<String, Instant>,
    tool_deadlines: HashMap<String, Instant>,
    store: Option<Arc<Mutex<CoordinatorStore>>>,
    recovery: Option<CoordinatorRecoveryInfo>,
    persistence_failed: bool,
    active_request_id: String,
    active_command: Value,
    snapshot_tx: watch::Sender<CoordinatorSnapshot>,
    event_tx: broadcast::Sender<CoordinatorEvent>,
}

impl CoordinatorActor {
    fn new(
        manager_id: String,
        snapshot_tx: watch::Sender<CoordinatorSnapshot>,
        event_tx: broadcast::Sender<CoordinatorEvent>,
        store: Option<Arc<Mutex<CoordinatorStore>>>,
        recovered: Option<RecoveryState>,
    ) -> Self {
        let mut actor = Self {
            manager_id,
            revision: 0,
            sequence: 0,
            retained_events: std::collections::VecDeque::new(),
            runs: BTreeMap::new(),
            activities: BTreeMap::new(),
            attempts: BTreeMap::new(),
            processes: BTreeMap::new(),
            flow_runs: BTreeMap::new(),
            approvals: BTreeMap::new(),
            tool_requests: BTreeMap::new(),
            latest_generation: HashMap::new(),
            activity_contact_deadlines: HashMap::new(),
            activity_timeouts: HashMap::new(),
            approval_deadlines: HashMap::new(),
            tool_deadlines: HashMap::new(),
            store,
            recovery: None,
            persistence_failed: false,
            active_request_id: String::new(),
            active_command: Value::Null,
            snapshot_tx,
            event_tx,
        };
        if let Some(recovered) = recovered {
            actor.restore(recovered);
        }
        actor
    }

    fn restore(&mut self, recovered: RecoveryState) {
        self.recovery = Some(CoordinatorRecoveryInfo {
            previous_epoch: recovered.previous_epoch,
            attempts: recovered
                .recovered_attempts
                .into_iter()
                .map(|attempt| RecoveredAttemptInfo {
                    attempt_id: attempt.attempt_id,
                    generation: attempt.generation,
                    classification: attempt.classification,
                    reason: attempt.reason,
                })
                .collect(),
            diagnostics: recovered.diagnostics,
        });
        let Some(snapshot) = recovered
            .snapshot
            .as_ref()
            .and_then(|value| serde_json::from_value::<CoordinatorSnapshot>(value.clone()).ok())
        else {
            return;
        };
        self.runs = snapshot
            .runs
            .into_iter()
            .map(|mut run| {
                if !run.status.terminal() {
                    run.status = ExecutionStatus::Unknown;
                }
                run.manager_id = self.manager_id.clone();
                (run.run_id.clone(), run)
            })
            .collect();
        self.activities = snapshot
            .activities
            .into_iter()
            .map(|mut activity| {
                if !activity.status.terminal() {
                    activity.status = ActivityStatus::Unknown;
                    activity.health = Health::Stale;
                    activity.waiting_reason = Some(
                        "service restarted; prior activity state requires reconciliation"
                            .to_owned(),
                    );
                }
                (activity.activity_id.clone(), activity)
            })
            .collect();
        self.attempts = snapshot
            .attempts
            .into_iter()
            .map(|mut attempt| {
                if !attempt.status.terminal() {
                    attempt.status = ExecutionStatus::Unknown;
                }
                let current = self
                    .latest_generation
                    .entry(attempt.activity_id.clone())
                    .or_insert(0);
                *current = (*current).max(attempt.generation);
                (attempt.attempt_id.clone(), attempt)
            })
            .collect();
        // Never restore process ownership from an old PID or process record.
        self.processes.clear();
        self.approvals = snapshot
            .approvals
            .into_iter()
            .map(|mut approval| {
                if approval.state == ApprovalRequestState::Pending {
                    approval.state = ApprovalRequestState::Interrupted;
                    approval.resolution_reason = Some(
                        "service restarted while approval was pending; worker must reconcile"
                            .to_owned(),
                    );
                }
                (
                    approval_key(
                        &approval.attempt_id,
                        approval.generation,
                        &approval.request_id,
                    ),
                    approval,
                )
            })
            .collect();
        self.tool_requests = snapshot
            .tool_requests
            .into_iter()
            .map(|mut request| {
                match request.state {
                    WorkerToolRequestState::Pending => {
                        request.state = WorkerToolRequestState::Interrupted;
                        request.error = Some(
                            "service restarted while tool request was pending".to_owned(),
                        );
                    }
                    WorkerToolRequestState::Claimed => {
                        request.state = WorkerToolRequestState::Unknown;
                        request.error = Some(
                            "service restarted after tool request was claimed; side effect is unknown and will not be replayed".to_owned(),
                        );
                    }
                    _ => {}
                }
                (
                    approval_key(&request.attempt_id, request.generation, &request.request_id),
                    request,
                )
            })
            .collect();
        self.flow_runs = snapshot
            .flow_runs
            .into_iter()
            .map(|workflow| (workflow.flow_id.clone(), workflow))
            .collect();
        // A persisted fence belongs to the previous service epoch. Advance it
        // synchronously during restore so requests from a disconnected Desktop
        // coordinator are rejected before the new service accepts commands.
        let recovered_flow_ids = self.flow_runs.keys().cloned().collect::<Vec<_>>();
        for flow_id in recovered_flow_ids {
            if let Some(flow) = self.flow_runs.get_mut(&flow_id) {
                for task in flow.tasks.values_mut() {
                    task.supervisor_run_id
                        .get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
                    task.supervisor_activity_id
                        .get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
                }
                let old_fence = flow.coordinator_fence;
                let _ = flow.claim(self.manager_id.clone(), old_fence);
            }
        }
        let interrupted = self.approvals.values().cloned().collect::<Vec<_>>();
        for approval in interrupted {
            if approval.state != ApprovalRequestState::Interrupted {
                continue;
            }
            if let Some(flow) = self.flow_runs.get_mut(&approval.flow_id) {
                if flow.tasks.get(&approval.task_id).is_some_and(|task| {
                    task.attempt_id.as_deref() == Some(approval.attempt_id.as_str())
                        && task.generation == Some(approval.generation)
                        && task.state == crate::workflow::WorkflowTaskState::WaitingForApproval
                }) {
                    let _ = flow.resolve_task_approval_wait(
                        &approval.task_id,
                        &approval.request_id,
                        false,
                        approval.resolution_reason.clone(),
                        flow.coordinator_fence,
                    );
                }
            }
        }
        for request in self.tool_requests.values_mut() {
            if request.state == WorkerToolRequestState::Interrupted
                || request.state == WorkerToolRequestState::Unknown
            {
                if let Some(flow) = self.flow_runs.get_mut(&request.flow_id) {
                    if let Some(task) = flow.tasks.get_mut(&request.task_id) {
                        if task.attempt_id.as_deref() == Some(request.attempt_id.as_str())
                            && task.generation == Some(request.generation)
                        {
                            task.state = crate::workflow::WorkflowTaskState::Waiting;
                            task.dispatch_pending = false;
                            task.waiting_reason = request.error.clone();
                        }
                    }
                }
            }
        }
        self.publish_snapshot();
    }

    async fn reserve_launch(
        &mut self,
        request_id: &str,
        command_fingerprint: Value,
    ) -> Result<ReservationOutcome, CoordinatorError> {
        let store = self.store.as_ref().ok_or(CoordinatorError::Persistence)?;
        let store = Arc::clone(store);
        let request_id = request_id.to_owned();
        let operation = tokio::task::spawn_blocking(move || {
            let mut store = store.lock().map_err(|_| CoordinatorError::Persistence)?;
            let result = store
                .reserve_command(&request_id, command_fingerprint)
                .map_err(map_store_error);
            Ok::<_, CoordinatorError>((result, store.diagnostics()))
        })
        .await
        .map_err(|_| CoordinatorError::Persistence)?;
        let (result, diagnostics) = operation?;
        if let Some(recovery) = self.recovery.as_mut() {
            recovery.diagnostics = diagnostics;
        }
        result
    }

    async fn complete_launch_reservation(
        &mut self,
        request_id: &str,
        result: Value,
    ) -> Result<CommitOutcome, CoordinatorError> {
        let store = self.store.as_ref().ok_or(CoordinatorError::Persistence)?;
        let store = Arc::clone(store);
        let request_id = request_id.to_owned();
        let operation = tokio::task::spawn_blocking(move || {
            let mut store = store.lock().map_err(|_| CoordinatorError::Persistence)?;
            let result = store
                .complete_reservation(&request_id, result)
                .map_err(map_store_error);
            Ok::<_, CoordinatorError>((result, store.diagnostics()))
        })
        .await
        .map_err(|_| CoordinatorError::Persistence)?;
        let (result, diagnostics) = operation?;
        if let Some(recovery) = self.recovery.as_mut() {
            recovery.diagnostics = diagnostics;
        }
        result
    }

    fn reconcile_workflow_attempt(
        &mut self,
        attempt: &AttemptRecord,
        status: ExecutionStatus,
        detail: &Value,
    ) {
        let outcome = AttemptOutcome {
            attempt_id: attempt.attempt_id.clone(),
            generation: attempt.generation,
            source_cursor: self.sequence.saturating_add(1),
            status: match status {
                ExecutionStatus::Queued => "queued",
                ExecutionStatus::Running => "running",
                ExecutionStatus::Succeeded => "succeeded",
                ExecutionStatus::Failed => "failed",
                ExecutionStatus::Cancelled => "cancelled",
                ExecutionStatus::Blocked => "blocked",
                ExecutionStatus::Unknown => "unknown",
            }
            .to_owned(),
            reason: detail
                .get("reason")
                .or_else(|| detail.get("reasonCode"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            workflow_cancel_requested: attempt.cancellation == CancellationState::Completed,
            retryable: detail
                .get("retryable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            evidence: detail.get("evidence").cloned(),
        };
        for flow in self.flow_runs.values_mut() {
            let task_id = flow.tasks.values().find_map(|task| {
                (task.attempt_id.as_deref() == Some(attempt.attempt_id.as_str())
                    && task.generation == Some(attempt.generation))
                .then(|| task.task_id.clone())
            });
            if let Some(task_id) = task_id {
                let _ = flow.reconcile_attempt(&task_id, outcome.clone(), flow.coordinator_fence);
            }
        }
    }

    async fn update_flowrun(
        &mut self,
        flow_id: &str,
        action: &str,
        apply: impl FnOnce(&mut FlowRunWorkflowState) -> Result<(), WorkflowError>,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        {
            let state = self
                .flow_runs
                .get_mut(flow_id)
                .ok_or_else(|| CoordinatorError::Workflow("unknown FlowRun".to_owned()))?;
            apply(state).map_err(map_workflow_error)?;
        }
        self.persist_flowrun_change(flow_id, action).await
    }

    async fn register_flow_worker_attempt(
        &mut self,
        flow_id: &str,
        task_id: &str,
        fence: u64,
    ) -> Result<AttemptRecord, CoordinatorError> {
        let flow = self
            .flow_runs
            .get(flow_id)
            .ok_or_else(|| CoordinatorError::Workflow("unknown FlowRun".to_owned()))?;
        if flow.coordinator_fence_exhausted || flow.coordinator_fence != fence {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        let task = flow
            .tasks
            .get(task_id)
            .cloned()
            .ok_or(CoordinatorError::NotFound("FlowRun task"))?;
        let (Some(run_id), Some(activity_id), Some(attempt_id), Some(generation)) = (
            task.supervisor_run_id.clone(),
            task.supervisor_activity_id.clone(),
            task.attempt_id.clone(),
            task.generation,
        ) else {
            return Err(CoordinatorError::Workflow(
                "FlowRun dispatch is not reserved".to_owned(),
            ));
        };
        if let Some(existing) = self.attempts.get(&attempt_id) {
            if existing.activity_id == activity_id
                && existing.run_id == run_id
                && existing.generation == generation
            {
                return Ok(existing.clone());
            }
            return Err(CoordinatorError::IdempotencyConflict);
        }
        if !task.dispatch_pending
            || !matches!(task.state, crate::workflow::WorkflowTaskState::Assigned)
        {
            return Err(CoordinatorError::Workflow(
                "FlowRun dispatch is not pending".to_owned(),
            ));
        }
        let now = timestamp_ms();
        self.runs.entry(run_id.clone()).or_insert_with(|| RunRecord {
            run_id: run_id.clone(),
            manager_id: self.manager_id.clone(),
            status: ExecutionStatus::Running,
            context: json!({"flowRunId":flow_id,"taskId":task_id,"postId":task.post_id,"title":task.title}),
            revision: 1,
            updated_at_ms: now,
        });
        self.activities
            .entry(activity_id.clone())
            .or_insert_with(|| ActivityRecord {
                activity_id: activity_id.clone(),
                run_id: run_id.clone(),
                kind: "flowrun_task".to_owned(),
                status: ActivityStatus::Running,
                health: Health::Healthy,
                last_contact_at_ms: Some(now),
                waiting_reason: None,
                revision: 1,
                updated_at_ms: now,
            });
        let attempt = AttemptRecord {
            attempt_id: attempt_id.clone(),
            activity_id: activity_id.clone(),
            run_id: run_id.clone(),
            generation,
            status: ExecutionStatus::Running,
            cancellation: CancellationState::None,
            process_ids: Vec::new(),
            reports: Vec::new(),
            revision: 1,
            updated_at_ms: now,
        };
        self.latest_generation.insert(activity_id, generation);
        self.attempts.insert(attempt_id.clone(), attempt.clone());
        self.changed(
            "attemptRegistered",
            Some(run_id),
            Some(task.supervisor_activity_id.clone().unwrap_or_default()),
            Some(attempt_id),
            None,
            json!({"generation":generation,"flowRunId":flow_id,"taskId":task_id,"postId":task.post_id}),
        )
        .await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(attempt)
        }
    }

    async fn request_worker_approval(
        &mut self,
        attempt_id: String,
        generation: u64,
        expected_fence: u64,
        request_id: String,
        kind: String,
        reason: String,
        payload: Value,
        deadline_at_ms: u64,
    ) -> Result<ApprovalRequestRecord, CoordinatorError> {
        let payload = sanitize_approval_payload(payload).ok_or(CoordinatorError::InvalidReport)?;
        let reason = sanitize_approval_text(&reason);
        let payload_bytes = serde_json::to_vec(&payload).map_or(usize::MAX, |bytes| bytes.len());
        if payload_bytes > 8 * 1024
            || request_id.is_empty()
            || request_id.len() > 128
            || kind.is_empty()
            || kind.len() > 96
            || reason.len() > 512
        {
            return Err(CoordinatorError::InvalidReport);
        }
        let attempt = self
            .attempts
            .get(&attempt_id)
            .filter(|attempt| attempt.generation == generation)
            .cloned()
            .ok_or(CoordinatorError::StaleGeneration)?;
        if attempt.status.terminal() {
            return Err(CoordinatorError::TerminalTransition);
        }
        let candidate = self
            .flow_runs
            .values()
            .find_map(|flow| {
                flow.tasks.values().find_map(|task| {
                    (task.attempt_id.as_deref() == Some(attempt_id.as_str())
                        && task.generation == Some(generation))
                    .then(|| (flow.flow_id.clone(), flow.coordinator_fence, task.clone()))
                })
            })
            .ok_or(CoordinatorError::NotFound("FlowRun attempt"))?;
        let (flow_id, fence, task) = candidate;
        if fence != expected_fence {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        if task.cancel_requested {
            return Err(CoordinatorError::TerminalTransition);
        }
        if deadline_at_ms <= timestamp_ms()
            || deadline_at_ms > timestamp_ms().saturating_add(30 * 60 * 1000)
        {
            return Err(CoordinatorError::InvalidReport);
        }
        let key = approval_key(&attempt_id, generation, &request_id);
        if let Some(existing) = self.approvals.get(&key) {
            return if existing.kind == kind && existing.payload == payload {
                Ok(existing.clone())
            } else {
                Err(CoordinatorError::IdempotencyConflict)
            };
        }
        let record = ApprovalRequestRecord {
            attempt_id: attempt_id.clone(),
            generation,
            request_id: request_id.clone(),
            flow_id: flow_id.clone(),
            task_id: task.task_id.clone(),
            post_id: task.post_id.clone(),
            kind,
            reason: reason.clone(),
            payload,
            state: ApprovalRequestState::Pending,
            requested_at_ms: timestamp_ms(),
            deadline_at_ms,
            resolution_id: None,
            grant: None,
            resolution_reason: None,
        };
        self.flow_runs
            .get_mut(&flow_id)
            .expect("flow retained")
            .set_task_approval_wait(&task.task_id, &request_id, fence)
            .map_err(map_workflow_error)?;
        if let Some(activity) = self.activities.get_mut(&attempt.activity_id) {
            activity.status = ActivityStatus::WaitingForApproval;
            activity.waiting_reason = Some("worker approval request is pending".to_owned());
            activity.revision = activity.revision.saturating_add(1);
            activity.updated_at_ms = timestamp_ms();
        }
        let deadline =
            Instant::now() + Duration::from_millis(deadline_at_ms.saturating_sub(timestamp_ms()));
        self.approval_deadlines.insert(key.clone(), deadline);
        self.approvals.insert(key, record.clone());
        self.changed(
            "approvalRequested",
            Some(attempt.run_id),
            Some(attempt.activity_id),
            Some(attempt_id),
            None,
            json!({"flowId":flow_id,"taskId":task.task_id,"postId":task.post_id,"generation":generation,"requestId":request_id,"kind":record.kind,"state":"pending"}),
        )
        .await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(record)
        }
    }

    async fn resolve_worker_approval(
        &mut self,
        attempt_id: String,
        generation: u64,
        request_id: String,
        resolution_id: String,
        decision: ApprovalRequestState,
        grant: Option<String>,
        fence: u64,
    ) -> Result<ApprovalRequestRecord, CoordinatorError> {
        if !matches!(
            decision,
            ApprovalRequestState::Approved
                | ApprovalRequestState::Denied
                | ApprovalRequestState::Cancelled
                | ApprovalRequestState::Interrupted
        ) || resolution_id.is_empty()
            || resolution_id.len() > 128
            || grant
                .as_ref()
                .is_some_and(|value| !matches!(value.as_str(), "once" | "session"))
        {
            return Err(CoordinatorError::InvalidReport);
        }
        let key = approval_key(&attempt_id, generation, &request_id);
        let mut record = self
            .approvals
            .get(&key)
            .cloned()
            .ok_or(CoordinatorError::NotFound("approval request"))?;
        if let Some(existing_id) = record.resolution_id.as_deref() {
            return if existing_id == resolution_id
                && record.state == decision
                && record.grant == grant
            {
                Ok(record)
            } else {
                Err(CoordinatorError::IdempotencyConflict)
            };
        }
        let current_attempt = self
            .attempts
            .get(&attempt_id)
            .cloned()
            .ok_or(CoordinatorError::StaleGeneration)?;
        if current_attempt.generation != generation {
            return Err(CoordinatorError::StaleGeneration);
        }
        let flow = self
            .flow_runs
            .get(&record.flow_id)
            .ok_or(CoordinatorError::NotFound("FlowRun"))?;
        if flow.coordinator_fence_exhausted || flow.coordinator_fence != fence {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        if record.state != ApprovalRequestState::Pending {
            return Err(CoordinatorError::TerminalTransition);
        }
        record.state = decision;
        record.resolution_id = Some(resolution_id);
        record.grant = grant;
        record.resolution_reason = Some(
            match decision {
                ApprovalRequestState::Approved => "approved by coordinator",
                ApprovalRequestState::Denied => "denied by coordinator",
                ApprovalRequestState::Cancelled => "cancelled with the worker attempt",
                ApprovalRequestState::Interrupted => {
                    "worker process exited before approval was resolved"
                }
                _ => "resolved",
            }
            .to_owned(),
        );
        self.flow_runs
            .get_mut(&record.flow_id)
            .expect("flow retained")
            .resolve_task_approval_wait(
                &record.task_id,
                &record.request_id,
                decision == ApprovalRequestState::Approved,
                record.resolution_reason.clone(),
                fence,
            )
            .map_err(map_workflow_error)?;
        if let Some(activity) = self.activities.get_mut(&current_attempt.activity_id) {
            activity.status = if decision == ApprovalRequestState::Approved {
                ActivityStatus::Running
            } else {
                ActivityStatus::Blocked
            };
            activity.waiting_reason = record.resolution_reason.clone();
            activity.revision = activity.revision.saturating_add(1);
            activity.updated_at_ms = timestamp_ms();
        }
        self.approval_deadlines.remove(&key);
        self.approvals.insert(key, record.clone());
        self.changed(
            "approvalResolved",
            Some(current_attempt.run_id.clone()),
            Some(current_attempt.activity_id.clone()),
            Some(attempt_id),
            None,
            json!({"flowId":record.flow_id,"taskId":record.task_id,"requestId":record.request_id,"resolutionId":record.resolution_id,"decision":record.state}),
        )
        .await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(record)
        }
    }

    async fn request_worker_tool(
        &mut self,
        attempt_id: String,
        generation: u64,
        expected_fence: u64,
        request_id: String,
        tool_name: String,
        arguments: Value,
        deadline_at_ms: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        let request_bytes = serde_json::to_vec(&arguments).map_or(usize::MAX, |bytes| bytes.len());
        if request_bytes > 64 * 1024
            || request_id.is_empty()
            || request_id.len() > 128
            || tool_name.is_empty()
            || tool_name.len() > 256
        {
            return Err(CoordinatorError::InvalidReport);
        }
        let attempt = self
            .attempts
            .get(&attempt_id)
            .filter(|attempt| attempt.generation == generation)
            .cloned()
            .ok_or(CoordinatorError::StaleGeneration)?;
        if attempt.status.terminal() {
            return Err(CoordinatorError::TerminalTransition);
        }
        let (flow_id, fence, task) = self
            .flow_runs
            .values()
            .find_map(|flow| {
                flow.tasks.values().find_map(|task| {
                    (task.attempt_id.as_deref() == Some(attempt_id.as_str())
                        && task.generation == Some(generation))
                    .then(|| (flow.flow_id.clone(), flow.coordinator_fence, task.clone()))
                })
            })
            .ok_or(CoordinatorError::NotFound("FlowRun attempt"))?;
        if fence != expected_fence {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        if task.cancel_requested {
            return Err(CoordinatorError::TerminalTransition);
        }
        let now = timestamp_ms();
        if deadline_at_ms <= now || deadline_at_ms > now.saturating_add(30 * 60 * 1000) {
            return Err(CoordinatorError::InvalidReport);
        }
        let key = approval_key(&attempt_id, generation, &request_id);
        if let Some(existing) = self.tool_requests.get(&key) {
            return if existing.tool_name == tool_name && existing.arguments == arguments {
                Ok(existing.clone())
            } else {
                Err(CoordinatorError::IdempotencyConflict)
            };
        }
        if self.tool_requests.len() >= 32 {
            let oldest_terminal = self
                .tool_requests
                .iter()
                .filter(|(_, record)| record.state.terminal())
                .min_by_key(|(_, record)| record.requested_at_ms)
                .map(|(key, _)| key.clone());
            if let Some(oldest_terminal) = oldest_terminal {
                self.tool_requests.remove(&oldest_terminal);
            } else {
                return Err(CoordinatorError::InvalidReport);
            }
        }
        let record = WorkerToolRequestRecord {
            attempt_id: attempt_id.clone(),
            generation,
            request_id,
            flow_id: flow_id.clone(),
            task_id: task.task_id.clone(),
            post_id: task.post_id.clone(),
            coordinator_fence: fence,
            tool_name,
            arguments,
            state: WorkerToolRequestState::Pending,
            requested_at_ms: now,
            deadline_at_ms,
            claim_id: None,
            resolution_id: None,
            result: None,
            error: None,
        };
        self.flow_runs
            .get_mut(&flow_id)
            .expect("flow retained")
            .set_task_approval_wait(&task.task_id, &record.request_id, fence)
            .map_err(map_workflow_error)?;
        if let Some(activity) = self.activities.get_mut(&attempt.activity_id) {
            activity.status = ActivityStatus::WaitingForApproval;
            activity.waiting_reason = Some(format!("waiting for tool call {}", record.tool_name));
            activity.revision = activity.revision.saturating_add(1);
            activity.updated_at_ms = now;
        }
        let deadline = Instant::now() + Duration::from_millis(deadline_at_ms.saturating_sub(now));
        let key = approval_key(&attempt_id, generation, &record.request_id);
        self.tool_deadlines.insert(key.clone(), deadline);
        self.tool_requests.insert(key, record.clone());
        self.changed(
            "workerToolRequested",
            Some(attempt.run_id),
            Some(attempt.activity_id),
            Some(attempt_id),
            None,
            json!({"flowId":flow_id,"taskId":task.task_id,"postId":task.post_id,"generation":generation,"requestId":record.request_id,"toolName":record.tool_name,"state":"pending"}),
        )
        .await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(record)
        }
    }

    async fn claim_worker_tool(
        &mut self,
        attempt_id: String,
        generation: u64,
        request_id: String,
        claim_id: String,
        fence: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        if claim_id.is_empty() || claim_id.len() > 128 {
            return Err(CoordinatorError::InvalidReport);
        }
        let key = approval_key(&attempt_id, generation, &request_id);
        let mut record = self
            .tool_requests
            .get(&key)
            .cloned()
            .ok_or(CoordinatorError::NotFound("worker tool request"))?;
        if record.coordinator_fence != fence
            || self.flow_runs.get(&record.flow_id).is_none_or(|flow| {
                flow.coordinator_fence != fence || flow.coordinator_fence_exhausted
            })
        {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        if self
            .flow_runs
            .get(&record.flow_id)
            .and_then(|flow| flow.tasks.get(&record.task_id))
            .is_some_and(|task| task.cancel_requested)
        {
            return Err(CoordinatorError::TerminalTransition);
        }
        if let Some(existing) = record.claim_id.as_deref() {
            return if existing == claim_id {
                Ok(record)
            } else {
                Err(CoordinatorError::IdempotencyConflict)
            };
        }
        if record.state != WorkerToolRequestState::Pending {
            return Err(CoordinatorError::TerminalTransition);
        }
        record.claim_id = Some(claim_id);
        record.state = WorkerToolRequestState::Claimed;
        self.tool_requests.insert(key, record.clone());
        self.changed(
            "workerToolClaimed",
            None,
            None,
            Some(attempt_id),
            None,
            json!({"requestId":request_id,"generation":generation,"claimId":record.claim_id,"state":"claimed"}),
        )
        .await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(record)
        }
    }

    async fn resolve_worker_tool(
        &mut self,
        attempt_id: String,
        generation: u64,
        request_id: String,
        claim_id: String,
        resolution_id: String,
        result: Option<Value>,
        error: Option<String>,
        fence: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        let result_bytes = result
            .as_ref()
            .and_then(|value| serde_json::to_vec(value).ok())
            .map_or(0, |bytes| bytes.len());
        if result_bytes > 64 * 1024
            || claim_id.is_empty()
            || resolution_id.is_empty()
            || claim_id.len() > 128
            || resolution_id.len() > 128
            || error.as_ref().is_some_and(|value| value.len() > 4096)
        {
            return Err(CoordinatorError::InvalidReport);
        }
        let key = approval_key(&attempt_id, generation, &request_id);
        let mut record = self
            .tool_requests
            .get(&key)
            .cloned()
            .ok_or(CoordinatorError::NotFound("worker tool request"))?;
        let safe_error = error.as_deref().map(sanitize_approval_text);
        if record.coordinator_fence != fence
            || self.flow_runs.get(&record.flow_id).is_none_or(|flow| {
                flow.coordinator_fence != fence || flow.coordinator_fence_exhausted
            })
        {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        if self
            .flow_runs
            .get(&record.flow_id)
            .and_then(|flow| flow.tasks.get(&record.task_id))
            .is_some_and(|task| task.cancel_requested)
        {
            return Err(CoordinatorError::TerminalTransition);
        }
        if let Some(existing_id) = record.resolution_id.as_deref() {
            return if existing_id == resolution_id
                && record.claim_id.as_deref() == Some(claim_id.as_str())
                && record.result == result
                && record.error == safe_error
            {
                Ok(record)
            } else {
                Err(CoordinatorError::IdempotencyConflict)
            };
        }
        if record.state != WorkerToolRequestState::Claimed
            || record.claim_id.as_deref() != Some(claim_id.as_str())
        {
            return Err(CoordinatorError::TerminalTransition);
        }
        record.state = if error.is_some() {
            WorkerToolRequestState::Failed
        } else {
            WorkerToolRequestState::Completed
        };
        record.resolution_id = Some(resolution_id);
        record.result = result;
        record.error = safe_error;
        self.finish_worker_tool_record(key, record, true).await
    }

    async fn finish_worker_tool(
        &mut self,
        attempt_id: String,
        generation: u64,
        request_id: String,
        state: WorkerToolRequestState,
        error: String,
        fence: u64,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        if !matches!(
            state,
            WorkerToolRequestState::Cancelled
                | WorkerToolRequestState::Interrupted
                | WorkerToolRequestState::Unknown
        ) {
            return Err(CoordinatorError::InvalidReport);
        }
        let key = approval_key(&attempt_id, generation, &request_id);
        let mut record = self
            .tool_requests
            .get(&key)
            .cloned()
            .ok_or(CoordinatorError::NotFound("worker tool request"))?;
        if record.coordinator_fence != fence
            || self.flow_runs.get(&record.flow_id).is_none_or(|flow| {
                flow.coordinator_fence != fence || flow.coordinator_fence_exhausted
            })
        {
            return Err(CoordinatorError::Workflow("FenceMismatch".to_owned()));
        }
        if record.state.terminal() {
            return Ok(record);
        }
        record.state = state;
        record.error = Some(sanitize_approval_text(&error));
        record.resolution_id = Some(format!(
            "terminal:{}:{}:{}",
            attempt_id, generation, request_id
        ));
        self.finish_worker_tool_record(key, record, false).await
    }

    async fn finish_worker_tool_record(
        &mut self,
        key: String,
        record: WorkerToolRequestRecord,
        success: bool,
    ) -> Result<WorkerToolRequestRecord, CoordinatorError> {
        let other_pending = self.tool_requests.iter().any(|(other_key, other)| {
            other_key != &key
                && other.attempt_id == record.attempt_id
                && other.generation == record.generation
                && !other.state.terminal()
        }) || self.approvals.values().any(|approval| {
            approval.attempt_id == record.attempt_id
                && approval.generation == record.generation
                && approval.state == ApprovalRequestState::Pending
        });
        if !other_pending {
            if let Some(flow) = self.flow_runs.get_mut(&record.flow_id) {
                if flow.tasks.get(&record.task_id).is_some_and(|task| {
                    task.attempt_id.as_deref() == Some(record.attempt_id.as_str())
                        && task.generation == Some(record.generation)
                }) {
                    let _ = flow.resolve_task_approval_wait(
                        &record.task_id,
                        &record.request_id,
                        success,
                        record.error.clone(),
                        record.coordinator_fence,
                    );
                }
            }
        }
        if let Some(attempt) = self.attempts.get(&record.attempt_id) {
            if let Some(activity) = self.activities.get_mut(&attempt.activity_id) {
                activity.status = if success {
                    ActivityStatus::Running
                } else {
                    ActivityStatus::Blocked
                };
                activity.waiting_reason = record.error.clone();
                activity.revision = activity.revision.saturating_add(1);
                activity.updated_at_ms = timestamp_ms();
            }
        }
        self.tool_deadlines.remove(&key);
        self.tool_requests.insert(key, record.clone());
        self.changed(
            "workerToolResolved",
            None,
            None,
            Some(record.attempt_id.clone()),
            None,
            json!({"flowId":record.flow_id,"taskId":record.task_id,"requestId":record.request_id,"generation":record.generation,"state":record.state}),
        ).await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(record)
        }
    }

    async fn persist_flowrun_change(
        &mut self,
        flow_id: &str,
        action: &str,
    ) -> Result<FlowRunWorkflowState, CoordinatorError> {
        let state = self
            .flow_runs
            .get(flow_id)
            .cloned()
            .ok_or_else(|| CoordinatorError::Workflow("unknown FlowRun".to_owned()))?;
        self.changed(
            "flowRunWorkflowChanged",
            None,
            None,
            None,
            None,
            json!({"flowId":flow_id,"action":action}),
        )
        .await;
        if self.persistence_failed {
            Err(CoordinatorError::Persistence)
        } else {
            Ok(state)
        }
    }

    async fn run(mut self, mut commands: mpsc::Receiver<Command>, startup: watch::Sender<i8>) {
        let recovered_flow_ids = self.flow_runs.keys().cloned().collect::<Vec<_>>();
        for flow_id in recovered_flow_ids {
            self.active_request_id = format!("internal:restore-fence:{}", flow_id);
            self.active_command = json!({"type":"restoreFlowRunFence","flowId":flow_id});
            self.changed(
                "flowRunWorkflowChanged",
                None,
                None,
                None,
                None,
                json!({"flowId":flow_id,"action":"restoreFenceAdvanced"}),
            )
            .await;
            if self.persistence_failed {
                startup.send_replace(-1);
                return;
            }
        }
        startup.send_replace(1);
        loop {
            let next_deadline = self.next_deadline();
            if let Some(deadline) = next_deadline {
                tokio::select! {
                    command = commands.recv() => match command {
                        Some(command) => {
                            self.handle(command).await;
                            if self.persistence_failed { break; }
                        },
                        None => break,
                    },
                    _ = sleep_until(deadline) => {
                        self.active_request_id = format!("internal:{}:{}", self.manager_id, self.sequence.saturating_add(1));
                        self.active_command = json!({"type":"deadline"});
                        self.expire_deadlines(Instant::now()).await;
                        if self.persistence_failed { break; }
                    },
                }
            } else if let Some(command) = commands.recv().await {
                self.handle(command).await;
                if self.persistence_failed {
                    break;
                }
            } else {
                break;
            }
        }
    }

    async fn handle(&mut self, command: Command) {
        // Reservation commands must query the store's reservation state even
        // after completion: their stored receipt contains the launch
        // fingerprint, while `ReserveLaunch::durable_payload` is the wire
        // command wrapper. `reserve_command` performs the correct comparison.
        if !matches!(&command, Command::ReserveLaunch { .. }) {
            if let (Some(request_id), Some(store)) = (command.request_id(), self.store.as_ref()) {
                let request_id = request_id.to_owned();
                let store = Arc::clone(store);
                let receipt = tokio::task::spawn_blocking(move || {
                    store
                        .lock()
                        .map_err(|_| ())
                        .map(|store| store.receipt(&request_id).cloned())
                })
                .await;
                match receipt {
                    Ok(Ok(Some(receipt))) => {
                        if receipt.command != command.durable_payload() {
                            command.reply_error(CoordinatorError::IdempotencyConflict);
                        } else {
                            command.reply_cached(receipt.result);
                        }
                        return;
                    }
                    Ok(Ok(None)) => {}
                    _ => {
                        self.persistence_failed = true;
                        command.reply_error(CoordinatorError::Persistence);
                        return;
                    }
                }
            }
        }
        self.active_request_id = command.request_id().map(str::to_owned).unwrap_or_else(|| {
            format!(
                "internal:{}:{}",
                self.manager_id,
                self.sequence.saturating_add(1)
            )
        });
        self.active_command = command.durable_payload();
        match command {
            Command::RegisterRun { context, reply, .. } => {
                let now = timestamp_ms();
                let run = RunRecord {
                    run_id: uuid::Uuid::new_v4().to_string(),
                    manager_id: self.manager_id.clone(),
                    status: ExecutionStatus::Running,
                    context,
                    revision: 1,
                    updated_at_ms: now,
                };
                self.runs.insert(run.run_id.clone(), run.clone());
                self.changed(
                    "runRegistered",
                    Some(run.run_id.clone()),
                    None,
                    None,
                    None,
                    json!({}),
                )
                .await;
                let result = if self.persistence_failed {
                    Err(CoordinatorError::Persistence)
                } else {
                    Ok(run)
                };
                let _ = reply.send(result);
            }
            Command::RegisterActivity {
                run_id,
                kind,
                reply,
                ..
            } => {
                let result = if !self.runs.contains_key(&run_id) {
                    Err(CoordinatorError::InvalidParent("activity"))
                } else {
                    let now = timestamp_ms();
                    let activity = ActivityRecord {
                        activity_id: uuid::Uuid::new_v4().to_string(),
                        run_id,
                        kind,
                        status: ActivityStatus::Running,
                        health: Health::Unknown,
                        last_contact_at_ms: None,
                        waiting_reason: None,
                        revision: 1,
                        updated_at_ms: now,
                    };
                    self.activities
                        .insert(activity.activity_id.clone(), activity.clone());
                    self.changed(
                        "activityRegistered",
                        Some(activity.run_id.clone()),
                        Some(activity.activity_id.clone()),
                        None,
                        None,
                        json!({}),
                    )
                    .await;
                    if self.persistence_failed {
                        Err(CoordinatorError::Persistence)
                    } else {
                        Ok(activity)
                    }
                };
                let _ = reply.send(result);
            }
            Command::RegisterAttempt {
                activity_id, reply, ..
            } => {
                let result = if let Some(activity) = self.activities.get(&activity_id).cloned() {
                    let generation = self
                        .latest_generation
                        .entry(activity_id.clone())
                        .or_insert(0);
                    *generation = generation.saturating_add(1);
                    let now = timestamp_ms();
                    let attempt = AttemptRecord {
                        attempt_id: uuid::Uuid::new_v4().to_string(),
                        activity_id,
                        run_id: activity.run_id,
                        generation: *generation,
                        status: ExecutionStatus::Running,
                        cancellation: CancellationState::None,
                        process_ids: Vec::new(),
                        reports: Vec::new(),
                        revision: 1,
                        updated_at_ms: now,
                    };
                    self.attempts
                        .insert(attempt.attempt_id.clone(), attempt.clone());
                    self.changed(
                        "attemptRegistered",
                        Some(attempt.run_id.clone()),
                        Some(attempt.activity_id.clone()),
                        Some(attempt.attempt_id.clone()),
                        None,
                        json!({"generation":attempt.generation}),
                    )
                    .await;
                    if self.persistence_failed {
                        Err(CoordinatorError::Persistence)
                    } else {
                        Ok(attempt)
                    }
                } else {
                    Err(CoordinatorError::InvalidParent("attempt"))
                };
                let _ = reply.send(result);
            }
            Command::ReportAttempt {
                attempt_id,
                generation,
                status,
                detail,
                reply,
                ..
            } => {
                let mut result = self
                    .report_attempt(&attempt_id, generation, status, detail)
                    .await;
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::RequestCancel {
                attempt_id,
                generation,
                reply,
                ..
            } => {
                let prior_revision = self
                    .attempts
                    .get(&attempt_id)
                    .map(|attempt| attempt.revision);
                let mut result = self.update_attempt(&attempt_id, generation, |attempt| {
                    if attempt.status.terminal() {
                        return Ok(());
                    }
                    if attempt.cancellation == CancellationState::None {
                        attempt.cancellation = CancellationState::Requested;
                        attempt.revision = attempt.revision.saturating_add(1);
                        attempt.updated_at_ms = timestamp_ms();
                    }
                    Ok(())
                });
                if let Ok(attempt) = &result {
                    if prior_revision != Some(attempt.revision) {
                        self.changed(
                            "attemptCancelRequested",
                            Some(attempt.run_id.clone()),
                            Some(attempt.activity_id.clone()),
                            Some(attempt.attempt_id.clone()),
                            None,
                            json!({"cancellation":attempt.cancellation}),
                        )
                        .await;
                    }
                }
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::MarkCancelDelivered {
                attempt_id,
                generation,
                reply,
                ..
            } => {
                let prior_revision = self
                    .attempts
                    .get(&attempt_id)
                    .map(|attempt| attempt.revision);
                let mut result = self.update_attempt(&attempt_id, generation, |attempt| {
                    if attempt.status.terminal() {
                        return Ok(());
                    }
                    if attempt.cancellation == CancellationState::Requested {
                        attempt.cancellation = CancellationState::Delivered;
                        attempt.revision = attempt.revision.saturating_add(1);
                        attempt.updated_at_ms = timestamp_ms();
                    }
                    Ok(())
                });
                if let Ok(attempt) = &result {
                    if prior_revision != Some(attempt.revision) {
                        self.changed(
                            "attemptCancelDelivered",
                            Some(attempt.run_id.clone()),
                            Some(attempt.activity_id.clone()),
                            Some(attempt.attempt_id.clone()),
                            None,
                            json!({"cancellation":attempt.cancellation}),
                        )
                        .await;
                    }
                }
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::AssociateProcess {
                process_id,
                attempt_id,
                generation,
                reply,
                ..
            } => {
                let mut result = self
                    .associate_process(process_id, attempt_id, generation)
                    .await;
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::ProcessExited {
                process_id,
                exit_status,
                reply,
                ..
            } => {
                let mut result = self.process_exited(&process_id, exit_status).await;
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::SetActivityStatus {
                activity_id,
                reason,
                status,
                reply,
                ..
            } => {
                let mut result = self.set_activity_status(&activity_id, status, reason).await;
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::SetRunStatus {
                run_id,
                status,
                reply,
                ..
            } => {
                let mut result = self.set_run_status(&run_id, status).await;
                if self.persistence_failed {
                    result = Err(CoordinatorError::Persistence);
                }
                let _ = reply.send(result);
            }
            Command::Replay {
                epoch,
                cursor,
                reply,
            } => {
                let result = self.replay(&epoch, cursor);
                let _ = reply.send(Ok(result));
            }
            Command::SetActivityTimeout {
                request_id: _,
                activity_id,
                timeout_ms: _,
                deadline,
                reply,
            } => {
                let result = if let Some(activity) = self.activities.get(&activity_id) {
                    if activity.status.terminal() {
                        Err(CoordinatorError::TerminalTransition)
                    } else {
                        let generation = self.latest_generation.get(&activity_id).copied();
                        self.activity_timeouts
                            .insert(activity_id, (deadline, generation));
                        Ok(())
                    }
                } else {
                    Err(CoordinatorError::NotFound("activity"))
                };
                let _ = reply.send(result);
            }
            Command::ReserveLaunch {
                request_id,
                command_fingerprint,
                reply,
            } => {
                let result = self.reserve_launch(&request_id, command_fingerprint).await;
                let _ = reply.send(result);
            }
            Command::CompleteLaunchReservation {
                request_id,
                result,
                reply,
            } => {
                let result = self.complete_launch_reservation(&request_id, result).await;
                let _ = reply.send(result);
            }
            Command::RegisterFlowRun {
                projection,
                coordinator_id,
                reply,
                ..
            } => {
                let flow_id = projection.flow_id.clone();
                let result = if self.flow_runs.contains_key(&flow_id) {
                    Err(CoordinatorError::Workflow(
                        "FlowRun is already registered".to_owned(),
                    ))
                } else {
                    match FlowRunWorkflowState::from_projection(projection, coordinator_id, 0) {
                        Ok(state) => {
                            self.flow_runs.insert(flow_id.clone(), state);
                            self.persist_flowrun_change(&flow_id, "registered").await
                        }
                        Err(error) => Err(map_workflow_error(error)),
                    }
                };
                let _ = reply.send(result);
            }
            Command::ClaimFlowRun {
                flow_id,
                coordinator_id,
                expected_fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "claimed", |state| {
                        state.claim(coordinator_id, expected_fence).map(|_| ())
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::AdvanceFlowStage {
                flow_id,
                expected_stage,
                next_stage,
                expected_flow_revision,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "stageAdvanced", |state| {
                        state.advance_stage(
                            &expected_stage,
                            &next_stage,
                            expected_flow_revision,
                            fence,
                        )
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::CompleteFlowRun {
                flow_id,
                expected_stage,
                expected_flow_revision,
                evidence,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "flowCompleted", |state| {
                        state.complete_flowrun(
                            &expected_stage,
                            expected_flow_revision,
                            evidence,
                            fence,
                        )
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::BindFlowAttempt {
                flow_id,
                task_id,
                attempt_id,
                generation,
                source_cursor,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "attemptBound", |state| {
                        state.bind_attempt(&task_id, attempt_id, generation, source_cursor, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::ReconcileFlowAttempt {
                flow_id,
                task_id,
                outcome,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "attemptReconciled", |state| {
                        state
                            .reconcile_attempt(&task_id, outcome, fence)
                            .map(|_| ())
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::ApplyFlowReview {
                flow_id,
                task_id,
                flow_revision,
                evidence,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "reviewApplied", |state| {
                        state.apply_review_approved(&task_id, flow_revision, evidence, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::RequestFlowTaskCancel {
                flow_id,
                task_id,
                attempt_id,
                generation,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "taskCancelRequested", |state| {
                        state.request_task_cancel(&task_id, &attempt_id, generation, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::AcknowledgeFlowboardEvent {
                flow_id,
                event_id,
                new_flow_revision,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "outboxAcknowledged", |state| {
                        state.acknowledge_outbox(&event_id, new_flow_revision, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::RebaseFlowboardOutbox {
                flow_id,
                flow_revision,
                existing_event_ids,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "outboxRebased", |state| {
                        state.rebase_outbox(flow_revision, &existing_event_ids, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::ReserveFlowDispatch {
                flow_id,
                task_id,
                fence,
                reply,
                ..
            } => {
                let reserved = if let Some(state) = self.flow_runs.get_mut(&flow_id) {
                    state
                        .reserve_dispatch(&task_id, fence)
                        .map(|_| ())
                        .map_err(map_workflow_error)
                } else {
                    Err(CoordinatorError::Workflow("unknown FlowRun".to_owned()))
                };
                let result = match reserved {
                    Ok(()) => {
                        self.persist_flowrun_change(&flow_id, "dispatchReserved")
                            .await
                    }
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Command::RegisterFlowWorkerAttempt {
                flow_id,
                task_id,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .register_flow_worker_attempt(&flow_id, &task_id, fence)
                    .await;
                let _ = reply.send(result);
            }
            Command::RequestWorkerApproval {
                attempt_id,
                generation,
                fence,
                request_id,
                kind,
                reason,
                payload,
                deadline_at_ms,
                reply,
                ..
            } => {
                let result = self
                    .request_worker_approval(
                        attempt_id,
                        generation,
                        fence,
                        request_id,
                        kind,
                        reason,
                        payload,
                        deadline_at_ms,
                    )
                    .await;
                let _ = reply.send(result);
            }
            Command::ResolveWorkerApproval {
                attempt_id,
                generation,
                request_id,
                resolution_id,
                decision,
                grant,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .resolve_worker_approval(
                        attempt_id,
                        generation,
                        request_id,
                        resolution_id,
                        decision,
                        grant,
                        fence,
                    )
                    .await;
                let _ = reply.send(result);
            }
            Command::RequestWorkerTool {
                attempt_id,
                generation,
                fence,
                request_id,
                tool_name,
                arguments,
                deadline_at_ms,
                reply,
                ..
            } => {
                let result = self
                    .request_worker_tool(
                        attempt_id,
                        generation,
                        fence,
                        request_id,
                        tool_name,
                        arguments,
                        deadline_at_ms,
                    )
                    .await;
                let _ = reply.send(result);
            }
            Command::ClaimWorkerTool {
                attempt_id,
                generation,
                request_id,
                claim_id,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .claim_worker_tool(attempt_id, generation, request_id, claim_id, fence)
                    .await;
                let _ = reply.send(result);
            }
            Command::ResolveWorkerTool {
                attempt_id,
                generation,
                request_id,
                claim_id,
                resolution_id,
                result,
                error,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .resolve_worker_tool(
                        attempt_id,
                        generation,
                        request_id,
                        claim_id,
                        resolution_id,
                        result,
                        error,
                        fence,
                    )
                    .await;
                let _ = reply.send(result);
            }
            Command::FinishWorkerTool {
                attempt_id,
                generation,
                request_id,
                state,
                error,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .finish_worker_tool(attempt_id, generation, request_id, state, error, fence)
                    .await;
                let _ = reply.send(result);
            }
            Command::CompleteFlowDispatch {
                flow_id,
                dispatch,
                receipt,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "dispatchStarted", |state| {
                        state.complete_dispatch(&dispatch, receipt, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
            Command::ReleaseFlowDispatch {
                flow_id,
                dispatch,
                fence,
                reply,
                ..
            } => {
                let result = self
                    .update_flowrun(&flow_id, "dispatchReleased", |state| {
                        state.release_dispatch_before_start(&dispatch, fence)
                    })
                    .await;
                let _ = reply.send(result);
            }
        }
    }

    async fn report_attempt(
        &mut self,
        attempt_id: &str,
        generation: u64,
        status: ExecutionStatus,
        detail: Value,
    ) -> Result<AttemptRecord, CoordinatorError> {
        let detail_size = serde_json::to_vec(&detail).map_or(usize::MAX, |bytes| bytes.len());
        if detail_size > MAX_REPORT_DETAIL_BYTES {
            return Err(CoordinatorError::InvalidReport);
        }
        let prior_revision = self
            .attempts
            .get(attempt_id)
            .ok_or(CoordinatorError::NotFound("attempt"))?
            .revision;
        let attempt = self.update_attempt(attempt_id, generation, |attempt| {
            if attempt.status.terminal() {
                return if attempt.status == status {
                    Ok(())
                } else {
                    Err(CoordinatorError::TerminalTransition)
                };
            }
            attempt.status = status;
            if status == ExecutionStatus::Cancelled
                && attempt.cancellation != CancellationState::None
            {
                attempt.cancellation = CancellationState::Completed;
            } else if status == ExecutionStatus::Failed
                && matches!(
                    attempt.cancellation,
                    CancellationState::Requested | CancellationState::Delivered
                )
            {
                attempt.cancellation = CancellationState::Failed;
            }
            attempt.revision = attempt.revision.saturating_add(1);
            attempt.updated_at_ms = timestamp_ms();
            attempt.reports.push(AttemptReport {
                status,
                detail: detail.clone(),
                revision: attempt.revision,
                reported_at_ms: attempt.updated_at_ms,
            });
            if attempt.reports.len() > MAX_RETAINED_REPORTS {
                attempt.reports.remove(0);
            }
            Ok(())
        })?;
        if attempt.revision != prior_revision {
            self.touch_activity(&attempt.activity_id, None);
            if let Some(activity) = self.activities.get_mut(&attempt.activity_id) {
                activity.status = match status {
                    ExecutionStatus::Queued => ActivityStatus::Queued,
                    ExecutionStatus::Running => ActivityStatus::Running,
                    ExecutionStatus::Succeeded => ActivityStatus::Succeeded,
                    ExecutionStatus::Failed => ActivityStatus::Failed,
                    ExecutionStatus::Cancelled => ActivityStatus::Cancelled,
                    ExecutionStatus::Blocked => ActivityStatus::Blocked,
                    ExecutionStatus::Unknown => ActivityStatus::Unknown,
                };
                activity.waiting_reason = detail
                    .get("reasonCode")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                activity.revision = activity.revision.saturating_add(1);
                activity.updated_at_ms = timestamp_ms();
            }
            if let Some(run) = self.runs.get_mut(&attempt.run_id) {
                if status.terminal() || status == ExecutionStatus::Running {
                    run.status = status;
                    run.revision = run.revision.saturating_add(1);
                    run.updated_at_ms = timestamp_ms();
                }
            }
            if detail.get("flowRunId").is_none() {
                self.reconcile_workflow_attempt(&attempt, status, &detail);
            }
            self.changed(
                "attemptReported",
                Some(attempt.run_id.clone()),
                Some(attempt.activity_id.clone()),
                Some(attempt.attempt_id.clone()),
                None,
                json!({"status":status,"generation":generation,"detail":detail}),
            )
            .await;
        }
        Ok(attempt)
    }

    fn update_attempt(
        &mut self,
        attempt_id: &str,
        generation: u64,
        update: impl FnOnce(&mut AttemptRecord) -> Result<(), CoordinatorError>,
    ) -> Result<AttemptRecord, CoordinatorError> {
        let attempt = self
            .attempts
            .get_mut(attempt_id)
            .ok_or(CoordinatorError::NotFound("attempt"))?;
        if attempt.generation != generation
            || self.latest_generation.get(&attempt.activity_id).copied() != Some(generation)
        {
            return Err(CoordinatorError::StaleGeneration);
        }
        update(attempt)?;
        Ok(attempt.clone())
    }

    async fn associate_process(
        &mut self,
        process_id: String,
        attempt_id: Option<String>,
        generation: Option<u64>,
    ) -> Result<ProcessRecord, CoordinatorError> {
        let now = timestamp_ms();
        let (run_id, activity_id) = match (attempt_id.as_deref(), generation) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(CoordinatorError::InvalidParent("process"));
            }
            (Some(id), Some(generation)) => {
                let attempt = self
                    .attempts
                    .get_mut(id)
                    .ok_or(CoordinatorError::NotFound("attempt"))?;
                if generation != attempt.generation
                    || self.latest_generation.get(&attempt.activity_id).copied()
                        != Some(attempt.generation)
                {
                    return Err(CoordinatorError::StaleGeneration);
                }
                if !attempt.process_ids.contains(&process_id) {
                    attempt.process_ids.push(process_id.clone());
                    attempt.revision = attempt.revision.saturating_add(1);
                    attempt.updated_at_ms = now;
                }
                (
                    Some(attempt.run_id.clone()),
                    Some(attempt.activity_id.clone()),
                )
            }
            (None, None) => (None, None),
        };
        let process = ProcessRecord {
            process_id: process_id.clone(),
            run_id: run_id.clone(),
            attempt_id: attempt_id.clone(),
            alive: true,
            exit_status: None,
            revision: self
                .processes
                .get(&process_id)
                .map_or(1, |p| p.revision.saturating_add(1)),
            updated_at_ms: now,
        };
        self.processes.insert(process_id.clone(), process.clone());
        self.changed(
            "processStarted",
            run_id,
            activity_id,
            attempt_id,
            Some(process_id),
            json!({"alive":true}),
        )
        .await;
        Ok(process)
    }

    async fn process_exited(
        &mut self,
        process_id: &str,
        exit_status: String,
    ) -> Result<ProcessRecord, CoordinatorError> {
        let process = self
            .processes
            .get_mut(process_id)
            .ok_or(CoordinatorError::NotFound("process"))?;
        if !process.alive {
            return Ok(process.clone());
        }
        process.alive = false;
        process.exit_status = Some(exit_status.clone());
        process.revision = process.revision.saturating_add(1);
        process.updated_at_ms = timestamp_ms();
        let record = process.clone();
        if let Some(attempt_id) = record.attempt_id.as_deref() {
            let activity_id = self
                .attempts
                .get(attempt_id)
                .map(|attempt| attempt.activity_id.clone());
            if let Some(activity_id) = activity_id {
                self.touch_activity(&activity_id, Some(Health::Unknown));
            }
        }
        let source_cursor = self.sequence.saturating_add(1);
        for flow in self.flow_runs.values_mut() {
            let task_id = flow.tasks.values().find_map(|task| {
                (task.process_id.as_deref() == Some(record.process_id.as_str())
                    || task.attempt_id.as_deref() == record.attempt_id.as_deref())
                .then(|| task.task_id.clone())
            });
            if let Some(task_id) = task_id {
                let _ = flow.observe_process_exit(
                    &task_id,
                    record.process_id.clone(),
                    exit_status.clone(),
                    source_cursor,
                    flow.coordinator_fence,
                );
            }
        }
        self.changed(
            "processExited",
            record.run_id.clone(),
            None,
            record.attempt_id.clone(),
            Some(record.process_id.clone()),
            json!({"exitStatus":exit_status}),
        )
        .await;
        Ok(record)
    }

    async fn set_activity_status(
        &mut self,
        activity_id: &str,
        status: ActivityStatus,
        reason: Option<String>,
    ) -> Result<ActivityRecord, CoordinatorError> {
        let activity = self
            .activities
            .get_mut(activity_id)
            .ok_or(CoordinatorError::NotFound("activity"))?;
        if activity.status.terminal() {
            return if activity.status == status {
                Ok(activity.clone())
            } else {
                Err(CoordinatorError::TerminalTransition)
            };
        }
        activity.status = status;
        activity.waiting_reason = reason;
        activity.revision = activity.revision.saturating_add(1);
        activity.updated_at_ms = timestamp_ms();
        let record = activity.clone();
        let health = if status == ActivityStatus::Unknown {
            Health::Unknown
        } else {
            Health::Healthy
        };
        self.touch_activity(activity_id, Some(health));
        self.changed(
            "activityStatusChanged",
            Some(record.run_id.clone()),
            Some(record.activity_id.clone()),
            None,
            None,
            json!({"status":status,"waitingReason":record.waiting_reason}),
        )
        .await;
        Ok(record)
    }

    async fn set_run_status(
        &mut self,
        run_id: &str,
        status: ExecutionStatus,
    ) -> Result<RunRecord, CoordinatorError> {
        let run = self
            .runs
            .get_mut(run_id)
            .ok_or(CoordinatorError::NotFound("run"))?;
        if run.status.terminal() {
            return if run.status == status {
                Ok(run.clone())
            } else {
                Err(CoordinatorError::TerminalTransition)
            };
        }
        run.status = status;
        run.revision = run.revision.saturating_add(1);
        run.updated_at_ms = timestamp_ms();
        let record = run.clone();
        self.changed(
            "runStatusChanged",
            Some(record.run_id.clone()),
            None,
            None,
            None,
            json!({"status":status}),
        )
        .await;
        Ok(record)
    }

    fn touch_activity(&mut self, activity_id: &str, health: Option<Health>) {
        if let Some(activity) = self.activities.get_mut(activity_id) {
            let now = Instant::now();
            activity.last_contact_at_ms = Some(timestamp_ms());
            if let Some(health) = health {
                activity.health = health;
            } else if !activity.status.terminal() {
                activity.health = Health::Healthy;
            }
            activity.revision = activity.revision.saturating_add(1);
            activity.updated_at_ms = timestamp_ms();
            let terminal = activity.status.terminal();
            if !terminal && activity.health == Health::Healthy {
                self.activity_contact_deadlines
                    .insert(activity_id.to_owned(), now + HEALTH_STALE_AFTER);
            } else if activity.health != Health::Healthy {
                self.activity_contact_deadlines.remove(activity_id);
            }
            if terminal {
                self.activity_contact_deadlines.remove(activity_id);
                self.activity_timeouts.remove(activity_id);
            }
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.activity_contact_deadlines
            .values()
            .copied()
            .chain(
                self.activity_timeouts
                    .values()
                    .map(|(deadline, _)| *deadline),
            )
            .chain(self.approval_deadlines.values().copied())
            .chain(self.tool_deadlines.values().copied())
            .min()
    }

    async fn expire_deadlines(&mut self, now: Instant) {
        let expired_health = self
            .activity_contact_deadlines
            .iter()
            .filter_map(|(id, deadline)| (*deadline <= now).then_some(id.clone()))
            .collect::<Vec<_>>();
        let stale_timeouts = self
            .activity_timeouts
            .iter()
            .filter_map(|(id, (_, generation))| {
                (self.latest_generation.get(id).copied() != *generation).then_some(id.clone())
            })
            .collect::<Vec<_>>();
        for id in stale_timeouts {
            self.activity_timeouts.remove(&id);
        }
        for id in expired_health {
            self.activity_contact_deadlines.remove(&id);
            let stale_run = if let Some(activity) = self.activities.get_mut(&id) {
                if !activity.status.terminal() && activity.health == Health::Healthy {
                    activity.health = Health::Stale;
                    activity.revision = activity.revision.saturating_add(1);
                    activity.updated_at_ms = timestamp_ms();
                    Some(activity.run_id.clone())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(run_id) = stale_run {
                self.changed(
                    "activityHealthStale",
                    Some(run_id),
                    Some(id),
                    None,
                    None,
                    json!({"health":"stale"}),
                )
                .await;
                if self.persistence_failed {
                    break;
                }
            }
        }
        let expired_timeouts = self
            .activity_timeouts
            .iter()
            .filter_map(|(id, (deadline, generation))| {
                (*deadline <= now && self.latest_generation.get(id).copied() == *generation)
                    .then_some(id.clone())
            })
            .collect::<Vec<_>>();
        for id in expired_timeouts {
            self.activity_timeouts.remove(&id);
            let timed_out_run = if let Some(activity) = self.activities.get_mut(&id) {
                if !activity.status.terminal() {
                    activity.status = ActivityStatus::TimedOut;
                    activity.health = Health::Stale;
                    activity.revision = activity.revision.saturating_add(1);
                    activity.updated_at_ms = timestamp_ms();
                    Some(activity.run_id.clone())
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(run_id) = timed_out_run {
                self.activity_contact_deadlines.remove(&id);
                self.changed(
                    "activityTimedOut",
                    Some(run_id),
                    Some(id),
                    None,
                    None,
                    json!({"status":"timedOut","health":"stale"}),
                )
                .await;
                if self.persistence_failed {
                    break;
                }
            }
        }
        let expired_approvals = self
            .approval_deadlines
            .iter()
            .filter_map(|(key, deadline)| (*deadline <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in expired_approvals {
            self.approval_deadlines.remove(&key);
            let Some(mut approval) = self.approvals.get(&key).cloned() else {
                continue;
            };
            if approval.state != ApprovalRequestState::Pending {
                continue;
            }
            approval.state = ApprovalRequestState::TimedOut;
            approval.resolution_reason = Some("approval request deadline elapsed".to_owned());
            if let Some(flow) = self.flow_runs.get_mut(&approval.flow_id) {
                let _ = flow.resolve_task_approval_wait(
                    &approval.task_id,
                    &approval.request_id,
                    false,
                    approval.resolution_reason.clone(),
                    flow.coordinator_fence,
                );
            }
            if let Some(attempt) = self.attempts.get(&approval.attempt_id).cloned() {
                if let Some(activity) = self.activities.get_mut(&attempt.activity_id) {
                    activity.status = ActivityStatus::Blocked;
                    activity.waiting_reason = Some("approval request timed out".to_owned());
                    activity.revision = activity.revision.saturating_add(1);
                    activity.updated_at_ms = timestamp_ms();
                }
            }
            self.approvals.insert(key, approval.clone());
            self.changed(
                "approvalTimedOut",
                None,
                None,
                Some(approval.attempt_id),
                None,
                json!({"flowId":approval.flow_id,"taskId":approval.task_id,"requestId":approval.request_id,"generation":approval.generation,"state":"timedOut"}),
            )
            .await;
            if self.persistence_failed {
                break;
            }
        }
        let expired_tools = self
            .tool_deadlines
            .iter()
            .filter_map(|(key, deadline)| (*deadline <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in expired_tools {
            self.tool_deadlines.remove(&key);
            let Some(mut request) = self.tool_requests.get(&key).cloned() else {
                continue;
            };
            if request.state.terminal() {
                continue;
            }
            if request.state == WorkerToolRequestState::Claimed {
                request.state = WorkerToolRequestState::Unknown;
                request.error = Some("claimed tool request timed out; side effect may have occurred and must not be replayed".to_owned());
            } else {
                request.state = WorkerToolRequestState::TimedOut;
                request.error =
                    Some("worker tool request timed out before it was claimed".to_owned());
            }
            let other_pending = self.tool_requests.iter().any(|(other_key, other)| {
                other_key != &key
                    && other.attempt_id == request.attempt_id
                    && other.generation == request.generation
                    && !other.state.terminal()
            }) || self.approvals.values().any(|approval| {
                approval.attempt_id == request.attempt_id
                    && approval.generation == request.generation
                    && approval.state == ApprovalRequestState::Pending
            });
            if !other_pending {
                if let Some(flow) = self.flow_runs.get_mut(&request.flow_id) {
                    let _ = flow.resolve_task_approval_wait(
                        &request.task_id,
                        &request.request_id,
                        false,
                        request.error.clone(),
                        request.coordinator_fence,
                    );
                }
            }
            if !other_pending {
                if let Some(attempt) = self.attempts.get(&request.attempt_id) {
                    if let Some(activity) = self.activities.get_mut(&attempt.activity_id) {
                        activity.status = ActivityStatus::Blocked;
                        activity.waiting_reason = request.error.clone();
                        activity.revision = activity.revision.saturating_add(1);
                        activity.updated_at_ms = timestamp_ms();
                    }
                }
            }
            self.tool_requests.insert(key, request.clone());
            let kind = if request.state == WorkerToolRequestState::Unknown {
                "workerToolOutcomeUnknown"
            } else {
                "workerToolTimedOut"
            };
            self.changed(kind, None, None, Some(request.attempt_id.clone()), None,
                json!({"flowId":request.flow_id,"taskId":request.task_id,"requestId":request.request_id,"generation":request.generation,"state":request.state,"error":request.error})).await;
            if self.persistence_failed {
                break;
            }
        }
        if !self.persistence_failed {
            self.publish_snapshot();
        }
    }

    async fn changed(
        &mut self,
        kind: &str,
        run_id: Option<String>,
        activity_id: Option<String>,
        attempt_id: Option<String>,
        process_id: Option<String>,
        detail: Value,
    ) {
        self.revision = self.revision.saturating_add(1);
        self.sequence = self.sequence.saturating_add(1);
        let event = CoordinatorEvent {
            sequence: self.sequence,
            cursor: self.sequence,
            epoch: self.manager_id.clone(),
            kind: kind.to_owned(),
            run_id,
            activity_id,
            attempt_id,
            process_id,
            revision: self.revision,
            occurred_at_ms: timestamp_ms(),
            detail,
        };
        self.retained_events.push_back(event.clone());
        while self.retained_events.len() > MAX_RETAINED_COORDINATOR_EVENTS {
            self.retained_events.pop_front();
        }
        let snapshot = self.build_snapshot();
        if let Some(store) = self.store.as_mut() {
            let store = Arc::clone(store);
            let request_id = if self.active_request_id.starts_with("internal:") {
                format!("{}:{}", self.active_request_id, self.sequence)
            } else {
                self.active_request_id.clone()
            };
            let command = self.active_command.clone();
            let event_value = serde_json::to_value(&event).unwrap_or(Value::Null);
            let snapshot_value = serde_json::to_value(snapshot).unwrap_or(Value::Null);
            let result_value = self.result_for_event(&event);
            let persisted = tokio::task::spawn_blocking(move || {
                store
                    .lock()
                    .map_err(|_| ())?
                    .commit(
                        &request_id,
                        command,
                        event_value,
                        snapshot_value,
                        result_value,
                    )
                    .map_err(|_| ())
            })
            .await;
            if !matches!(persisted, Ok(Ok(_))) {
                self.persistence_failed = true;
                return;
            }
        }
        let _ = self.event_tx.send(event);
        self.publish_snapshot();
    }

    fn replay(&self, epoch: &str, cursor: u64) -> CoordinatorReplay {
        let oldest = self.retained_events.front().map(|event| event.cursor);
        let history_gap = oldest.is_some_and(|oldest| cursor.saturating_add(1) < oldest);
        let resync_required = epoch != self.manager_id || history_gap || cursor > self.sequence;
        CoordinatorReplay {
            epoch: self.manager_id.clone(),
            cursor: self.sequence,
            resync_required,
            events: if resync_required {
                Vec::new()
            } else {
                self.retained_events
                    .iter()
                    .filter(|event| event.cursor > cursor)
                    .cloned()
                    .collect()
            },
        }
    }

    fn publish_snapshot(&self) {
        self.snapshot_tx.send_replace(self.build_snapshot());
    }

    fn build_snapshot(&self) -> CoordinatorSnapshot {
        let active_runs = self
            .runs
            .values()
            .filter(|run| !run.status.terminal())
            .count();
        let active_activities = self
            .activities
            .values()
            .filter(|activity| !activity.status.terminal())
            .count();
        let active_attempts = self
            .attempts
            .values()
            .filter(|attempt| !attempt.status.terminal())
            .count();
        let active_processes = self
            .processes
            .values()
            .filter(|process| process.alive)
            .count();
        CoordinatorSnapshot {
            manager_id: self.manager_id.clone(),
            epoch: self.manager_id.clone(),
            cursor: self.sequence,
            revision: self.revision,
            runs: self.runs.values().cloned().collect(),
            activities: self.activities.values().cloned().collect(),
            attempts: self.attempts.values().cloned().collect(),
            processes: self.processes.values().cloned().collect(),
            flow_runs: self.flow_runs.values().cloned().collect(),
            approvals: self.approvals.values().cloned().collect(),
            tool_requests: self.tool_requests.values().cloned().collect(),
            counts: CoordinatorCounts {
                runs: self.runs.len(),
                active_runs,
                activities: self.activities.len(),
                active_activities,
                attempts: self.attempts.len(),
                active_attempts,
                processes: self.processes.len(),
                active_processes,
            },
            recovery: self.recovery.clone(),
        }
    }

    fn result_for_event(&self, event: &CoordinatorEvent) -> Value {
        if event.kind == "flowRunWorkflowChanged" {
            if let Some(flow_id) = event.detail.get("flowId").and_then(Value::as_str) {
                if let Some(state) = self.flow_runs.get(flow_id) {
                    return serde_json::to_value(state).unwrap_or(Value::Null);
                }
            }
        }
        if event.kind.starts_with("run") {
            if let Some(record) = event.run_id.as_ref().and_then(|id| self.runs.get(id)) {
                return serde_json::to_value(record).unwrap_or(Value::Null);
            }
        }
        if event.kind.starts_with("activity") {
            if let Some(record) = event
                .activity_id
                .as_ref()
                .and_then(|id| self.activities.get(id))
            {
                return serde_json::to_value(record).unwrap_or(Value::Null);
            }
        }
        if event.kind.starts_with("attempt") {
            if let Some(record) = event
                .attempt_id
                .as_ref()
                .and_then(|id| self.attempts.get(id))
            {
                return serde_json::to_value(record).unwrap_or(Value::Null);
            }
        }
        if event.kind.starts_with("process") {
            if let Some(record) = event
                .process_id
                .as_ref()
                .and_then(|id| self.processes.get(id))
            {
                return serde_json::to_value(record).unwrap_or(Value::Null);
            }
        }
        Value::Null
    }
}

impl Command {
    fn reply_error(self, error: CoordinatorError) {
        match self {
            Self::RegisterRun { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RegisterActivity { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RegisterAttempt { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ReportAttempt { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RequestCancel { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::MarkCancelDelivered { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::AssociateProcess { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ProcessExited { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::SetActivityStatus { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::SetRunStatus { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::Replay { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::SetActivityTimeout { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ReserveLaunch { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::CompleteLaunchReservation { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RegisterFlowRun { reply, .. }
            | Self::ClaimFlowRun { reply, .. }
            | Self::AdvanceFlowStage { reply, .. }
            | Self::CompleteFlowRun { reply, .. }
            | Self::BindFlowAttempt { reply, .. }
            | Self::ReconcileFlowAttempt { reply, .. }
            | Self::ApplyFlowReview { reply, .. }
            | Self::RequestFlowTaskCancel { reply, .. }
            | Self::AcknowledgeFlowboardEvent { reply, .. }
            | Self::RebaseFlowboardOutbox { reply, .. }
            | Self::ReserveFlowDispatch { reply, .. }
            | Self::CompleteFlowDispatch { reply, .. }
            | Self::ReleaseFlowDispatch { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RegisterFlowWorkerAttempt { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RequestWorkerApproval { reply, .. }
            | Self::ResolveWorkerApproval { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RequestWorkerTool { reply, .. }
            | Self::ClaimWorkerTool { reply, .. }
            | Self::ResolveWorkerTool { reply, .. }
            | Self::FinishWorkerTool { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn reply_cached(self, value: Value) {
        fn decode<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, CoordinatorError> {
            serde_json::from_value(value).map_err(|_| CoordinatorError::Persistence)
        }
        match self {
            Self::RegisterRun { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::RegisterActivity { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::RegisterAttempt { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::ReportAttempt { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::RequestCancel { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::MarkCancelDelivered { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::AssociateProcess { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::ProcessExited { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::SetActivityStatus { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::SetRunStatus { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::Replay { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::SetActivityTimeout { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::ReserveLaunch { reply, .. } => {
                let _ = reply.send(Err(CoordinatorError::Persistence));
            }
            Self::CompleteLaunchReservation { reply, .. } => {
                let _ = reply.send(Err(CoordinatorError::Persistence));
            }
            Self::RegisterFlowRun { reply, .. }
            | Self::ClaimFlowRun { reply, .. }
            | Self::AdvanceFlowStage { reply, .. }
            | Self::CompleteFlowRun { reply, .. }
            | Self::BindFlowAttempt { reply, .. }
            | Self::ReconcileFlowAttempt { reply, .. }
            | Self::ApplyFlowReview { reply, .. }
            | Self::RequestFlowTaskCancel { reply, .. }
            | Self::AcknowledgeFlowboardEvent { reply, .. }
            | Self::RebaseFlowboardOutbox { reply, .. }
            | Self::ReserveFlowDispatch { reply, .. }
            | Self::CompleteFlowDispatch { reply, .. }
            | Self::ReleaseFlowDispatch { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::RegisterFlowWorkerAttempt { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::RequestWorkerApproval { reply, .. }
            | Self::ResolveWorkerApproval { reply, .. } => {
                let _ = reply.send(decode(value));
            }
            Self::RequestWorkerTool { reply, .. }
            | Self::ClaimWorkerTool { reply, .. }
            | Self::ResolveWorkerTool { reply, .. }
            | Self::FinishWorkerTool { reply, .. } => {
                let _ = reply.send(decode(value));
            }
        }
    }

    fn request_id(&self) -> Option<&str> {
        match self {
            Self::RegisterRun { request_id, .. }
            | Self::RegisterActivity { request_id, .. }
            | Self::RegisterAttempt { request_id, .. }
            | Self::ReportAttempt { request_id, .. }
            | Self::RequestCancel { request_id, .. }
            | Self::MarkCancelDelivered { request_id, .. }
            | Self::AssociateProcess { request_id, .. }
            | Self::ProcessExited { request_id, .. }
            | Self::SetActivityStatus { request_id, .. }
            | Self::SetRunStatus { request_id, .. } => Some(request_id),
            Self::RegisterFlowRun { request_id, .. }
            | Self::ClaimFlowRun { request_id, .. }
            | Self::AdvanceFlowStage { request_id, .. }
            | Self::CompleteFlowRun { request_id, .. }
            | Self::BindFlowAttempt { request_id, .. }
            | Self::ReconcileFlowAttempt { request_id, .. }
            | Self::ApplyFlowReview { request_id, .. }
            | Self::RequestFlowTaskCancel { request_id, .. }
            | Self::AcknowledgeFlowboardEvent { request_id, .. }
            | Self::RebaseFlowboardOutbox { request_id, .. }
            | Self::ReserveFlowDispatch { request_id, .. }
            | Self::CompleteFlowDispatch { request_id, .. }
            | Self::ReleaseFlowDispatch { request_id, .. } => Some(request_id),
            Self::RegisterFlowWorkerAttempt { request_id, .. } => Some(request_id),
            Self::RequestWorkerApproval { command_id, .. }
            | Self::ResolveWorkerApproval { command_id, .. }
            | Self::RequestWorkerTool { command_id, .. }
            | Self::ClaimWorkerTool { command_id, .. }
            | Self::ResolveWorkerTool { command_id, .. }
            | Self::FinishWorkerTool { command_id, .. } => Some(command_id),
            Self::Replay { .. }
            | Self::ReserveLaunch { .. }
            | Self::CompleteLaunchReservation { .. } => None,
            Self::SetActivityTimeout { request_id, .. } => Some(request_id),
        }
    }

    fn durable_payload(&self) -> Value {
        match self {
            Self::RegisterRun { context, .. } => json!({"type":"registerRun","context":context}),
            Self::RegisterActivity { run_id, kind, .. } => {
                json!({"type":"registerActivity","runId":run_id,"kind":kind})
            }
            Self::RegisterAttempt { activity_id, .. } => {
                json!({"type":"registerAttempt","activityId":activity_id})
            }
            Self::ReportAttempt {
                attempt_id,
                generation,
                status,
                detail,
                ..
            } => {
                json!({"type":"reportAttempt","attemptId":attempt_id,"generation":generation,"status":status,"detail":detail})
            }
            Self::RequestCancel {
                attempt_id,
                generation,
                ..
            } => json!({"type":"requestCancel","attemptId":attempt_id,"generation":generation}),
            Self::MarkCancelDelivered {
                attempt_id,
                generation,
                ..
            } => {
                json!({"type":"markCancelDelivered","attemptId":attempt_id,"generation":generation})
            }
            Self::AssociateProcess {
                process_id,
                attempt_id,
                generation,
                ..
            } => {
                json!({"type":"associateProcess","processId":process_id,"attemptId":attempt_id,"generation":generation})
            }
            Self::ProcessExited {
                process_id,
                exit_status,
                ..
            } => json!({"type":"processExited","processId":process_id,"exitStatus":exit_status}),
            Self::SetActivityStatus {
                activity_id,
                reason,
                status,
                ..
            } => {
                json!({"type":"setActivityStatus","activityId":activity_id,"reason":reason,"status":status})
            }
            Self::SetRunStatus { run_id, status, .. } => {
                json!({"type":"setRunStatus","runId":run_id,"status":status})
            }
            Self::Replay { epoch, cursor, .. } => {
                json!({"type":"replay","epoch":epoch,"cursor":cursor})
            }
            Self::SetActivityTimeout {
                activity_id,
                timeout_ms,
                ..
            } => {
                json!({"type":"setActivityTimeout","activityId":activity_id,"timeoutMs":timeout_ms})
            }
            Self::ReserveLaunch {
                command_fingerprint,
                ..
            } => {
                json!({"type":"reserveLaunch","command":command_fingerprint})
            }
            Self::CompleteLaunchReservation { result, .. } => {
                json!({"type":"completeLaunchReservation","result":result})
            }
            Self::RegisterFlowRun {
                projection,
                coordinator_id,
                ..
            } => {
                json!({"type":"registerFlowRun","projection":projection,"coordinatorId":coordinator_id})
            }
            Self::ClaimFlowRun {
                flow_id,
                coordinator_id,
                expected_fence,
                ..
            } => {
                json!({"type":"claimFlowRun","flowId":flow_id,"coordinatorId":coordinator_id,"expectedFence":expected_fence})
            }
            Self::AdvanceFlowStage {
                flow_id,
                expected_stage,
                next_stage,
                expected_flow_revision,
                fence,
                ..
            } => {
                json!({"type":"advanceFlowStage","flowId":flow_id,"expectedStage":expected_stage,"nextStage":next_stage,"expectedFlowRevision":expected_flow_revision,"fence":fence})
            }
            Self::CompleteFlowRun {
                flow_id,
                expected_stage,
                expected_flow_revision,
                evidence,
                fence,
                ..
            } => {
                json!({"type":"completeFlowRun","flowId":flow_id,"expectedStage":expected_stage,"expectedFlowRevision":expected_flow_revision,"evidence":evidence,"fence":fence})
            }
            Self::BindFlowAttempt {
                flow_id,
                task_id,
                attempt_id,
                generation,
                source_cursor,
                fence,
                ..
            } => {
                json!({"type":"bindFlowAttempt","flowId":flow_id,"taskId":task_id,"attemptId":attempt_id,"generation":generation,"sourceCursor":source_cursor,"fence":fence})
            }
            Self::ReconcileFlowAttempt {
                flow_id,
                task_id,
                outcome,
                fence,
                ..
            } => {
                json!({"type":"reconcileFlowAttempt","flowId":flow_id,"taskId":task_id,"outcome":outcome,"fence":fence})
            }
            Self::ApplyFlowReview {
                flow_id,
                task_id,
                flow_revision,
                evidence,
                fence,
                ..
            } => {
                json!({"type":"applyFlowReview","flowId":flow_id,"taskId":task_id,"flowRevision":flow_revision,"evidence":evidence,"fence":fence})
            }
            Self::RequestFlowTaskCancel {
                flow_id,
                task_id,
                attempt_id,
                generation,
                fence,
                ..
            } => {
                json!({"type":"cancelFlowTask","flowId":flow_id,"taskId":task_id,"attemptId":attempt_id,"generation":generation,"fence":fence})
            }
            Self::AcknowledgeFlowboardEvent {
                flow_id,
                event_id,
                new_flow_revision,
                fence,
                ..
            } => {
                json!({"type":"acknowledgeFlowboardEvent","flowId":flow_id,"eventId":event_id,"newFlowRevision":new_flow_revision,"fence":fence})
            }
            Self::RebaseFlowboardOutbox {
                flow_id,
                flow_revision,
                existing_event_ids,
                fence,
                ..
            } => {
                json!({"type":"rebaseFlowboardOutbox","flowId":flow_id,"flowRevision":flow_revision,"existingEventIds":existing_event_ids,"fence":fence})
            }
            Self::ReserveFlowDispatch {
                flow_id,
                task_id,
                fence,
                ..
            } => {
                json!({"type":"reserveFlowDispatch","flowId":flow_id,"taskId":task_id,"fence":fence})
            }
            Self::CompleteFlowDispatch {
                flow_id,
                dispatch,
                receipt,
                fence,
                ..
            } => {
                json!({"type":"completeFlowDispatch","flowId":flow_id,"dispatch":dispatch,"receipt":receipt,"fence":fence})
            }
            Self::ReleaseFlowDispatch {
                flow_id,
                dispatch,
                fence,
                ..
            } => {
                json!({"type":"releaseFlowDispatch","flowId":flow_id,"dispatch":dispatch,"fence":fence})
            }
            Self::RegisterFlowWorkerAttempt {
                flow_id,
                task_id,
                fence,
                ..
            } => {
                json!({"type":"registerFlowWorkerAttempt","flowId":flow_id,"taskId":task_id,"fence":fence})
            }
            Self::RequestWorkerApproval {
                attempt_id,
                generation,
                fence,
                request_id,
                kind,
                reason,
                payload,
                deadline_at_ms,
                ..
            } => {
                json!({"type":"requestWorkerApproval","attemptId":attempt_id,"generation":generation,"fence":fence,"requestId":request_id,"kind":kind,"reason":reason,"payload":payload,"deadlineAtMs":deadline_at_ms})
            }
            Self::ResolveWorkerApproval {
                attempt_id,
                generation,
                request_id,
                resolution_id,
                decision,
                grant,
                fence,
                ..
            } => {
                json!({"type":"resolveWorkerApproval","attemptId":attempt_id,"generation":generation,"requestId":request_id,"resolutionId":resolution_id,"decision":decision,"grant":grant,"fence":fence})
            }
            Self::RequestWorkerTool {
                attempt_id,
                generation,
                fence,
                request_id,
                tool_name,
                arguments,
                deadline_at_ms,
                ..
            } => {
                json!({"type":"requestWorkerTool","attemptId":attempt_id,"generation":generation,"fence":fence,"requestId":request_id,"toolName":tool_name,"arguments":arguments,"deadlineAtMs":deadline_at_ms})
            }
            Self::ClaimWorkerTool {
                attempt_id,
                generation,
                request_id,
                claim_id,
                fence,
                ..
            } => {
                json!({"type":"claimWorkerTool","attemptId":attempt_id,"generation":generation,"requestId":request_id,"claimId":claim_id,"fence":fence})
            }
            Self::ResolveWorkerTool {
                attempt_id,
                generation,
                request_id,
                claim_id,
                resolution_id,
                result,
                error,
                fence,
                ..
            } => {
                json!({"type":"resolveWorkerTool","attemptId":attempt_id,"generation":generation,"requestId":request_id,"claimId":claim_id,"resolutionId":resolution_id,"result":result,"error":error,"fence":fence})
            }
            Self::FinishWorkerTool {
                attempt_id,
                generation,
                request_id,
                state,
                error,
                fence,
                ..
            } => {
                json!({"type":"finishWorkerTool","attemptId":attempt_id,"generation":generation,"requestId":request_id,"state":state,"error":error,"fence":fence})
            }
        }
    }
}

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn map_store_error(error: CoordinatorStoreError) -> CoordinatorError {
    match error {
        CoordinatorStoreError::IdempotencyConflict => CoordinatorError::IdempotencyConflict,
        _ => CoordinatorError::Persistence,
    }
}

fn map_workflow_error(error: WorkflowError) -> CoordinatorError {
    CoordinatorError::Workflow(format!("{error:?}"))
}

fn approval_key(attempt_id: &str, generation: u64, request_id: &str) -> String {
    format!("{attempt_id}:{generation}:{request_id}")
}

fn sanitize_approval_payload(payload: Value) -> Option<Value> {
    const ALLOWED: &[&str] = &[
        "flowId",
        "boardId",
        "taskId",
        "postId",
        "runtimeRunId",
        "workerId",
        "projectId",
        "runId",
        "cwd",
        "commandSha256",
        "commandPreview",
        "interpreter",
        "executable",
        "jobId",
        "textSha256",
        "textPreview",
    ];
    fn clean_text(text: &str) -> String {
        let mut value = text.to_owned();
        for marker in [
            "Bearer ",
            "api_key=",
            "apiKey=",
            "token=",
            "password=",
            "secret=",
        ] {
            let mut offset = 0;
            while offset < value.len() {
                let Some(relative) = value[offset..].find(marker) else {
                    break;
                };
                let start = offset + relative + marker.len();
                let end = value[start..]
                    .find(|character: char| {
                        character.is_whitespace() || matches!(character, '&' | ',' | ';')
                    })
                    .map(|length| start + length)
                    .unwrap_or(value.len());
                value.replace_range(start..end, "[REDACTED]");
                offset = start + "[REDACTED]".len();
            }
        }
        value
    }
    let object = payload.as_object()?;
    let mut safe = serde_json::Map::new();
    for (key, value) in object {
        if !ALLOWED.contains(&key.as_str()) {
            continue;
        }
        let value = if matches!(
            key.as_str(),
            "commandPreview" | "textPreview" | "cwd" | "executable" | "interpreter"
        ) {
            let text = value.as_str()?;
            if text.len() > 4096 {
                return None;
            }
            json!(clean_text(text))
        } else {
            value.clone()
        };
        safe.insert(key.clone(), value);
    }
    Some(Value::Object(safe))
}

fn sanitize_approval_text(text: &str) -> String {
    let mut value = text.chars().take(512).collect::<String>();
    for marker in [
        "Bearer ",
        "api_key=",
        "apiKey=",
        "token=",
        "password=",
        "secret=",
    ] {
        let mut offset = 0;
        while offset < value.len() {
            let Some(relative) = value[offset..].find(marker) else {
                break;
            };
            let start = offset + relative + marker.len();
            let end = value[start..]
                .find(|character: char| {
                    character.is_whitespace() || matches!(character, '&' | ',' | ';')
                })
                .map(|length| start + length)
                .unwrap_or(value.len());
            value.replace_range(start..end, "[REDACTED]");
            offset = start + "[REDACTED]".len();
        }
    }
    value
}

#[cfg(test)]
mod persistence_tests {
    use super::*;

    fn projection() -> FlowRunProjection {
        FlowRunProjection {
            flow_id: "flow-persist".to_owned(),
            board_id: "board-persist".to_owned(),
            flow_revision: 4,
            current_stage: None,
            stage_transitions: BTreeMap::new(),
            max_attempts: 3,
            tasks: vec![crate::workflow::WorkflowTask {
                task_id: "task-1".to_owned(),
                post_id: "post-1".to_owned(),
                title: "Persist workflow state".to_owned(),
                stage_id: None,
                supervisor_run_id: None,
                supervisor_activity_id: None,
                dependencies: Vec::new(),
                assignee: None,
                state: crate::workflow::WorkflowTaskState::Waiting,
                waiting_reason: None,
                attempt_id: None,
                generation: None,
                last_source_cursor: 0,
                attempts_started: 0,
                dispatch_id: None,
                dispatch_pending: false,
                cancel_requested: false,
                runtime_run_id: None,
                process_id: None,
                last_outcome: None,
            }],
        }
    }

    #[tokio::test]
    async fn failed_durable_commit_is_not_published_and_freezes_actor() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (store, recovered) =
            CoordinatorStore::open(dir.path().join("coordinator-v1.jsonl")).expect("open store");
        let handle = CoordinatorHandle::spawn_persistent("epoch-test", store, recovered);
        let mut events = handle.subscribe();
        let oversized = json!({"payload":"x".repeat(5 * 1024 * 1024)});

        let result = handle
            .register_run_with_id("failed-commit".into(), oversized)
            .await;
        assert!(matches!(result, Err(CoordinatorError::Persistence)));
        assert_eq!(handle.snapshot().cursor, 0);
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        let later = handle
            .register_run_with_id("after-failure".into(), json!({}))
            .await;
        assert!(matches!(later, Err(CoordinatorError::Closed)));
        assert_eq!(handle.snapshot().cursor, 0);
    }

    #[tokio::test]
    async fn flowrun_and_unresolved_dispatch_outbox_survive_restart_without_redispatch() {
        let dir = tempfile::tempdir().expect("temp dir");
        let journal = dir.path().join("coordinator-v1.jsonl");
        let (store, recovered) = CoordinatorStore::open(&journal).expect("open store");
        let handle = CoordinatorHandle::spawn_persistent("epoch-before", store, recovered);
        handle.wait_ready().await.expect("initial owner ready");

        handle
            .register_flowrun_with_id("register-flow".into(), projection(), "coordinator-a".into())
            .await
            .expect("register flow");
        let claimed = handle
            .claim_flowrun_with_id(
                "claim-flow".into(),
                "flow-persist".into(),
                "coordinator-a".into(),
                0,
            )
            .await
            .expect("claim flow");
        let reserved = handle
            .reserve_flow_dispatch_with_id(
                "reserve-dispatch".into(),
                "flow-persist".into(),
                "task-1".into(),
                claimed.coordinator_fence,
            )
            .await
            .expect("reserve dispatch");
        let original = reserved.tasks.get("task-1").expect("task");
        assert!(original.dispatch_pending);
        assert_eq!(original.attempts_started, 1);
        assert!(!reserved.outbox.is_empty());
        let original_dispatch = reserved
            .dispatch_request("task-1")
            .expect("dispatch request");
        let generic_attempt = handle
            .register_flow_worker_attempt_with_id(
                format!("register-generic:{}", original_dispatch.idempotency_key),
                "flow-persist".into(),
                "task-1".into(),
                original_dispatch.coordinator_fence,
            )
            .await
            .expect("generic attempt linked");
        assert_eq!(generic_attempt.attempt_id, original_dispatch.attempt_id);
        assert_ne!(generic_attempt.run_id, "flow-persist");
        let duplicate_generic_attempt = handle
            .register_flow_worker_attempt_with_id(
                format!("register-generic:{}", original_dispatch.idempotency_key),
                "flow-persist".into(),
                "task-1".into(),
                original_dispatch.coordinator_fence,
            )
            .await
            .expect("idempotent generic registration");
        assert_eq!(
            duplicate_generic_attempt.attempt_id,
            generic_attempt.attempt_id
        );
        assert_eq!(handle.snapshot().attempts.len(), 1);

        drop(handle);
        tokio::task::yield_now().await;

        let (store, recovered) = CoordinatorStore::open(&journal).expect("reopen store");
        let restored = CoordinatorHandle::spawn_persistent("epoch-after", store, recovered);
        restored
            .wait_ready()
            .await
            .expect("restored owner fence persisted");
        for _ in 0..10 {
            if restored
                .snapshot()
                .flow_runs
                .iter()
                .any(|flow| flow.flow_id == "flow-persist")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let snapshot = restored.snapshot();
        assert_eq!(snapshot.manager_id, "epoch-after");
        let flow = snapshot
            .flow_runs
            .iter()
            .find(|flow| flow.flow_id == "flow-persist")
            .expect("restored flowrun");
        let task = flow.tasks.get("task-1").expect("restored task");
        assert!(
            task.dispatch_pending,
            "recovery must preserve unresolved reservation"
        );
        assert!(
            task.process_id.is_none(),
            "old process ownership is never restored"
        );
        assert_eq!(task.attempts_started, 1);
        assert!(snapshot.attempts.iter().any(|attempt| {
            attempt.attempt_id == generic_attempt.attempt_id
                && attempt.generation == generic_attempt.generation
        }));
        assert!(
            !flow.outbox.is_empty(),
            "pending Flowboard events remain durable"
        );
        assert_eq!(flow.coordinator_fence, claimed.coordinator_fence + 1);
        assert!(matches!(
            restored
                .reserve_flow_dispatch_with_id(
                    "stale-owner-dispatch".into(),
                    "flow-persist".into(),
                    "task-1".into(),
                    claimed.coordinator_fence,
                )
                .await,
            Err(CoordinatorError::Workflow(_))
        ));

        let intervening_fence = flow.coordinator_fence;
        drop(restored);
        tokio::task::yield_now().await;
        let (store, recovered) = CoordinatorStore::open(&journal).expect("reopen after idle epoch");
        let newest = CoordinatorHandle::spawn_persistent("epoch-third", store, recovered);
        newest
            .wait_ready()
            .await
            .expect("second restored fence persisted");
        let newest_flow = newest
            .snapshot()
            .flow_runs
            .into_iter()
            .next()
            .expect("FlowRun restored");
        assert_eq!(newest_flow.coordinator_fence, intervening_fence + 1);
        assert!(matches!(
            newest
                .claim_flowrun_with_id(
                    "stale-intervening-owner".into(),
                    "flow-persist".into(),
                    "epoch-after-client".into(),
                    intervening_fence,
                )
                .await,
            Err(CoordinatorError::Workflow(_))
        ));
        let missing_process = newest_flow.tasks["task-1"]
            .attempt_id
            .clone()
            .expect("reserved attempt ID persists");
        newest
            .report_attempt_with_id(
                "worker-missing-after-restart-generic".into(),
                missing_process.clone(),
                newest_flow.tasks["task-1"].generation.unwrap(),
                ExecutionStatus::Unknown,
                json!({"flowRunId":"flow-persist","taskId":"task-1","reasonCode":"process_not_live_after_restart"}),
            )
            .await
            .expect("generic attempt marked unknown");
        newest
            .reconcile_flow_attempt_with_id(
                "worker-missing-after-restart-flow".into(),
                "flow-persist".into(),
                "task-1".into(),
                AttemptOutcome {
                    attempt_id: missing_process,
                    generation: newest_flow.tasks["task-1"].generation.unwrap(),
                    source_cursor: 1,
                    status: "unknown".into(),
                    reason: Some("worker process was not live after restart".into()),
                    workflow_cancel_requested: false,
                    retryable: false,
                    evidence: None,
                },
                newest_flow.coordinator_fence,
            )
            .await
            .expect("FlowRun outcome reconciled as unknown");
        let unknown = newest.snapshot();
        let unknown_task = &unknown.flow_runs[0].tasks["task-1"];
        assert_eq!(
            unknown_task.state,
            crate::workflow::WorkflowTaskState::Waiting
        );
        assert!(!unknown_task.dispatch_pending);
        assert_eq!(unknown.attempts[0].status, ExecutionStatus::Unknown);
        assert!(matches!(
            newest
                .reserve_flow_dispatch_with_id(
                    "duplicate-dispatch".into(),
                    "flow-persist".into(),
                    "task-1".into(),
                    newest_flow.coordinator_fence,
                )
                .await,
            Err(CoordinatorError::Workflow(_))
        ));
    }

    #[test]
    fn approval_payload_uses_allowlist_and_redacts_secrets() {
        let payload = json!({
            "taskId": "task-1",
            "commandPreview": "build token=private Bearer abc123",
            "textPreview": "password=hunter2",
            "args": ["--secret", "should-not-persist"],
            "authorization": "Bearer raw-token",
            "environment": {"API_KEY": "secret"}
        });
        let safe = sanitize_approval_payload(payload).expect("object accepted");
        assert_eq!(safe["taskId"], "task-1");
        assert_eq!(
            safe["commandPreview"],
            "build token=[REDACTED] Bearer [REDACTED]"
        );
        assert_eq!(safe["textPreview"], "password=[REDACTED]");
        assert!(safe.get("args").is_none());
        assert!(safe.get("authorization").is_none());
        assert!(safe.get("environment").is_none());
    }

    #[tokio::test]
    async fn worker_tool_requests_restore_without_replay() {
        let dir = tempfile::tempdir().expect("temp dir");
        let journal = dir.path().join("tool-coordinator.jsonl");
        let (store, recovered) = CoordinatorStore::open(&journal).unwrap();
        let handle = CoordinatorHandle::spawn_persistent("tool-epoch-before", store, recovered);
        handle.wait_ready().await.unwrap();
        handle
            .register_flowrun_with_id(
                "tool-flow-register".into(),
                projection(),
                "coordinator".into(),
            )
            .await
            .unwrap();
        let flow = handle
            .claim_flowrun_with_id(
                "tool-flow-claim".into(),
                "flow-persist".into(),
                "coordinator".into(),
                0,
            )
            .await
            .unwrap();
        let flow = handle
            .reserve_flow_dispatch_with_id(
                "tool-flow-reserve".into(),
                "flow-persist".into(),
                "task-1".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let dispatch = flow.dispatch_request("task-1").unwrap();
        let attempt = handle
            .register_flow_worker_attempt_with_id(
                format!("tool-attempt:{}", dispatch.idempotency_key),
                dispatch.flow_id.clone(),
                dispatch.task_id.clone(),
                dispatch.coordinator_fence,
            )
            .await
            .unwrap();
        let flow = handle
            .complete_flow_dispatch_with_id(
                "tool-complete-dispatch".into(),
                dispatch.flow_id.clone(),
                dispatch.clone(),
                WorkflowDispatchReceipt {
                    process_id: "tool-process".into(),
                    runtime_run_id: "tool-runtime".into(),
                },
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let deadline = timestamp_ms() + 300_000;
        let pending = handle
            .request_worker_tool_with_id(
                "tool-pending-request".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                flow.coordinator_fence,
                "pending-call".into(),
                "search".into(),
                json!({"q":"pending"}),
                deadline,
            )
            .await
            .unwrap();
        let claimed = handle
            .request_worker_tool_with_id(
                "tool-claimed-request".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                flow.coordinator_fence,
                "claimed-call".into(),
                "invoke".into(),
                json!({"q":"side effect"}),
                deadline,
            )
            .await
            .unwrap();
        handle
            .claim_worker_tool_with_id(
                "tool-claimed-claim".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                claimed.request_id.clone(),
                "desktop-claim".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        assert_eq!(pending.state, WorkerToolRequestState::Pending);
        drop(handle);
        tokio::task::yield_now().await;
        let (store, recovered) = CoordinatorStore::open(&journal).unwrap();
        let restored = CoordinatorHandle::spawn_persistent("tool-epoch-after", store, recovered);
        restored.wait_ready().await.unwrap();
        let snapshot = restored.snapshot();
        assert_eq!(
            snapshot
                .tool_requests
                .iter()
                .find(|request| request.request_id == "pending-call")
                .unwrap()
                .state,
            WorkerToolRequestState::Interrupted
        );
        let claimed_after = snapshot
            .tool_requests
            .iter()
            .find(|request| request.request_id == "claimed-call")
            .unwrap();
        assert_eq!(claimed_after.state, WorkerToolRequestState::Unknown);
        assert!(claimed_after
            .error
            .as_deref()
            .unwrap()
            .contains("will not be replayed"));
        assert!(!snapshot.flow_runs[0].tasks["task-1"].dispatch_pending);
    }

    #[tokio::test]
    async fn lagged_subscriber_does_not_block_mutations_and_can_resync() {
        let coordinator = CoordinatorHandle::spawn("slow-subscriber-test");
        let mut events = coordinator.subscribe();
        let mut snapshot_updates = coordinator.subscribe_snapshots();
        for index in 0..(EVENT_CAPACITY + 8) {
            coordinator
                .register_run_with_id(
                    format!("slow-subscriber-run-{index}"),
                    json!({"index":index}),
                )
                .await
                .expect("slow subscriber must not block actor mutations");
        }
        assert!(matches!(
            events.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        let (receiver, replay) = coordinator.subscribe_from("stale-epoch".into(), 0).await;
        assert!(replay.resync_required);
        assert!(replay.events.is_empty());
        let latest = coordinator.snapshot();
        assert_eq!(latest.runs.len(), EVENT_CAPACITY + 8);
        snapshot_updates.changed().await.expect("snapshot advanced");
        assert_eq!(snapshot_updates.borrow().runs.len(), EVENT_CAPACITY + 8);
        drop(receiver);
    }

    #[tokio::test]
    async fn claimed_tool_timeout_is_unknown_and_never_replayable() {
        let coordinator = CoordinatorHandle::spawn("tool-claimed-timeout-test");
        let flow = coordinator
            .register_flowrun_with_id(
                "timeout-flow-register".into(),
                projection(),
                "supervisor".into(),
            )
            .await
            .unwrap();
        let flow = coordinator
            .claim_flowrun_with_id(
                "timeout-flow-claim".into(),
                flow.flow_id.clone(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        let flow = coordinator
            .reserve_flow_dispatch_with_id(
                "timeout-flow-reserve".into(),
                flow.flow_id.clone(),
                "task-1".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let dispatch = flow.dispatch_request("task-1").unwrap();
        let attempt = coordinator
            .register_flow_worker_attempt_with_id(
                format!("timeout-attempt:{}", dispatch.idempotency_key),
                dispatch.flow_id.clone(),
                dispatch.task_id.clone(),
                dispatch.coordinator_fence,
            )
            .await
            .unwrap();
        coordinator
            .complete_flow_dispatch_with_id(
                "timeout-complete-dispatch".into(),
                dispatch.flow_id.clone(),
                dispatch.clone(),
                WorkflowDispatchReceipt {
                    process_id: "timeout-process".into(),
                    runtime_run_id: "timeout-runtime".into(),
                },
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let request = coordinator
            .request_worker_tool_with_id(
                "timeout-tool-request".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                flow.coordinator_fence,
                "claimed-timeout".into(),
                "invoke".into(),
                json!({"sideEffect":true}),
                timestamp_ms() + 100,
            )
            .await
            .unwrap();
        coordinator
            .claim_worker_tool_with_id(
                "timeout-tool-claim".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                request.request_id.clone(),
                "claim-before-timeout".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let finished = tokio::time::timeout(
            Duration::from_secs(2),
            coordinator.wait_worker_tool(
                &attempt.attempt_id,
                attempt.generation,
                &request.request_id,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(finished.state, WorkerToolRequestState::Unknown);
        assert!(finished
            .error
            .as_deref()
            .unwrap()
            .contains("must not be replayed"));
        assert_eq!(
            coordinator.snapshot().flow_runs[0].tasks["task-1"].state,
            crate::workflow::WorkflowTaskState::Blocked
        );
    }

    #[tokio::test]
    async fn worker_tool_cancellation_wakes_waiter_with_cancelled_state() {
        let coordinator = CoordinatorHandle::spawn("tool-cancel-wakeup-test");
        let flow = coordinator
            .register_flowrun_with_id(
                "cancel-flow-register".into(),
                projection(),
                "supervisor".into(),
            )
            .await
            .unwrap();
        let flow = coordinator
            .claim_flowrun_with_id(
                "cancel-flow-claim".into(),
                flow.flow_id.clone(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        let flow = coordinator
            .reserve_flow_dispatch_with_id(
                "cancel-flow-reserve".into(),
                flow.flow_id.clone(),
                "task-1".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let dispatch = flow.dispatch_request("task-1").unwrap();
        let attempt = coordinator
            .register_flow_worker_attempt_with_id(
                format!("cancel-attempt:{}", dispatch.idempotency_key),
                dispatch.flow_id.clone(),
                dispatch.task_id.clone(),
                dispatch.coordinator_fence,
            )
            .await
            .unwrap();
        coordinator
            .complete_flow_dispatch_with_id(
                "cancel-complete-dispatch".into(),
                dispatch.flow_id.clone(),
                dispatch.clone(),
                WorkflowDispatchReceipt {
                    process_id: "cancel-process".into(),
                    runtime_run_id: "cancel-runtime".into(),
                },
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let request = coordinator
            .request_worker_tool_with_id(
                "cancel-tool-request".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                flow.coordinator_fence,
                "cancel-wakeup".into(),
                "invoke".into(),
                json!({"sideEffect":true}),
                timestamp_ms() + 30_000,
            )
            .await
            .unwrap();

        let waiter_coordinator = coordinator.clone();
        let attempt_id = attempt.attempt_id.clone();
        let request_id = request.request_id.clone();
        let waiter = tokio::spawn(async move {
            waiter_coordinator
                .wait_worker_tool(&attempt_id, attempt.generation, &request_id)
                .await
        });
        tokio::task::yield_now().await;
        coordinator
            .finish_worker_tool_with_id(
                "cancel-tool-finish".into(),
                attempt.attempt_id.clone(),
                attempt.generation,
                request.request_id.clone(),
                WorkerToolRequestState::Cancelled,
                "flow task cancellation requested".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();

        let finished = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("cancellation wakes suspended worker waiter")
            .unwrap()
            .unwrap();
        assert_eq!(finished.state, WorkerToolRequestState::Cancelled);
        assert_eq!(
            finished.error.as_deref(),
            Some("flow task cancellation requested")
        );
    }
}
