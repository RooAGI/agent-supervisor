//! Supervisor-owned FlowRun reconciliation and durable Flowboard event outbox.
//!
//! This module models workflow readiness and creates idempotent events. It
//! never launches workers and never treats a process exit as workflow success.

// Some typed states are retained for coordinator recovery and adapter hooks.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::coordinator::CoordinatorHandle;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum WorkflowTaskState {
    Waiting,
    Ready,
    Assigned,
    Running,
    Blocked,
    WaitingForApproval,
    ReadyForReview,
    Completed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowTask {
    pub task_id: String,
    pub post_id: String,
    pub title: String,
    #[serde(default)]
    pub stage_id: Option<String>,
    #[serde(default)]
    pub supervisor_run_id: Option<String>,
    #[serde(default)]
    pub supervisor_activity_id: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub assignee: Option<String>,
    pub state: WorkflowTaskState,
    pub waiting_reason: Option<String>,
    pub attempt_id: Option<String>,
    pub generation: Option<u64>,
    pub last_source_cursor: u64,
    #[serde(default)]
    pub attempts_started: u32,
    #[serde(default)]
    pub dispatch_id: Option<String>,
    #[serde(default)]
    pub dispatch_pending: bool,
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default)]
    pub runtime_run_id: Option<String>,
    #[serde(default)]
    pub process_id: Option<String>,
    #[serde(default)]
    pub last_outcome: Option<AttemptOutcome>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FlowboardOutboxEvent {
    pub event_id: String,
    pub request_id: String,
    pub flow_id: String,
    pub actor: String,
    pub expected_revision: u64,
    pub coordinator_fence: u64,
    pub kind: String,
    pub task_id: Option<String>,
    pub post_id: Option<String>,
    pub attempt_id: Option<String>,
    pub data: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FlowRunWorkflowState {
    pub flow_id: String,
    pub board_id: String,
    pub coordinator_id: String,
    pub coordinator_fence: u64,
    #[serde(default)]
    pub coordinator_fence_exhausted: bool,
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
    pub tasks: BTreeMap<String, WorkflowTask>,
    pub outbox: VecDeque<FlowboardOutboxEvent>,
    pub pending_sync: bool,
    pub next_event_sequence: u64,
    pub sync_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FlowRunProjection {
    pub flow_id: String,
    pub board_id: String,
    pub flow_revision: u64,
    #[serde(default)]
    pub current_stage: Option<String>,
    #[serde(default)]
    pub stage_transitions: BTreeMap<String, Vec<String>>,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    pub tasks: Vec<WorkflowTask>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WorkflowError {
    FlowNotFound,
    TaskNotFound,
    FenceMismatch,
    StaleCursor,
    StaleGeneration,
    InvalidDependencies(String),
    InvalidStage(String),
    StageMismatch,
    FenceExhausted,
    OutboxMismatch,
    NotReady,
    RetryLimitReached,
    DispatchUnknown,
}

#[derive(Clone, Debug)]
pub(crate) enum FlowboardAppendResult {
    Appended { revision: u64 },
    RevisionConflict,
    Rejected(String),
    Transient(String),
}

#[derive(Clone, Debug)]
pub(crate) struct FlowboardCurrent {
    pub revision: u64,
    pub event_ids: Vec<String>,
}

pub(crate) type SinkFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Implemented by a service-owned Flowboard connection, for example a
/// supervised MCP stdio adapter. Desktop clients never supply this callback.
pub(crate) trait FlowboardSink: Send + Sync + 'static {
    fn append<'a>(
        &'a self,
        event: &'a FlowboardOutboxEvent,
    ) -> SinkFuture<'a, FlowboardAppendResult>;
    fn current<'a>(&'a self, flow_id: &'a str) -> SinkFuture<'a, Result<FlowboardCurrent, String>>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttemptOutcome {
    pub attempt_id: String,
    pub generation: u64,
    pub source_cursor: u64,
    pub status: String,
    pub reason: Option<String>,
    /// True only when the workflow itself requested cancellation.
    pub workflow_cancel_requested: bool,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub evidence: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowDispatchRequest {
    pub flow_id: String,
    pub board_id: String,
    pub task_id: String,
    pub post_id: String,
    pub title: String,
    pub attempt_id: String,
    pub generation: u64,
    pub idempotency_key: String,
    pub coordinator_fence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowDispatchReceipt {
    pub process_id: String,
    pub runtime_run_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowWorkerEvent {
    pub attempt_id: String,
    pub generation: u64,
    pub source_cursor: u64,
    pub status: String,
    pub reason: Option<String>,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub evidence: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkflowDispatchInput {
    pub request: WorkflowDispatchRequest,
    /// Resolved on demand by the service adapter; never written to the journal.
    pub invocation: Value,
    pub workflow_context: Value,
}

pub(crate) trait WorkflowDispatcher: Send + Sync + 'static {
    fn resolve_invocation<'a>(
        &'a self,
        flow: &'a FlowRunWorkflowState,
        task: &'a WorkflowTask,
    ) -> SinkFuture<'a, Result<Value, String>>;
    fn dispatch<'a>(
        &'a self,
        input: WorkflowDispatchInput,
    ) -> SinkFuture<'a, Result<WorkflowDispatchReceipt, String>>;
    fn attach<'a>(
        &'a self,
        request: &'a WorkflowDispatchRequest,
        receipt: Option<&'a WorkflowDispatchReceipt>,
    ) -> SinkFuture<'a, Result<tokio::sync::mpsc::Receiver<WorkflowWorkerEvent>, String>>;
    fn resume<'a>(
        &'a self,
        request: &'a WorkflowDispatchRequest,
        input: Value,
    ) -> SinkFuture<'a, Result<(), String>>;
    fn cancel<'a>(
        &'a self,
        request: &'a WorkflowDispatchRequest,
    ) -> SinkFuture<'a, Result<(), String>>;
}

impl FlowRunWorkflowState {
    pub(crate) fn from_projection(
        projection: FlowRunProjection,
        coordinator_id: String,
        coordinator_fence: u64,
    ) -> Result<Self, WorkflowError> {
        let mut tasks = BTreeMap::new();
        let current_stage = projection.current_stage.clone();
        let stage_transitions = projection.stage_transitions.clone();
        for mut task in projection.tasks {
            if tasks.contains_key(&task.task_id) {
                return Err(WorkflowError::InvalidDependencies(format!(
                    "duplicate task ID {}",
                    task.task_id
                )));
            }
            task.state = WorkflowTaskState::Waiting;
            task.waiting_reason = None;
            task.attempt_id = None;
            task.generation = None;
            task.last_source_cursor = 0;
            task.attempts_started = 0;
            task.dispatch_id = None;
            task.dispatch_pending = false;
            task.cancel_requested = false;
            task.runtime_run_id = None;
            task.process_id = None;
            task.last_outcome = None;
            task.supervisor_run_id
                .get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
            task.supervisor_activity_id
                .get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
            tasks.insert(task.task_id.clone(), task);
        }
        let has_stage_contract = current_stage.is_some() || !stage_transitions.is_empty();
        if has_stage_contract {
            let mut stages = stage_transitions
                .keys()
                .cloned()
                .collect::<std::collections::HashSet<_>>();
            for destinations in stage_transitions.values() {
                stages.extend(destinations.iter().cloned());
            }
            if let Some(stage) = &current_stage {
                stages.insert(stage.clone());
            }
            for task in tasks.values() {
                let Some(stage_id) = task.stage_id.as_ref() else {
                    return Err(WorkflowError::InvalidStage(format!(
                        "task {} has no stage assignment",
                        task.task_id
                    )));
                };
                if !stages.contains(stage_id) {
                    return Err(WorkflowError::InvalidStage(format!(
                        "task {} references unknown stage {stage_id}",
                        task.task_id
                    )));
                }
            }
            for (from, destinations) in &stage_transitions {
                let mut unique = std::collections::HashSet::new();
                if destinations
                    .iter()
                    .any(|to| to == from || !unique.insert(to))
                {
                    return Err(WorkflowError::InvalidStage(format!(
                        "stage {from} has an invalid or duplicate transition"
                    )));
                }
            }
            if current_stage
                .as_ref()
                .is_none_or(|stage| !stages.contains(stage))
            {
                return Err(WorkflowError::InvalidStage(
                    "stage contract requires a known current stage".to_owned(),
                ));
            }
        }
        for task in tasks.values() {
            for dependency in &task.dependencies {
                if !tasks.contains_key(dependency) || dependency == &task.task_id {
                    return Err(WorkflowError::InvalidDependencies(format!(
                        "task {} has invalid dependency {dependency}",
                        task.task_id
                    )));
                }
            }
        }
        ensure_acyclic(&tasks)?;
        let mut state = Self {
            flow_id: projection.flow_id,
            board_id: projection.board_id,
            coordinator_id,
            coordinator_fence,
            coordinator_fence_exhausted: false,
            flow_revision: projection.flow_revision,
            current_stage,
            stage_transitions,
            waiting_reason: None,
            completed: false,
            max_attempts: projection.max_attempts.max(1),
            source_cursor: 0,
            tasks,
            outbox: VecDeque::new(),
            pending_sync: false,
            next_event_sequence: 1,
            sync_error: None,
        };
        state.refresh_readiness();
        state.refresh_waiting_reason();
        Ok(state)
    }

    /// Advances the cooperative fence and queues the Flowboard ownership event.
    pub(crate) fn claim(
        &mut self,
        coordinator_id: String,
        expected_fence: u64,
    ) -> Result<u64, WorkflowError> {
        if expected_fence != self.coordinator_fence {
            return Err(WorkflowError::FenceMismatch);
        }
        let previous_fence = self.coordinator_fence;
        let Some(next_fence) = self.coordinator_fence.checked_add(1) else {
            self.coordinator_fence_exhausted = true;
            return Err(WorkflowError::FenceExhausted);
        };
        self.coordinator_fence = next_fence;
        self.coordinator_id = coordinator_id.clone();
        self.enqueue_with_fence(
            "coordinator_claimed",
            None,
            None,
            None,
            json!({"coordinatorId":coordinator_id,"summary":"Supervisor coordinator claimed this FlowRun"}),
            previous_fence,
        );
        Ok(self.coordinator_fence)
    }

    pub(crate) fn advance_stage(
        &mut self,
        expected_stage: &str,
        next_stage: &str,
        expected_flow_revision: u64,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        if self.flow_revision != expected_flow_revision {
            return Err(WorkflowError::StaleCursor);
        }
        let current = self
            .current_stage
            .as_deref()
            .ok_or(WorkflowError::NotReady)?;
        if current != expected_stage {
            return Err(WorkflowError::StageMismatch);
        }
        if !self
            .stage_transitions
            .get(expected_stage)
            .is_some_and(|allowed| allowed.iter().any(|stage| stage == next_stage))
        {
            return Err(WorkflowError::InvalidStage(format!(
                "transition {expected_stage} -> {next_stage} is not declared"
            )));
        }
        if self.tasks.values().any(|task| {
            task.stage_id.as_deref() == Some(expected_stage)
                && !matches!(
                    task.state,
                    WorkflowTaskState::Completed | WorkflowTaskState::Cancelled
                )
        }) {
            return Err(WorkflowError::NotReady);
        }
        self.current_stage = Some(next_stage.to_owned());
        self.refresh_readiness();
        self.refresh_waiting_reason();
        self.enqueue(
            "stage_changed",
            None,
            None,
            None,
            json!({
                "fromStage": expected_stage,
                "toStage": next_stage,
                "summary": format!("Supervisor advanced FlowRun stage to {next_stage}")
            }),
        );
        Ok(())
    }

    pub(crate) fn complete_flowrun(
        &mut self,
        expected_stage: &str,
        expected_flow_revision: u64,
        evidence: Value,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        if self.completed {
            return Err(WorkflowError::NotReady);
        }
        if self.flow_revision != expected_flow_revision {
            return Err(WorkflowError::StaleCursor);
        }
        if self.current_stage.as_deref() != Some(expected_stage)
            || self
                .stage_transitions
                .get(expected_stage)
                .is_some_and(|edges| !edges.is_empty())
            || self
                .tasks
                .values()
                .any(|task| task.state != WorkflowTaskState::Completed)
        {
            return Err(WorkflowError::NotReady);
        }
        self.completed = true;
        self.waiting_reason = None;
        self.enqueue(
            "stage_completed",
            None,
            None,
            None,
            json!({
                "stage": expected_stage,
                "evidence": evidence,
                "summary": "Supervisor coordinator completed the terminal FlowRun stage"
            }),
        );
        Ok(())
    }

    pub(crate) fn set_task_approval_wait(
        &mut self,
        task_id: &str,
        request_id: &str,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        let task = self
            .tasks
            .get_mut(task_id)
            .ok_or(WorkflowError::TaskNotFound)?;
        if !matches!(
            task.state,
            WorkflowTaskState::Running
                | WorkflowTaskState::Assigned
                | WorkflowTaskState::WaitingForApproval
        ) {
            return Err(WorkflowError::NotReady);
        }
        task.state = WorkflowTaskState::WaitingForApproval;
        task.waiting_reason = Some(format!("approval requested: {request_id}"));
        self.waiting_reason = Some(format!("waiting for approval on task {task_id}"));
        self.enqueue(
            "progress",
            Some(task_id.to_owned()),
            Some(self.tasks[task_id].post_id.clone()),
            self.tasks[task_id].attempt_id.clone(),
            json!({"waitingForApproval":true,"requestId":request_id,"summary":"Worker is waiting for an approval decision"}),
        );
        Ok(())
    }

    pub(crate) fn resolve_task_approval_wait(
        &mut self,
        task_id: &str,
        request_id: &str,
        approved: bool,
        reason: Option<String>,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        let task = self
            .tasks
            .get_mut(task_id)
            .ok_or(WorkflowError::TaskNotFound)?;
        if task.state != WorkflowTaskState::WaitingForApproval {
            return Err(WorkflowError::NotReady);
        }
        task.state = if approved {
            WorkflowTaskState::Running
        } else {
            WorkflowTaskState::Blocked
        };
        task.waiting_reason = (!approved).then(|| {
            reason
                .clone()
                .unwrap_or_else(|| "approval was denied or interrupted".to_owned())
        });
        self.refresh_waiting_reason();
        self.enqueue(
            "progress",
            Some(task_id.to_owned()),
            Some(self.tasks[task_id].post_id.clone()),
            self.tasks[task_id].attempt_id.clone(),
            json!({"waitingForApproval":false,"requestId":request_id,"approved":approved,"reason":reason}),
        );
        Ok(())
    }

    pub(crate) fn bind_attempt(
        &mut self,
        task_id: &str,
        attempt_id: String,
        generation: u64,
        source_cursor: u64,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        if source_cursor < self.source_cursor {
            return Err(WorkflowError::StaleCursor);
        }
        let (post_id, title, assignee) = {
            let task = self
                .tasks
                .get_mut(task_id)
                .ok_or(WorkflowError::TaskNotFound)?;
            if task.generation.is_some_and(|current| generation < current) {
                return Err(WorkflowError::StaleGeneration);
            }
            task.attempt_id = Some(attempt_id.clone());
            task.generation = Some(generation);
            task.state = WorkflowTaskState::Assigned;
            task.waiting_reason = None;
            task.last_source_cursor = source_cursor;
            task.attempts_started = task.attempts_started.saturating_add(1);
            (
                task.post_id.clone(),
                task.title.clone(),
                task.assignee.clone(),
            )
        };
        self.source_cursor = source_cursor;
        self.enqueue(
            "task_assigned",
            Some(task_id.to_owned()),
            Some(post_id),
            Some(attempt_id),
            json!({"title":title,"assignee":assignee,"generation":generation,"summary":"Supervisor assigned an execution attempt"}),
        );
        Ok(())
    }

    pub(crate) fn reconcile_attempt(
        &mut self,
        task_id: &str,
        outcome: AttemptOutcome,
        fence: u64,
    ) -> Result<WorkflowTaskState, WorkflowError> {
        self.check_fence(fence)?;
        let post_id = {
            let task = self
                .tasks
                .get_mut(task_id)
                .ok_or(WorkflowError::TaskNotFound)?;
            if task.attempt_id.as_deref() != Some(outcome.attempt_id.as_str())
                || task.generation != Some(outcome.generation)
            {
                return Err(WorkflowError::StaleGeneration);
            }
            if outcome.source_cursor <= task.last_source_cursor {
                return Err(WorkflowError::StaleCursor);
            }
            task.last_source_cursor = outcome.source_cursor;
            task.last_outcome = Some(outcome.clone());
            if matches!(
                outcome.status.as_str(),
                "succeeded" | "failed" | "blocked" | "cancelled"
            ) {
                task.cancel_requested = false;
            }
            task.post_id.clone()
        };
        self.source_cursor = self.source_cursor.max(outcome.source_cursor);
        let detail = json!({
            "attemptId":outcome.attempt_id,
            "generation":outcome.generation,
            "sourceCursor":outcome.source_cursor,
            "executionOutcome":outcome.status,
            "reason":outcome.reason,
            "evidence":outcome.evidence,
        });
        let (state, kind, data) = {
            let task = self.tasks.get_mut(task_id).expect("validated task");
            match outcome.status.as_str() {
                "running" if task.state == WorkflowTaskState::WaitingForApproval => (
                    task.state,
                    "progress".to_owned(),
                    json!({"execution":detail,"waitingForApproval":true,"waitingReason":task.waiting_reason}),
                ),
                "running" => {
                    let kind = if task.state == WorkflowTaskState::Running {
                        "progress"
                    } else {
                        "task_started"
                    };
                    task.state = WorkflowTaskState::Running;
                    task.waiting_reason = task
                        .cancel_requested
                        .then(|| "worker cancellation requested".to_owned());
                    (task.state, kind.to_owned(), detail.clone())
                }
                "succeeded" => {
                    task.state = WorkflowTaskState::ReadyForReview;
                    task.waiting_reason =
                        Some("execution succeeded; workflow review is still required".to_owned());
                    (task.state, "ready_for_review".to_owned(), detail.clone())
                }
                "failed" | "blocked" => {
                    let can_retry = outcome.status == "failed"
                        && outcome.retryable
                        && task.attempts_started < self.max_attempts;
                    task.state = if can_retry {
                        WorkflowTaskState::Ready
                    } else {
                        WorkflowTaskState::Blocked
                    };
                    task.waiting_reason = if can_retry {
                        Some("retry is ready under the FlowRun attempt limit".to_owned())
                    } else {
                        outcome.reason.clone().or_else(|| {
                            Some("worker attempt failed or retry limit reached".to_owned())
                        })
                    };
                    (
                        task.state,
                        "blocked".to_owned(),
                        json!({"blocker":task.waiting_reason,"execution":detail,"nextAction":"review failure and decide whether to retry"}),
                    )
                }
                "cancelled" if outcome.workflow_cancel_requested => {
                    task.state = WorkflowTaskState::Cancelled;
                    task.waiting_reason = None;
                    (task.state, "task_cancelled".to_owned(), detail.clone())
                }
                "cancelled" => {
                    task.state = WorkflowTaskState::Blocked;
                    task.waiting_reason = Some(
                        "attempt was cancelled; FlowRun cancellation was not requested".to_owned(),
                    );
                    (
                        task.state,
                        "blocked".to_owned(),
                        json!({"blocker":task.waiting_reason,"execution":detail,"nextAction":"decide whether to retry or cancel the task"}),
                    )
                }
                "unknown" => {
                    task.dispatch_pending = false;
                    task.state = WorkflowTaskState::Waiting;
                    task.waiting_reason = Some(
                        "worker outcome is unknown; reconcile or explicitly retry before dispatch"
                            .to_owned(),
                    );
                    (task.state, "progress".to_owned(), detail.clone())
                }
                _ => {
                    task.state = WorkflowTaskState::Waiting;
                    task.waiting_reason =
                        Some("attempt outcome is unknown; reconcile with its framework".to_owned());
                    (task.state, "progress".to_owned(), detail.clone())
                }
            }
        };
        self.enqueue(
            &kind,
            Some(task_id.to_owned()),
            Some(post_id),
            Some(outcome.attempt_id),
            data,
        );
        self.refresh_waiting_reason();
        Ok(state)
    }

    /// Process exit is supporting execution evidence. It deliberately leaves
    /// workflow task state unchanged and cannot approve or complete a task.
    pub(crate) fn observe_process_exit(
        &mut self,
        task_id: &str,
        process_id: String,
        exit_status: String,
        source_cursor: u64,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        if source_cursor <= self.source_cursor {
            return Err(WorkflowError::StaleCursor);
        }
        let task = self.tasks.get(task_id).ok_or(WorkflowError::TaskNotFound)?;
        self.source_cursor = source_cursor;
        self.enqueue(
            "progress",
            Some(task_id.to_owned()),
            Some(task.post_id.clone()),
            task.attempt_id.clone(),
            json!({"processId":process_id,"processExit":exit_status,"sourceCursor":source_cursor,"summary":"Supervisor observed process exit; workflow outcome remains pending"}),
        );
        Ok(())
    }

    /// Only Flowboard review evidence may transition a task into Completed.
    pub(crate) fn apply_review_approved(
        &mut self,
        task_id: &str,
        flow_revision: u64,
        evidence: Value,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        if flow_revision <= self.flow_revision {
            return Err(WorkflowError::StaleCursor);
        }
        let task = self
            .tasks
            .get_mut(task_id)
            .ok_or(WorkflowError::TaskNotFound)?;
        if task.state != WorkflowTaskState::ReadyForReview {
            return Err(WorkflowError::NotReady);
        }
        task.state = WorkflowTaskState::Completed;
        task.waiting_reason = None;
        self.flow_revision = flow_revision;
        self.refresh_readiness();
        self.refresh_waiting_reason();
        let task = self.tasks.get(task_id).expect("task retained");
        self.enqueue(
            "progress",
            Some(task_id.to_owned()),
            Some(task.post_id.clone()),
            task.attempt_id.clone(),
            json!({"reviewApproved":true,"reviewEvidence":evidence,"summary":"Flowboard review approved this task"}),
        );
        Ok(())
    }

    pub(crate) fn ready_tasks(&self) -> Vec<String> {
        if self.completed {
            return Vec::new();
        }
        self.tasks
            .values()
            .filter(|task| {
                task.state == WorkflowTaskState::Ready
                    && self
                        .current_stage
                        .as_ref()
                        .is_none_or(|stage| task.stage_id.as_ref() == Some(stage))
            })
            .map(|task| task.task_id.clone())
            .collect()
    }

    fn dispatch_context(&self, task_id: &str) -> Value {
        let Some(task) = self.tasks.get(task_id) else {
            return Value::Null;
        };
        let mut evidence_budget = 64 * 1024usize;
        let dependencies = task
            .dependencies
            .iter()
            .filter_map(|id| self.tasks.get(id))
            .map(|dependency| {
                let mut evidence = dependency.last_outcome.as_ref().and_then(|outcome| outcome.evidence.clone());
                if let Some(value) = evidence.as_ref() {
                    let size = serde_json::to_vec(value).map(|bytes| bytes.len()).unwrap_or(0);
                    if size > evidence_budget {
                        evidence = None;
                    } else {
                        evidence_budget -= size;
                    }
                }
                json!({
                    "taskId":dependency.task_id,
                    "title":dependency.title,
                    "state":dependency.state,
                    "waitingReason":dependency.waiting_reason,
                    "lastOutcome":dependency.last_outcome.as_ref().map(|outcome| json!({"status":outcome.status,"reason":outcome.reason,"evidence":evidence})),
                })
            })
            .collect::<Vec<_>>();
        json!({"flowId":self.flow_id,"boardId":self.board_id,"stage":self.current_stage,"taskId":task_id,"dependencies":dependencies})
    }

    pub(crate) fn request_task_cancel(
        &mut self,
        task_id: &str,
        attempt_id: &str,
        generation: u64,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        let post_id = {
            let task = self
                .tasks
                .get_mut(task_id)
                .ok_or(WorkflowError::TaskNotFound)?;
            if task.attempt_id.as_deref() != Some(attempt_id) || task.generation != Some(generation)
            {
                return Err(WorkflowError::StaleGeneration);
            }
            if !matches!(
                task.state,
                WorkflowTaskState::Assigned
                    | WorkflowTaskState::Running
                    | WorkflowTaskState::WaitingForApproval
            ) {
                return Err(WorkflowError::NotReady);
            }
            if task.cancel_requested {
                return Ok(());
            }
            task.cancel_requested = true;
            task.waiting_reason = Some("worker cancellation requested".to_owned());
            task.post_id.clone()
        };
        self.enqueue(
            "task_cancel_requested",
            Some(task_id.to_owned()),
            Some(post_id),
            Some(attempt_id.to_owned()),
            json!({"generation":generation,"summary":"Supervisor requested worker cancellation"}),
        );
        Ok(())
    }

    pub(crate) fn reserve_dispatch(
        &mut self,
        task_id: &str,
        fence: u64,
    ) -> Result<WorkflowDispatchRequest, WorkflowError> {
        self.check_fence(fence)?;
        let task = self
            .tasks
            .get_mut(task_id)
            .ok_or(WorkflowError::TaskNotFound)?;
        if task.state != WorkflowTaskState::Ready {
            return Err(WorkflowError::NotReady);
        }
        if task.attempts_started >= self.max_attempts {
            return Err(WorkflowError::RetryLimitReached);
        }
        let generation = task.generation.unwrap_or(0).saturating_add(1);
        let attempt_id = uuid::Uuid::new_v4().to_string();
        let idempotency_key = format!("{}:{}:{}", self.flow_id, task_id, generation);
        let dispatch_id = idempotency_key.clone();
        task.attempt_id = Some(attempt_id.clone());
        task.generation = Some(generation);
        task.attempts_started = task.attempts_started.saturating_add(1);
        task.dispatch_id = Some(dispatch_id);
        task.supervisor_run_id = Some(uuid::Uuid::new_v4().to_string());
        task.supervisor_activity_id = Some(uuid::Uuid::new_v4().to_string());
        task.dispatch_pending = true;
        task.cancel_requested = false;
        task.runtime_run_id = None;
        task.process_id = None;
        task.last_source_cursor = 0;
        task.state = WorkflowTaskState::Assigned;
        task.waiting_reason = Some("dispatch reserved; awaiting worker host".to_owned());
        Ok(WorkflowDispatchRequest {
            flow_id: self.flow_id.clone(),
            board_id: self.board_id.clone(),
            task_id: task.task_id.clone(),
            post_id: task.post_id.clone(),
            title: task.title.clone(),
            attempt_id,
            generation,
            idempotency_key,
            coordinator_fence: self.coordinator_fence,
        })
    }

    pub(crate) fn complete_dispatch(
        &mut self,
        request: &WorkflowDispatchRequest,
        receipt: WorkflowDispatchReceipt,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        let task = self
            .tasks
            .get_mut(&request.task_id)
            .ok_or(WorkflowError::TaskNotFound)?;
        if task.attempt_id.as_deref() != Some(request.attempt_id.as_str())
            || task.generation != Some(request.generation)
            || task.dispatch_id.as_deref() != Some(request.idempotency_key.as_str())
        {
            return Err(WorkflowError::StaleGeneration);
        }
        task.dispatch_pending = false;
        task.runtime_run_id = Some(receipt.runtime_run_id);
        task.process_id = Some(receipt.process_id);
        task.state = WorkflowTaskState::Running;
        task.waiting_reason = None;
        Ok(())
    }

    pub(crate) fn release_dispatch_before_start(
        &mut self,
        request: &WorkflowDispatchRequest,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        let task = self
            .tasks
            .get_mut(&request.task_id)
            .ok_or(WorkflowError::TaskNotFound)?;
        if !task.dispatch_pending
            || task.attempt_id.as_deref() != Some(request.attempt_id.as_str())
            || task.generation != Some(request.generation)
        {
            return Err(WorkflowError::StaleGeneration);
        }
        task.dispatch_pending = false;
        task.dispatch_id = None;
        task.attempt_id = None;
        task.generation = None;
        task.attempts_started = task.attempts_started.saturating_sub(1);
        task.state = WorkflowTaskState::Ready;
        task.waiting_reason = Some("worker host unavailable before dispatch".to_owned());
        Ok(())
    }

    pub(crate) fn dispatch_request(
        &self,
        task_id: &str,
    ) -> Result<WorkflowDispatchRequest, WorkflowError> {
        let task = self.tasks.get(task_id).ok_or(WorkflowError::TaskNotFound)?;
        let generation = task.generation.ok_or(WorkflowError::DispatchUnknown)?;
        let attempt_id = task
            .attempt_id
            .clone()
            .ok_or(WorkflowError::DispatchUnknown)?;
        let idempotency_key = task
            .dispatch_id
            .clone()
            .ok_or(WorkflowError::DispatchUnknown)?;
        Ok(WorkflowDispatchRequest {
            flow_id: self.flow_id.clone(),
            board_id: self.board_id.clone(),
            task_id: task.task_id.clone(),
            post_id: task.post_id.clone(),
            title: task.title.clone(),
            attempt_id,
            generation,
            idempotency_key,
            coordinator_fence: self.coordinator_fence,
        })
    }

    pub(crate) fn next_outbox(&self) -> Option<&FlowboardOutboxEvent> {
        self.outbox.front()
    }

    pub(crate) fn acknowledge_outbox(
        &mut self,
        event_id: &str,
        new_flow_revision: u64,
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        let item = self.outbox.front().ok_or(WorkflowError::OutboxMismatch)?;
        if item.event_id != event_id {
            return Err(WorkflowError::OutboxMismatch);
        }
        self.outbox.pop_front();
        self.flow_revision = new_flow_revision;
        self.sync_error = None;
        self.pending_sync = !self.outbox.is_empty();
        for item in &mut self.outbox {
            item.expected_revision = self.flow_revision;
        }
        Ok(())
    }

    pub(crate) fn rebase_outbox(
        &mut self,
        flow_revision: u64,
        existing_event_ids: &[String],
        fence: u64,
    ) -> Result<(), WorkflowError> {
        self.check_fence(fence)?;
        self.flow_revision = flow_revision;
        while self
            .outbox
            .front()
            .is_some_and(|item| existing_event_ids.contains(&item.event_id))
        {
            self.outbox.pop_front();
        }
        for item in &mut self.outbox {
            item.expected_revision = self.flow_revision;
            item.request_id = format!("{}:revision:{}", item.event_id, self.flow_revision);
        }
        self.sync_error = None;
        self.pending_sync = !self.outbox.is_empty();
        Ok(())
    }

    fn check_fence(&self, fence: u64) -> Result<(), WorkflowError> {
        if self.coordinator_fence_exhausted {
            Err(WorkflowError::FenceExhausted)
        } else if fence == self.coordinator_fence {
            Ok(())
        } else {
            Err(WorkflowError::FenceMismatch)
        }
    }

    fn refresh_readiness(&mut self) {
        let complete = self
            .tasks
            .iter()
            .map(|(id, task)| (id.clone(), task.state == WorkflowTaskState::Completed))
            .collect::<BTreeMap<_, _>>();
        for task in self.tasks.values_mut() {
            if matches!(
                task.state,
                WorkflowTaskState::Waiting | WorkflowTaskState::Ready
            ) && task.attempt_id.is_none()
            {
                let stage_ready = self
                    .current_stage
                    .as_ref()
                    .is_none_or(|stage| task.stage_id.as_ref() == Some(stage));
                let ready = stage_ready
                    && task
                        .dependencies
                        .iter()
                        .all(|dependency| complete.get(dependency).copied().unwrap_or(false));
                task.state = if ready {
                    WorkflowTaskState::Ready
                } else {
                    WorkflowTaskState::Waiting
                };
                task.waiting_reason = if !stage_ready {
                    task.stage_id
                        .as_ref()
                        .map(|stage| format!("waiting for stage {stage}"))
                } else if !ready {
                    Some("waiting for task dependencies".to_owned())
                } else {
                    None
                };
            }
        }
    }

    fn refresh_waiting_reason(&mut self) {
        self.waiting_reason = self.current_stage.as_ref().and_then(|stage| {
            let tasks = self
                .tasks
                .values()
                .filter(|task| task.stage_id.as_ref() == Some(stage))
                .collect::<Vec<_>>();
            if tasks.iter().any(|task| task.state == WorkflowTaskState::Blocked) {
                return Some(format!(
                    "waiting for coordinator decision on blocked tasks in stage {stage}"
                ));
            }
            if tasks
                .iter()
                .any(|task| task.state == WorkflowTaskState::ReadyForReview)
            {
                return Some(format!("waiting for review evidence in stage {stage}"));
            }
            if tasks
                .iter()
                .any(|task| task.state == WorkflowTaskState::WaitingForApproval)
            {
                return Some(format!("waiting for worker approval in stage {stage}"));
            }
            if tasks.iter().all(|task| {
                matches!(
                    task.state,
                    WorkflowTaskState::Completed | WorkflowTaskState::Cancelled
                )
            }) {
                return Some(
                    if self
                        .stage_transitions
                        .get(stage)
                        .is_some_and(|edges| !edges.is_empty())
                    {
                        format!("waiting for coordinator decision to advance from stage {stage}")
                    } else {
                        format!("terminal stage {stage} is complete; waiting for coordinator decision to complete the FlowRun")
                    },
                );
            }
            None
        });
    }

    fn enqueue(
        &mut self,
        kind: &str,
        task_id: Option<String>,
        post_id: Option<String>,
        attempt_id: Option<String>,
        data: Value,
    ) {
        self.enqueue_with_fence(
            kind,
            task_id,
            post_id,
            attempt_id,
            data,
            self.coordinator_fence,
        );
    }

    fn enqueue_with_fence(
        &mut self,
        kind: &str,
        task_id: Option<String>,
        post_id: Option<String>,
        attempt_id: Option<String>,
        data: Value,
        event_fence: u64,
    ) {
        let seq = self.next_event_sequence;
        self.next_event_sequence = self.next_event_sequence.saturating_add(1);
        let task_key = task_id.as_deref().unwrap_or("flow");
        let generation = task_id
            .as_deref()
            .and_then(|id| self.tasks.get(id))
            .and_then(|task| task.generation)
            .unwrap_or(0);
        let event_id = format!(
            "supervisor:{}:{}:{}:{}:{}",
            self.flow_id, self.source_cursor, task_key, generation, seq
        );
        self.outbox.push_back(FlowboardOutboxEvent {
            event_id: event_id.clone(),
            request_id: event_id,
            flow_id: self.flow_id.clone(),
            actor: self.coordinator_id.clone(),
            expected_revision: self.flow_revision,
            coordinator_fence: event_fence,
            kind: kind.to_owned(),
            task_id,
            post_id,
            attempt_id,
            data: add_fence(data, self.coordinator_fence),
        });
        self.pending_sync = true;
    }
}

fn default_max_attempts() -> u32 {
    3
}

/// Long-running outbox worker owned by the Supervisor service. It waits on
/// coordinator snapshot changes, retries transient failures with bounded
/// backoff, and rebases stale revisions only after confirming the stable
/// event ID is absent from Flowboard history.
pub(crate) async fn run_flowboard_outbox(
    coordinator: CoordinatorHandle,
    sink: Arc<dyn FlowboardSink>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut snapshots = coordinator.subscribe_snapshots();
    let mut backoff = Duration::from_millis(250);
    loop {
        if *shutdown.borrow() {
            return;
        }
        let pending = snapshots
            .borrow()
            .flow_runs
            .iter()
            .find_map(|flow| flow.next_outbox().cloned());
        let Some(item) = pending else {
            tokio::select! {
                changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
                changed = snapshots.changed() => if changed.is_err() { return; },
            }
            continue;
        };
        let append_result = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return; }
                continue;
            }
            result = sink.append(&item) => result,
        };
        match append_result {
            FlowboardAppendResult::Appended { revision } => {
                let current_fence = coordinator
                    .snapshot()
                    .flow_runs
                    .iter()
                    .find(|flow| flow.flow_id == item.flow_id)
                    .map(|flow| flow.coordinator_fence)
                    .unwrap_or(item.coordinator_fence);
                let _ = coordinator
                    .acknowledge_flowboard_event_with_id(
                        format!("outbox-ack:{}:{revision}", item.event_id),
                        item.flow_id,
                        item.event_id,
                        revision,
                        current_fence,
                    )
                    .await;
                backoff = Duration::from_millis(250);
            }
            FlowboardAppendResult::RevisionConflict => match tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                    continue;
                }
                result = sink.current(&item.flow_id) => result,
            } {
                Ok(current) => {
                    let current_fence = coordinator
                        .snapshot()
                        .flow_runs
                        .iter()
                        .find(|flow| flow.flow_id == item.flow_id)
                        .map(|flow| flow.coordinator_fence)
                        .unwrap_or(item.coordinator_fence);
                    let _ = coordinator
                        .rebase_flowboard_outbox_with_id(
                            format!("outbox-rebase:{}:{}", item.event_id, current.revision),
                            item.flow_id,
                            current.revision,
                            current.event_ids,
                            current_fence,
                        )
                        .await;
                }
                Err(_) => tokio::select! {
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
                    _ = tokio::time::sleep(backoff) => {},
                },
            },
            FlowboardAppendResult::Rejected(_) | FlowboardAppendResult::Transient(_) => {
                tokio::select! {
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
                    _ = tokio::time::sleep(backoff) => {},
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Event-driven task dispatcher owned by the Supervisor service. It reserves
/// each task attempt durably before calling the configured worker host. After
/// restart, unresolved reservations are attached by idempotency key and are
/// never dispatched again.
pub(crate) async fn run_workflow_dispatcher(
    coordinator: CoordinatorHandle,
    dispatcher: Arc<dyn WorkflowDispatcher>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut snapshots = coordinator.subscribe_snapshots();
    let (finished_tx, mut finished_rx) = tokio::sync::mpsc::channel::<(String, String, u64)>(128);
    let (cancel_retry_tx, mut cancel_retry_rx) =
        tokio::sync::mpsc::channel::<(String, String, u64)>(128);
    let (dispatch_done_tx, mut dispatch_done_rx) =
        tokio::sync::mpsc::channel::<(String, String)>(128);
    let mut attached = std::collections::HashSet::new();
    let mut preparing = std::collections::HashSet::new();
    let mut cancel_sent = std::collections::HashSet::new();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let snapshot = snapshots.borrow().clone();

        for flow in &snapshot.flow_runs {
            for task in flow.tasks.values() {
                if task.cancel_requested {
                    if let Some(generation) = task.generation {
                        let key = (flow.flow_id.clone(), task.task_id.clone(), generation);
                        if cancel_sent.insert(key.clone()) {
                            if let Ok(request) = flow.dispatch_request(&task.task_id) {
                                let dispatcher = Arc::clone(&dispatcher);
                                let retry = cancel_retry_tx.clone();
                                tokio::spawn(async move {
                                    if dispatcher.cancel(&request).await.is_err() {
                                        let _ = retry.send(key).await;
                                    }
                                });
                            } else {
                                cancel_sent.remove(&key);
                            }
                        }
                    }
                }
                let should_attach =
                    task.dispatch_pending || task.state == WorkflowTaskState::Running;
                if preparing.contains(&(flow.flow_id.clone(), task.task_id.clone())) {
                    continue;
                }
                let (Some(attempt_id), Some(generation), Some(dispatch_id)) = (
                    task.attempt_id.as_ref(),
                    task.generation,
                    task.dispatch_id.as_ref(),
                ) else {
                    continue;
                };
                if !should_attach {
                    continue;
                }
                let key = (flow.flow_id.clone(), task.task_id.clone(), generation);
                if !attached.insert(key.clone()) {
                    continue;
                }
                let request = match flow.dispatch_request(&task.task_id) {
                    Ok(request) => request,
                    Err(_) => {
                        attached.remove(&key);
                        continue;
                    }
                };
                if coordinator
                    .register_flow_worker_attempt_with_id(
                        format!("flow-attempt-register:{}", request.idempotency_key),
                        request.flow_id.clone(),
                        request.task_id.clone(),
                        request.coordinator_fence,
                    )
                    .await
                    .is_err()
                {
                    attached.remove(&key);
                    continue;
                }
                let receipt = task
                    .process_id
                    .clone()
                    .zip(task.runtime_run_id.clone())
                    .map(|(process_id, runtime_run_id)| WorkflowDispatchReceipt {
                        process_id,
                        runtime_run_id,
                    });
                let dispatcher = Arc::clone(&dispatcher);
                let coordinator = coordinator.clone();
                let finished = finished_tx.clone();
                let flow_id = flow.flow_id.clone();
                let attempt_id = attempt_id.clone();
                let dispatch_id = dispatch_id.clone();
                tokio::spawn(async move {
                    match dispatcher.attach(&request, receipt.as_ref()).await {
                        Ok(mut events) => {
                            while let Some(event) = events.recv().await {
                                let Some(status) = parse_execution_status(&event.status) else {
                                    continue;
                                };
                                if event.attempt_id != attempt_id
                                    || event.generation != request.generation
                                {
                                    continue;
                                }
                                let request_id =
                                    format!("worker-event:{dispatch_id}:{}", event.source_cursor);
                                // Worker cursors are per-attempt and deterministic,
                                // so a replay after a lost acknowledgement has the
                                // same idempotency payload across service restarts.
                                // Reconcile directly against the fenced FlowRun task.
                                let outcome = AttemptOutcome {
                                    attempt_id: attempt_id.clone(),
                                    generation: event.generation,
                                    source_cursor: event.source_cursor,
                                    status: event.status.clone(),
                                    reason: event.reason.clone(),
                                    workflow_cancel_requested: coordinator
                                        .snapshot()
                                        .flow_runs
                                        .iter()
                                        .find(|flow| flow.flow_id == request.flow_id)
                                        .and_then(|flow| flow.tasks.get(&request.task_id))
                                        .is_some_and(|task| task.cancel_requested),
                                    retryable: event.retryable,
                                    evidence: event.evidence.clone(),
                                };
                                let generic_status = match status {
                                    crate::coordinator::ExecutionStatus::Queued => "queued",
                                    crate::coordinator::ExecutionStatus::Running => "running",
                                    crate::coordinator::ExecutionStatus::Succeeded => "succeeded",
                                    crate::coordinator::ExecutionStatus::Failed => "failed",
                                    crate::coordinator::ExecutionStatus::Cancelled => "cancelled",
                                    crate::coordinator::ExecutionStatus::Blocked => "blocked",
                                    crate::coordinator::ExecutionStatus::Unknown => "unknown",
                                };
                                let _ = coordinator
                                    .report_attempt_with_id(
                                        format!("flow-generic-report:{}:{}", dispatch_id, event.source_cursor),
                                        attempt_id.clone(),
                                        event.generation,
                                        status,
                                        json!({"sourceCursor":event.source_cursor,"reasonCode":event.reason,"retryable":event.retryable,"evidence":event.evidence,"flowRunId":request.flow_id,"taskId":request.task_id,"postId":request.post_id,"workerStatus":generic_status}),
                                    )
                                    .await;
                                let _ = coordinator
                                    .reconcile_flow_attempt_with_id(
                                        request_id,
                                        request.flow_id.clone(),
                                        request.task_id.clone(),
                                        outcome,
                                        request.coordinator_fence,
                                    )
                                    .await;
                                if matches!(
                                    status,
                                    crate::coordinator::ExecutionStatus::Succeeded
                                        | crate::coordinator::ExecutionStatus::Failed
                                        | crate::coordinator::ExecutionStatus::Cancelled
                                        | crate::coordinator::ExecutionStatus::Blocked
                                ) {
                                    break;
                                }
                            }
                        }
                        Err(error) => {
                            reconcile_worker_disconnect(
                                &coordinator,
                                &request,
                                &format!("worker process could not be attached: {error}"),
                            )
                            .await;
                        }
                    }
                    reconcile_worker_disconnect(
                        &coordinator,
                        &request,
                        "worker event stream ended before a terminal outcome was confirmed",
                    )
                    .await;
                    let _ = finished
                        .send((flow_id, request.task_id, request.generation))
                        .await;
                });
            }
        }

        for flow in &snapshot.flow_runs {
            if flow.coordinator_fence == 0 {
                continue;
            }
            for task_id in flow.ready_tasks() {
                let preparation_key = (flow.flow_id.clone(), task_id.clone());
                if !preparing.insert(preparation_key.clone()) {
                    continue;
                }
                let task = flow.tasks[&task_id].clone();
                let flow = flow.clone();
                let coordinator = coordinator.clone();
                let dispatcher = Arc::clone(&dispatcher);
                let done = dispatch_done_tx.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    prepare_and_dispatch_task(coordinator, dispatcher, flow, task, shutdown).await;
                    let _ = done.send(preparation_key).await;
                });
            }
        }
        tokio::select! {
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
            changed = snapshots.changed() => if changed.is_err() { return; },
            finished = finished_rx.recv() => {
                if let Some(key) = finished {
                    attached.remove(&key);
                    cancel_sent.remove(&key);
                }
            }
            retry = cancel_retry_rx.recv() => {
                if let Some(key) = retry {
                    cancel_sent.remove(&key);
                }
            }
            done = dispatch_done_rx.recv() => {
                if let Some(key) = done {
                    preparing.remove(&key);
                }
            }
        }
    }
}

async fn prepare_and_dispatch_task(
    coordinator: CoordinatorHandle,
    dispatcher: Arc<dyn WorkflowDispatcher>,
    flow: FlowRunWorkflowState,
    task: WorkflowTask,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_millis(250);
    let invocation = loop {
        if *shutdown.borrow() {
            return;
        }
        let current = coordinator.snapshot();
        let still_ready = current
            .flow_runs
            .iter()
            .find(|current| current.flow_id == flow.flow_id)
            .is_some_and(|current| {
                current.coordinator_fence == flow.coordinator_fence
                    && current
                        .tasks
                        .get(&task.task_id)
                        .is_some_and(|task| task.state == WorkflowTaskState::Ready)
            });
        if !still_ready {
            return;
        }
        match tokio::time::timeout(
            Duration::from_secs(30),
            dispatcher.resolve_invocation(&flow, &task),
        )
        .await
        {
            Ok(Ok(invocation)) => break invocation,
            Ok(Err(_)) | Err(_) => {
                tokio::select! {
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
                    _ = tokio::time::sleep(backoff) => {},
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    };
    let request_id = format!(
        "reserve-dispatch:{}:{}:{}",
        flow.flow_id,
        task.task_id,
        task.attempts_started.saturating_add(1)
    );
    let reserved = tokio::select! {
        changed = shutdown.changed() => { let _ = changed; return; },
        result = coordinator.reserve_flow_dispatch_with_id(
            request_id,
            flow.flow_id.clone(),
            task.task_id.clone(),
            flow.coordinator_fence,
        ) => result,
    };
    let Ok(reserved) = reserved else {
        return;
    };
    let Ok(request) = reserved.dispatch_request(&task.task_id) else {
        return;
    };
    if coordinator
        .register_flow_worker_attempt_with_id(
            format!("flow-attempt-register:{}", request.idempotency_key),
            request.flow_id.clone(),
            request.task_id.clone(),
            request.coordinator_fence,
        )
        .await
        .is_err()
    {
        return;
    }
    let workflow_context = reserved.dispatch_context(&task.task_id);
    let result = tokio::select! {
        changed = shutdown.changed() => { let _ = changed; return; },
        result = tokio::time::timeout(
            Duration::from_secs(60),
            dispatcher.dispatch(WorkflowDispatchInput {
                request: request.clone(),
                invocation,
                workflow_context,
            }),
        ) => result,
    };
    let Ok(Ok(receipt)) = result else {
        // The host may have accepted the reserved attempt before losing the
        // reply; keep the reservation and recover through the attempt ID.
        return;
    };
    let _ = coordinator
        .complete_flow_dispatch_with_id(
            format!("complete-dispatch:{}", request.idempotency_key),
            request.flow_id.clone(),
            request.clone(),
            receipt,
            request.coordinator_fence,
        )
        .await;
}

async fn reconcile_worker_disconnect(
    coordinator: &CoordinatorHandle,
    request: &WorkflowDispatchRequest,
    reason: &str,
) {
    let snapshot = coordinator.snapshot();
    let Some(flow) = snapshot
        .flow_runs
        .iter()
        .find(|flow| flow.flow_id == request.flow_id)
    else {
        return;
    };
    let Some(task) = flow.tasks.get(&request.task_id) else {
        return;
    };
    if task.attempt_id.as_deref() != Some(request.attempt_id.as_str())
        || task.generation != Some(request.generation)
        || !(task.dispatch_pending
            || matches!(
                task.state,
                WorkflowTaskState::Assigned | WorkflowTaskState::Running
            ))
    {
        return;
    }
    let source_cursor = task.last_source_cursor.saturating_add(1);
    let outcome = AttemptOutcome {
        attempt_id: request.attempt_id.clone(),
        generation: request.generation,
        source_cursor,
        status: "unknown".to_owned(),
        reason: Some(reason.to_owned()),
        workflow_cancel_requested: task.cancel_requested,
        retryable: false,
        evidence: None,
    };
    let _ = coordinator
        .report_attempt_with_id(
            format!("worker-generic-disconnect:{}:{source_cursor}", request.idempotency_key),
            request.attempt_id.clone(),
            request.generation,
            crate::coordinator::ExecutionStatus::Unknown,
            json!({"flowRunId":request.flow_id,"taskId":request.task_id,"postId":request.post_id,"reasonCode":"worker_outcome_unknown","reason":reason}),
        )
        .await;
    let _ = coordinator
        .reconcile_flow_attempt_with_id(
            format!(
                "worker-attach-unknown:{}:{source_cursor}",
                request.idempotency_key
            ),
            request.flow_id.clone(),
            request.task_id.clone(),
            outcome,
            request.coordinator_fence,
        )
        .await;
}

fn parse_execution_status(status: &str) -> Option<crate::coordinator::ExecutionStatus> {
    match status {
        "queued" => Some(crate::coordinator::ExecutionStatus::Queued),
        "running" => Some(crate::coordinator::ExecutionStatus::Running),
        "succeeded" => Some(crate::coordinator::ExecutionStatus::Succeeded),
        "failed" => Some(crate::coordinator::ExecutionStatus::Failed),
        "cancelled" => Some(crate::coordinator::ExecutionStatus::Cancelled),
        "blocked" => Some(crate::coordinator::ExecutionStatus::Blocked),
        "unknown" => Some(crate::coordinator::ExecutionStatus::Unknown),
        _ => None,
    }
}

fn add_fence(mut data: Value, fence: u64) -> Value {
    if !data.is_object() {
        data = json!({"detail":data});
    }
    data["coordinatorFence"] = json!(fence);
    data
}

fn ensure_acyclic(tasks: &BTreeMap<String, WorkflowTask>) -> Result<(), WorkflowError> {
    fn visit(
        id: &str,
        tasks: &BTreeMap<String, WorkflowTask>,
        visiting: &mut Vec<String>,
        visited: &mut Vec<String>,
    ) -> Result<(), WorkflowError> {
        if visited.iter().any(|current| current == id) {
            return Ok(());
        }
        if visiting.iter().any(|current| current == id) {
            return Err(WorkflowError::InvalidDependencies(format!(
                "dependency cycle includes {id}"
            )));
        }
        visiting.push(id.to_owned());
        if let Some(task) = tasks.get(id) {
            for dependency in &task.dependencies {
                visit(dependency, tasks, visiting, visited)?;
            }
        }
        visiting.pop();
        visited.push(id.to_owned());
        Ok(())
    }
    let mut visiting = Vec::new();
    let mut visited = Vec::new();
    for id in tasks.keys() {
        visit(id, tasks, &mut visiting, &mut visited)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn projection() -> FlowRunProjection {
        FlowRunProjection {
            flow_id: "flow-1".to_owned(),
            board_id: "board-1".to_owned(),
            flow_revision: 10,
            current_stage: None,
            stage_transitions: BTreeMap::new(),
            max_attempts: 3,
            tasks: vec![
                WorkflowTask {
                    task_id: "a".to_owned(),
                    post_id: "post-a".to_owned(),
                    title: "Build base".to_owned(),
                    stage_id: None,
                    supervisor_run_id: None,
                    supervisor_activity_id: None,
                    dependencies: vec![],
                    assignee: Some("agent-a".to_owned()),
                    state: WorkflowTaskState::Waiting,
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
                },
                WorkflowTask {
                    task_id: "b".to_owned(),
                    post_id: "post-b".to_owned(),
                    title: "Build dependent".to_owned(),
                    stage_id: None,
                    supervisor_run_id: None,
                    supervisor_activity_id: None,
                    dependencies: vec!["a".to_owned()],
                    assignee: Some("agent-b".to_owned()),
                    state: WorkflowTaskState::Waiting,
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
                },
            ],
        }
    }

    fn state() -> FlowRunWorkflowState {
        FlowRunWorkflowState::from_projection(projection(), "supervisor-1".to_owned(), 0)
            .expect("valid projection")
    }

    fn staged_state() -> FlowRunWorkflowState {
        let mut projection = projection();
        projection.current_stage = Some("planning".into());
        projection.stage_transitions = BTreeMap::from([
            ("planning".into(), vec!["execution".into()]),
            ("execution".into(), vec![]),
        ]);
        projection.tasks[0].stage_id = Some("planning".into());
        projection.tasks[1].stage_id = Some("execution".into());
        FlowRunWorkflowState::from_projection(projection, "supervisor-1".into(), 0).unwrap()
    }

    struct WorkerUnavailableAdapter {
        attach_calls: AtomicUsize,
    }

    impl WorkflowDispatcher for WorkerUnavailableAdapter {
        fn resolve_invocation<'a>(
            &'a self,
            _flow: &'a FlowRunWorkflowState,
            _task: &'a WorkflowTask,
        ) -> SinkFuture<'a, Result<Value, String>> {
            Box::pin(async { Ok(json!({})) })
        }

        fn dispatch<'a>(
            &'a self,
            _input: WorkflowDispatchInput,
        ) -> SinkFuture<'a, Result<WorkflowDispatchReceipt, String>> {
            Box::pin(async { Err("unexpected dispatch".to_owned()) })
        }

        fn attach<'a>(
            &'a self,
            _request: &'a WorkflowDispatchRequest,
            _receipt: Option<&'a WorkflowDispatchReceipt>,
        ) -> SinkFuture<'a, Result<tokio::sync::mpsc::Receiver<WorkflowWorkerEvent>, String>>
        {
            self.attach_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err("worker process is absent after service restart".to_owned()) })
        }

        fn resume<'a>(
            &'a self,
            _request: &'a WorkflowDispatchRequest,
            _input: Value,
        ) -> SinkFuture<'a, Result<(), String>> {
            Box::pin(async { Err("worker process is unavailable".to_owned()) })
        }

        fn cancel<'a>(
            &'a self,
            _request: &'a WorkflowDispatchRequest,
        ) -> SinkFuture<'a, Result<(), String>> {
            Box::pin(async { Err("worker process is unavailable".to_owned()) })
        }
    }

    struct SlowCancelAdapter {
        cancel_calls: AtomicUsize,
        cancel_release: Arc<tokio::sync::Notify>,
    }

    impl WorkflowDispatcher for SlowCancelAdapter {
        fn resolve_invocation<'a>(
            &'a self,
            _flow: &'a FlowRunWorkflowState,
            _task: &'a WorkflowTask,
        ) -> SinkFuture<'a, Result<Value, String>> {
            Box::pin(async { Ok(json!({})) })
        }

        fn dispatch<'a>(
            &'a self,
            _input: WorkflowDispatchInput,
        ) -> SinkFuture<'a, Result<WorkflowDispatchReceipt, String>> {
            Box::pin(async { Err("unexpected dispatch".to_owned()) })
        }

        fn attach<'a>(
            &'a self,
            _request: &'a WorkflowDispatchRequest,
            _receipt: Option<&'a WorkflowDispatchReceipt>,
        ) -> SinkFuture<'a, Result<tokio::sync::mpsc::Receiver<WorkflowWorkerEvent>, String>>
        {
            Box::pin(async { Err("test host has no worker stream".to_owned()) })
        }

        fn resume<'a>(
            &'a self,
            _request: &'a WorkflowDispatchRequest,
            _input: Value,
        ) -> SinkFuture<'a, Result<(), String>> {
            Box::pin(async { Err("unexpected resume".to_owned()) })
        }

        fn cancel<'a>(
            &'a self,
            _request: &'a WorkflowDispatchRequest,
        ) -> SinkFuture<'a, Result<(), String>> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            let release = Arc::clone(&self.cancel_release);
            Box::pin(async move {
                release.notified().await;
                Ok(())
            })
        }
    }

    #[test]
    fn dependency_readiness_and_cycles_are_validated() {
        let state = state();
        assert_eq!(state.ready_tasks(), vec!["a"]);
        assert_eq!(state.tasks["b"].state, WorkflowTaskState::Waiting);

        let mut cyclic = projection();
        cyclic.tasks[0].dependencies.push("b".to_owned());
        assert!(matches!(
            FlowRunWorkflowState::from_projection(cyclic, "supervisor".to_owned(), 0),
            Err(WorkflowError::InvalidDependencies(_))
        ));
    }

    #[test]
    fn stage_dispatch_waits_for_review_and_advances_only_over_declared_edges() {
        let mut state = staged_state();
        assert_eq!(state.ready_tasks(), vec!["a"]);
        assert_eq!(
            state.tasks["b"].waiting_reason.as_deref(),
            Some("waiting for stage execution")
        );
        assert_eq!(
            state.advance_stage("planning", "execution", 10, 0),
            Err(WorkflowError::NotReady)
        );

        let request = state.reserve_dispatch("a", 0).unwrap();
        state
            .complete_dispatch(
                &request,
                WorkflowDispatchReceipt {
                    process_id: "process-a".into(),
                    runtime_run_id: "runtime-a".into(),
                },
                0,
            )
            .unwrap();
        state
            .reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: request.attempt_id,
                    generation: request.generation,
                    source_cursor: 1,
                    status: "succeeded".into(),
                    reason: None,
                    workflow_cancel_requested: false,
                    retryable: false,
                    evidence: Some(json!({"output":"reviewable result"})),
                },
                0,
            )
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::ReadyForReview);
        assert_eq!(
            state.tasks["a"].last_outcome.as_ref().unwrap().evidence,
            Some(json!({"output":"reviewable result"}))
        );
        assert_eq!(state.ready_tasks(), Vec::<String>::new());
        state
            .apply_review_approved("a", 11, json!({"verdict":"approved"}), 0)
            .unwrap();

        assert_eq!(
            state.advance_stage("planning", "missing", 11, 0),
            Err(WorkflowError::InvalidStage(
                "transition planning -> missing is not declared".into()
            ))
        );
        state.advance_stage("planning", "execution", 11, 0).unwrap();
        assert_eq!(state.current_stage.as_deref(), Some("execution"));
        assert_eq!(state.ready_tasks(), vec!["b"]);
        assert_eq!(state.outbox.back().unwrap().kind, "stage_changed");
    }

    #[test]
    fn fences_and_generations_reject_stale_attempts() {
        let mut state = state();
        assert_eq!(
            state.bind_attempt("a", "attempt-2".to_owned(), 2, 1, 1),
            Err(WorkflowError::FenceMismatch)
        );
        state
            .bind_attempt("a", "attempt-2".to_owned(), 2, 1, 0)
            .unwrap();
        assert_eq!(
            state.bind_attempt("a", "attempt-1".to_owned(), 1, 2, 0),
            Err(WorkflowError::StaleGeneration)
        );
        assert_eq!(
            state.reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: "attempt-2".to_owned(),
                    generation: 2,
                    source_cursor: 2,
                    status: "succeeded".to_owned(),
                    reason: None,
                    workflow_cancel_requested: false,
                    retryable: false,
                    evidence: None,
                },
                1,
            ),
            Err(WorkflowError::FenceMismatch)
        );
    }

    #[test]
    fn exhausted_coordinator_fence_fails_closed() {
        let mut state = state();
        state.coordinator_fence = u64::MAX;
        assert_eq!(
            state.claim("next-owner".to_owned(), u64::MAX),
            Err(WorkflowError::FenceExhausted)
        );
        assert!(state.coordinator_fence_exhausted);
        assert_eq!(
            state.bind_attempt("a", "attempt".into(), 1, 1, u64::MAX),
            Err(WorkflowError::FenceExhausted)
        );
    }

    #[test]
    fn task_cancellation_is_fenced_and_requires_worker_acknowledgement() {
        let mut state = state();
        state
            .bind_attempt("a", "attempt-cancel".to_owned(), 1, 1, 0)
            .unwrap();
        assert_eq!(
            state.request_task_cancel("a", "stale-attempt", 1, 0),
            Err(WorkflowError::StaleGeneration)
        );
        state
            .request_task_cancel("a", "attempt-cancel", 1, 0)
            .unwrap();
        assert!(state.tasks["a"].cancel_requested);
        assert_eq!(
            state.tasks["a"].waiting_reason.as_deref(),
            Some("worker cancellation requested")
        );
        assert_eq!(
            state.reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: "attempt-cancel".to_owned(),
                    generation: 1,
                    source_cursor: 2,
                    status: "cancelled".to_owned(),
                    reason: Some("worker_cancelled".to_owned()),
                    workflow_cancel_requested: true,
                    retryable: false,
                    evidence: None,
                },
                0,
            ),
            Ok(WorkflowTaskState::Cancelled)
        );
        assert!(!state.tasks["a"].cancel_requested);
    }

    #[tokio::test]
    async fn disconnected_worker_is_marked_unknown_without_redispatch_or_busy_loop() {
        let coordinator = CoordinatorHandle::spawn("workflow-disconnect-test");
        let flow = coordinator
            .register_flowrun_with_id(
                "register-disconnect-test".into(),
                projection(),
                "supervisor".into(),
            )
            .await
            .unwrap();
        let flow = coordinator
            .claim_flowrun_with_id(
                "claim-disconnect-test".into(),
                flow.flow_id.clone(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        let flow = coordinator
            .reserve_flow_dispatch_with_id(
                "reserve-disconnect-test".into(),
                flow.flow_id.clone(),
                "a".into(),
                flow.coordinator_fence,
            )
            .await
            .unwrap();
        let request = flow.dispatch_request("a").unwrap();
        coordinator
            .register_flow_worker_attempt_with_id(
                format!("flow-attempt-register:{}", request.idempotency_key),
                request.flow_id.clone(),
                request.task_id.clone(),
                request.coordinator_fence,
            )
            .await
            .unwrap();
        coordinator
            .complete_flow_dispatch_with_id(
                "complete-disconnect-test".into(),
                flow.flow_id.clone(),
                request.clone(),
                WorkflowDispatchReceipt {
                    process_id: "stale-process".into(),
                    runtime_run_id: "runtime-attempt".into(),
                },
                flow.coordinator_fence,
            )
            .await
            .unwrap();

        let adapter = Arc::new(WorkerUnavailableAdapter {
            attach_calls: AtomicUsize::new(0),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_task = tokio::spawn(run_workflow_dispatcher(
            coordinator.clone(),
            adapter.clone(),
            shutdown_rx,
        ));
        let reconciled = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = coordinator.snapshot();
                let task = &snapshot.flow_runs[0].tasks["a"];
                if task.state == WorkflowTaskState::Waiting
                    && !task.dispatch_pending
                    && task
                        .waiting_reason
                        .as_deref()
                        .is_some_and(|reason| reason.contains("outcome is unknown"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            reconciled.is_ok(),
            "disconnected attempt was not reconciled: {:?}",
            coordinator.snapshot().flow_runs
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        let task = &coordinator.snapshot().flow_runs[0].tasks["a"];
        assert_eq!(task.state, WorkflowTaskState::Waiting);
        assert!(!task.dispatch_pending);
        assert_eq!(
            task.attempt_id.as_deref(),
            Some(request.attempt_id.as_str())
        );
        assert_eq!(adapter.attach_calls.load(Ordering::SeqCst), 1);
        let recovered = coordinator.snapshot();
        assert_eq!(recovered.attempts.len(), 1);
        assert_eq!(recovered.attempts[0].attempt_id, request.attempt_id);
        assert_eq!(
            recovered.attempts[0].status,
            crate::coordinator::ExecutionStatus::Unknown
        );
        assert!(recovered.processes.is_empty());
        shutdown_tx.send_replace(true);
        loop_task.await.unwrap();
    }

    #[tokio::test]
    async fn slow_cancel_callbacks_do_not_block_sibling_cancellation_or_snapshot_updates() {
        let coordinator = CoordinatorHandle::spawn("workflow-slow-cancel-test");
        let mut flow_projection = projection();
        flow_projection.tasks[1].dependencies.clear();
        let mut flow = coordinator
            .register_flowrun_with_id(
                "register-slow-cancel-test".into(),
                flow_projection,
                "supervisor".into(),
            )
            .await
            .unwrap();
        flow = coordinator
            .claim_flowrun_with_id(
                "claim-slow-cancel-test".into(),
                flow.flow_id.clone(),
                "supervisor".into(),
                0,
            )
            .await
            .unwrap();
        for task_id in ["a", "b"] {
            flow = coordinator
                .reserve_flow_dispatch_with_id(
                    format!("reserve-slow-cancel-{task_id}"),
                    flow.flow_id.clone(),
                    task_id.into(),
                    flow.coordinator_fence,
                )
                .await
                .unwrap();
            let request = flow.dispatch_request(task_id).unwrap();
            coordinator
                .register_flow_worker_attempt_with_id(
                    format!("flow-attempt-register:{}", request.idempotency_key),
                    request.flow_id.clone(),
                    request.task_id.clone(),
                    request.coordinator_fence,
                )
                .await
                .unwrap();
            flow = coordinator
                .complete_flow_dispatch_with_id(
                    format!("complete-slow-cancel-{task_id}"),
                    flow.flow_id.clone(),
                    request.clone(),
                    WorkflowDispatchReceipt {
                        process_id: format!("process-{task_id}"),
                        runtime_run_id: format!("runtime-{task_id}"),
                    },
                    flow.coordinator_fence,
                )
                .await
                .unwrap();
            coordinator
                .request_flow_task_cancel_with_id(
                    format!("cancel-slow-{task_id}"),
                    flow.flow_id.clone(),
                    task_id.into(),
                    request.attempt_id,
                    request.generation,
                    flow.coordinator_fence,
                )
                .await
                .unwrap();
        }

        let adapter = Arc::new(SlowCancelAdapter {
            cancel_calls: AtomicUsize::new(0),
            cancel_release: Arc::new(tokio::sync::Notify::new()),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let loop_task = tokio::spawn(run_workflow_dispatcher(
            coordinator.clone(),
            adapter.clone(),
            shutdown_rx,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            while adapter.cancel_calls.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("both slow cancellation callbacks start concurrently");

        let mut unrelated = projection();
        unrelated.flow_id = "flow-unrelated-snapshot".into();
        coordinator
            .register_flowrun_with_id(
                "register-unrelated-snapshot".into(),
                unrelated,
                "supervisor".into(),
            )
            .await
            .unwrap();
        assert!(coordinator
            .snapshot()
            .flow_runs
            .iter()
            .any(|flow| flow.flow_id == "flow-unrelated-snapshot"));

        shutdown_tx.send_replace(true);
        adapter.cancel_release.notify_waiters();
        loop_task.await.unwrap();
    }

    #[test]
    fn process_exit_and_success_never_complete_without_flowboard_review() {
        let mut state = state();
        state
            .bind_attempt("a", "attempt-1".to_owned(), 1, 1, 0)
            .unwrap();
        state
            .observe_process_exit("a", "process-1".to_owned(), "exit:0".to_owned(), 2, 0)
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::Assigned);

        state
            .reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: "attempt-1".to_owned(),
                    generation: 1,
                    source_cursor: 3,
                    status: "succeeded".to_owned(),
                    reason: None,
                    workflow_cancel_requested: false,
                    retryable: false,
                    evidence: None,
                },
                0,
            )
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::ReadyForReview);
        assert!(state
            .outbox
            .iter()
            .any(|event| event.kind == "ready_for_review"));
        assert_eq!(state.tasks["b"].state, WorkflowTaskState::Waiting);

        state
            .apply_review_approved("a", 11, json!({"review":"approved"}), 0)
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::Completed);
        assert_eq!(state.tasks["b"].state, WorkflowTaskState::Ready);
    }

    #[test]
    fn unrequested_cancel_blocks_and_outbox_rebases_idempotently() {
        let mut state = state();
        state.claim("supervisor-2".to_owned(), 0).unwrap();
        state
            .bind_attempt("a", "attempt-1".to_owned(), 1, 1, 1)
            .unwrap();
        state
            .reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: "attempt-1".to_owned(),
                    generation: 1,
                    source_cursor: 2,
                    status: "cancelled".to_owned(),
                    reason: Some("worker stopped".to_owned()),
                    workflow_cancel_requested: false,
                    retryable: false,
                    evidence: None,
                },
                1,
            )
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::Blocked);

        let first = state.next_outbox().unwrap().clone();
        state.rebase_outbox(15, &[], 1).unwrap();
        let rebased = state.next_outbox().unwrap();
        assert_eq!(rebased.event_id, first.event_id);
        assert_eq!(rebased.expected_revision, 15);
        let request_id = rebased.request_id.clone();
        state.rebase_outbox(15, &[], 1).unwrap();
        assert_eq!(state.next_outbox().unwrap().request_id, request_id);
        state.acknowledge_outbox(&first.event_id, 16, 1).unwrap();
        let mut revision = 17;
        while let Some(event) = state.next_outbox().cloned() {
            state
                .acknowledge_outbox(&event.event_id, revision, 1)
                .unwrap();
            revision += 1;
        }
        assert!(!state.pending_sync);
    }

    #[test]
    fn retryable_failures_obey_the_declared_attempt_cap() {
        let mut capped = projection();
        capped.max_attempts = 2;
        let mut state = FlowRunWorkflowState::from_projection(capped, "supervisor-1".to_owned(), 0)
            .expect("valid capped projection");
        state.claim("supervisor-2".to_owned(), 0).unwrap();

        let first = state.reserve_dispatch("a", 1).unwrap();
        state
            .reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: first.attempt_id.clone(),
                    generation: first.generation,
                    source_cursor: 1,
                    status: "failed".to_owned(),
                    reason: Some("temporary".to_owned()),
                    workflow_cancel_requested: false,
                    retryable: true,
                    evidence: None,
                },
                1,
            )
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::Ready);

        let second = state.reserve_dispatch("a", 1).unwrap();
        assert_eq!(second.generation, 2);
        state
            .reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: second.attempt_id,
                    generation: second.generation,
                    source_cursor: 2,
                    status: "failed".to_owned(),
                    reason: Some("attempt cap reached".to_owned()),
                    workflow_cancel_requested: false,
                    retryable: true,
                    evidence: None,
                },
                1,
            )
            .unwrap();
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::Blocked);
        assert_eq!(state.tasks["a"].attempts_started, 2);
    }

    #[test]
    fn unknown_dispatch_outcome_requires_reconciliation_before_retry() {
        let mut state = state();
        state.claim("supervisor-2".to_owned(), 0).unwrap();
        let request = state.reserve_dispatch("a", 1).unwrap();
        state
            .reconcile_attempt(
                "a",
                AttemptOutcome {
                    attempt_id: request.attempt_id,
                    generation: request.generation,
                    source_cursor: 1,
                    status: "unknown".to_owned(),
                    reason: Some("worker could not be reattached".to_owned()),
                    workflow_cancel_requested: false,
                    retryable: false,
                    evidence: None,
                },
                1,
            )
            .unwrap();
        assert!(!state.tasks["a"].dispatch_pending);
        assert_eq!(state.tasks["a"].state, WorkflowTaskState::Waiting);
        assert!(!state.ready_tasks().contains(&"a".to_owned()));
    }
}
