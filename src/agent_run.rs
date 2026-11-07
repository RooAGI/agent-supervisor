//! Rust-owned lifecycle for processes associated with one agent run.
use crate::{
    Command, ExecutionOutput, Policy, SandboxError, SandboxErrorCode, SandboxPhase,
    SupervisedChild, Supervisor,
};
use std::time::Duration;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex, Weak},
};
use tokio_util::sync::CancellationToken;

/// Limits and cleanup behavior for one agent run.
#[derive(Debug, Clone)]
pub struct AgentRunPolicy {
    /// Maximum wall time for the run.
    pub timeout: Duration,
    /// Grace period before registered process groups are forcibly terminated.
    pub shutdown_grace: Duration,
}

impl Default for AgentRunPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            shutdown_grace: Duration::from_secs(5),
        }
    }
}

/// Registry of agent runs owned by a top-level [`Supervisor`](crate::Supervisor).
#[derive(Default)]
pub(crate) struct AgentRunRegistry {
    next_id: AtomicU64,
    runs: Mutex<BTreeMap<u64, Weak<AgentRunInner>>>,
}

impl AgentRunRegistry {
    pub(crate) fn active_count(&self) -> usize {
        let mut runs = lock_runs(&self.runs);
        runs.retain(|_, run| run.strong_count() > 0);
        runs.len()
    }

    pub(crate) async fn cancel_all(&self) {
        let runs = lock_runs(&self.runs)
            .values()
            .filter_map(Weak::upgrade)
            .map(|inner| AgentRun { inner })
            .collect::<Vec<_>>();
        futures::future::join_all(runs.iter().map(AgentRun::cancel)).await;
    }

    pub(crate) fn cancel_all_now(&self) {
        let runs = lock_runs(&self.runs)
            .values()
            .filter_map(Weak::upgrade)
            .map(|inner| AgentRun { inner })
            .collect::<Vec<_>>();
        for run in runs {
            run.cancel_now();
        }
    }
}

/// One agent-framework invocation owned by a top-level [`Supervisor`](crate::Supervisor).
/// It tracks the local processes started for that invocation. Call `cancel()` when
/// the invocation completes to clean up any persistent run-owned processes.
#[derive(Clone)]
pub struct AgentRun {
    inner: Arc<AgentRunInner>,
}

struct AgentRunInner {
    id: u64,
    policy: AgentRunPolicy,
    cancellation: CancellationToken,
    supervisor: Supervisor,
    deadline: tokio::time::Instant,
    registry: Weak<AgentRunRegistry>,
}

impl AgentRun {
    pub(crate) fn new(policy: AgentRunPolicy, owner: &Supervisor) -> Self {
        let registry = owner.agent_run_registry();
        let id = registry.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let deadline = tokio::time::Instant::now() + policy.timeout;
        let inner = Arc::new(AgentRunInner {
            policy,
            cancellation: CancellationToken::new(),
            supervisor: owner.agent_run_view(id),
            deadline,
            registry: Arc::downgrade(registry),
            id,
        });
        let run = Self { inner };
        lock_runs(&registry.runs).insert(id, Arc::downgrade(&run.inner));
        owner.event_log().run_started(id);
        run
    }

    fn unregister(&self) {
        if let Some(registry) = self.inner.registry.upgrade() {
            if lock_runs(&registry.runs).remove(&self.inner.id).is_some() {
                self.inner.supervisor.event_log().run_ended(self.inner.id);
            }
        }
    }

    /// This run's ID within its supervisor, as seen in
    /// [`SupervisorEvent::run_id`](crate::SupervisorEvent::run_id).
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub(crate) fn supervisor_uuid(&self) -> uuid::Uuid {
        self.inner.supervisor.uuid()
    }

    /// Publish an agent framework's event to watchers of this run. The payload
    /// is carried unchanged and never reaches the model.
    pub fn publish_agent_event(&self, payload: crate::AgentEventPayload) {
        self.inner.supervisor.event_log().publish(
            Some(self.inner.id),
            None,
            crate::SupervisorEventKind::Agent(payload),
        );
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    pub fn cancel_now(&self) {
        self.inner.cancellation.cancel();
        self.inner.supervisor.terminate_all();
        self.unregister();
    }

    /// Signal cancellation without bypassing the configured graceful period.
    pub fn request_cancel(&self) {
        self.inner.cancellation.cancel();
    }

    pub fn active_processes(&self) -> usize {
        self.inner.supervisor.active_processes()
    }

    pub fn start(
        &self,
        command: &Command,
        policy: &Policy,
    ) -> Result<SupervisedChild, SandboxError> {
        if self.inner.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        if tokio::time::Instant::now() >= self.inner.deadline {
            self.cancel_now();
            return Err(timeout_error());
        }
        self.inner.supervisor.start(command, policy)
    }

    pub async fn run_command(
        &self,
        command: &Command,
        policy: &Policy,
        input: &[u8],
    ) -> Result<ExecutionOutput, SandboxError> {
        if self.inner.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let request = command.request(policy);
        let remaining = self
            .inner
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            self.cancel().await;
            return Err(timeout_error());
        }
        let token = self.inner.cancellation.clone();
        match tokio::time::timeout(
            remaining,
            crate::supervisor::execute_with_tap(
                &request,
                input,
                token,
                self.inner.supervisor.one_shot_tap(),
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.cancel().await;
                Err(timeout_error())
            }
        }
    }

    /// Run an async SDK operation under this run's deadline. Timeout or
    /// cancellation cleans up local processes; call `cancel()` after normal
    /// completion to finish the run and clean up its processes.
    pub async fn run<T, F>(&self, operation: F) -> Result<T, SandboxError>
    where
        F: std::future::Future<Output = T>,
    {
        if self.inner.cancellation.is_cancelled() {
            self.cancel().await;
            return Err(cancelled_error());
        }
        let remaining = self
            .inner
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            self.cancel().await;
            return Err(timeout_error());
        }
        let result = tokio::select! {
            biased;
            _ = self.inner.cancellation.cancelled() => None,
            result = tokio::time::timeout(remaining, operation) => Some(result),
        };
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(_)) => {
                self.cancel().await;
                Err(timeout_error())
            }
            None => {
                self.cancel().await;
                Err(cancelled_error())
            }
        }
    }

    pub async fn cancel(&self) {
        self.request_cancel();
        self.inner
            .supervisor
            .terminate_all_with_grace(self.inner.policy.shutdown_grace)
            .await;
        self.unregister();
    }
}

impl Drop for AgentRunInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.supervisor.terminate_all();
        if let Some(registry) = self.registry.upgrade() {
            if lock_runs(&registry.runs).remove(&self.id).is_some() {
                self.supervisor.event_log().run_ended(self.id);
            }
        }
    }
}

fn lock_runs(
    runs: &Mutex<BTreeMap<u64, Weak<AgentRunInner>>>,
) -> std::sync::MutexGuard<'_, BTreeMap<u64, Weak<AgentRunInner>>> {
    runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn timeout_error() -> SandboxError {
    SandboxError::new(
        SandboxErrorCode::TimedOut,
        SandboxPhase::Wait,
        "agent run exceeded its deadline",
    )
}

fn cancelled_error() -> SandboxError {
    SandboxError::new(
        SandboxErrorCode::ExecutionFailed,
        SandboxPhase::Shutdown,
        "agent run was cancelled",
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn policy_defaults_match_documented_run_limits() {
        let policy = AgentRunPolicy::default();
        assert_eq!(policy.timeout, Duration::from_secs(300));
        assert_eq!(policy.shutdown_grace, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn one_shot_command_uses_agent_deadline() {
        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_millis(40),
            shutdown_grace: Duration::ZERO,
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "5".into()],
            cwd: None,
        };
        let error = run
            .run_command(&command, &Policy::default(), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code(), "timed_out");
    }

    #[tokio::test]
    async fn sdk_operation_uses_deadline_from_creation() {
        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_millis(40),
            shutdown_grace: Duration::ZERO,
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let error = run.run(async {}).await.unwrap_err();
        assert_eq!(error.code(), "timed_out");
    }

    #[tokio::test]
    async fn cancel_terminates_registered_processes_and_unregisters_run() {
        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let child = run.start(&command, &Policy::default()).unwrap();
        assert_eq!(run.active_processes(), 1);
        assert_eq!(owner.active_agent_runs(), 1);
        assert_eq!(owner.active_processes(), 1);

        run.cancel().await;
        drop(child);

        assert_eq!(run.active_processes(), 0);
        assert_eq!(owner.active_agent_runs(), 0);
        assert_eq!(owner.active_processes(), 0);
        let error = run
            .run_command(&command, &Policy::default(), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code(), "execution_failed");
    }

    #[tokio::test]
    async fn run_returns_successful_operation_value() {
        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy::default());
        assert_eq!(run.run(async { 42 }).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn request_cancel_cancels_operation_and_cleans_registered_processes() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        struct DropSignal(Arc<AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let dropped = Arc::new(AtomicBool::new(false));
        let operation_dropped = dropped.clone();
        let cancel_run = run.clone();
        let task = tokio::spawn(async move {
            cancel_run
                .run(async move {
                    let _drop_signal = DropSignal(operation_dropped);
                    std::future::pending::<()>().await;
                })
                .await
        });

        let _child = run.start(&command, &Policy::default()).unwrap();
        tokio::task::yield_now().await;
        run.request_cancel();

        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code(), "execution_failed");
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(run.active_processes(), 0);
    }

    #[tokio::test]
    async fn cancellation_token_clone_cancels_the_shared_run() {
        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
        });
        let token = run.cancellation_token();
        let task_run = run.clone();
        let task = tokio::spawn(async move { task_run.run(std::future::pending::<()>()).await });
        tokio::task::yield_now().await;

        token.cancel();

        assert_eq!(task.await.unwrap().unwrap_err().code(), "execution_failed");
    }

    #[tokio::test]
    async fn cancel_now_stops_process_without_waiting_for_grace_period() {
        let owner = Supervisor::new();
        let run = owner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(30),
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let _child = run.start(&command, &Policy::default()).unwrap();
        assert_eq!(run.active_processes(), 1);

        run.cancel_now();
        tokio::time::timeout(Duration::from_secs(2), async {
            while run.active_processes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancel_now should terminate registered processes immediately");
    }
}
