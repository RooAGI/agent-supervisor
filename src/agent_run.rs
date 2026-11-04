//! Rust-owned lifecycle for processes associated with one agent run.
use crate::{
    Command, ExecutionOutput, Policy, SandboxError, SandboxErrorCode, SandboxPhase,
    SupervisedChild, Supervisor,
};
use std::time::Duration;
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

/// Shared Rust owner for local processes started during one agent run.
///
/// Agent SDK adapters keep a clone for the duration of the SDK run and route
/// their local process tools through this scope. Cancellation terminates every
/// process group registered with the scope.
#[derive(Clone)]
pub struct AgentRunScope {
    policy: AgentRunPolicy,
    cancellation: CancellationToken,
    supervisor: std::sync::Arc<Supervisor>,
    deadline: tokio::time::Instant,
}

impl AgentRunScope {
    pub fn new(policy: AgentRunPolicy) -> Self {
        let deadline = tokio::time::Instant::now() + policy.timeout;
        Self {
            policy,
            cancellation: CancellationToken::new(),
            supervisor: std::sync::Arc::new(Supervisor::new()),
            deadline,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn cancel_now(&self) {
        self.cancellation.cancel();
        self.supervisor.terminate_all();
    }

    /// Signal cancellation without bypassing the configured graceful period.
    pub fn request_cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn active_processes(&self) -> usize {
        self.supervisor.active_processes()
    }

    pub fn supervisor(&self) -> std::sync::Arc<Supervisor> {
        self.supervisor.clone()
    }

    pub fn start(
        &self,
        command: &Command,
        policy: &Policy,
    ) -> Result<SupervisedChild, SandboxError> {
        if self.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        if tokio::time::Instant::now() >= self.deadline {
            self.cancel_now();
            return Err(timeout_error());
        }
        self.supervisor.start(command, policy)
    }

    pub async fn run_command(
        &self,
        command: &Command,
        policy: &Policy,
        input: &[u8],
    ) -> Result<ExecutionOutput, SandboxError> {
        if self.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let request = command.request(policy);
        let remaining = self
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            self.cancel().await;
            return Err(timeout_error());
        }
        let token = self.cancellation.clone();
        match tokio::time::timeout(
            remaining,
            crate::execute_with_cancellation(&request, input, token),
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

    /// Run an async SDK operation under the scope deadline. A timeout or
    /// cancellation stops all process groups started through this scope.
    pub async fn run<T, F>(&self, operation: F) -> Result<T, SandboxError>
    where
        F: std::future::Future<Output = T>,
    {
        if self.cancellation.is_cancelled() {
            self.cancel().await;
            return Err(cancelled_error());
        }
        let remaining = self
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            self.cancel().await;
            return Err(timeout_error());
        }
        let result = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => None,
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
        self.supervisor
            .terminate_all_with_grace(self.policy.shutdown_grace)
            .await;
    }
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
        let scope = AgentRunScope::new(AgentRunPolicy {
            timeout: Duration::from_millis(40),
            shutdown_grace: Duration::ZERO,
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "5".into()],
            cwd: None,
        };
        let error = scope
            .run_command(&command, &Policy::default(), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code(), "timed_out");
    }

    #[tokio::test]
    async fn sdk_operation_uses_deadline_from_scope_creation() {
        let scope = AgentRunScope::new(AgentRunPolicy {
            timeout: Duration::from_millis(40),
            shutdown_grace: Duration::ZERO,
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let error = scope.run(async { () }).await.unwrap_err();
        assert_eq!(error.code(), "timed_out");
    }

    #[tokio::test]
    async fn cancel_terminates_registered_processes_and_closes_scope() {
        let scope = AgentRunScope::new(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let _child = scope.start(&command, &Policy::default()).unwrap();
        assert_eq!(scope.active_processes(), 1);

        scope.cancel().await;

        assert_eq!(scope.active_processes(), 0);
        let error = scope
            .run_command(&command, &Policy::default(), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code(), "execution_failed");
    }

    #[tokio::test]
    async fn run_returns_successful_operation_value() {
        let scope = AgentRunScope::new(AgentRunPolicy::default());
        assert_eq!(scope.run(async { 42 }).await.unwrap(), 42);
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

        let scope = AgentRunScope::new(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let dropped = Arc::new(AtomicBool::new(false));
        let operation_dropped = dropped.clone();
        let cancel_scope = scope.clone();
        let task = tokio::spawn(async move {
            cancel_scope
                .run(async move {
                    let _drop_signal = DropSignal(operation_dropped);
                    std::future::pending::<()>().await;
                })
                .await
        });

        let _child = scope.start(&command, &Policy::default()).unwrap();
        tokio::task::yield_now().await;
        scope.request_cancel();

        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code(), "execution_failed");
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(scope.active_processes(), 0);
    }

    #[tokio::test]
    async fn cancellation_token_clone_cancels_the_shared_scope() {
        let scope = AgentRunScope::new(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::ZERO,
        });
        let token = scope.cancellation_token();
        let task_scope = scope.clone();
        let task = tokio::spawn(async move { task_scope.run(std::future::pending::<()>()).await });
        tokio::task::yield_now().await;

        token.cancel();

        assert_eq!(task.await.unwrap().unwrap_err().code(), "execution_failed");
    }

    #[tokio::test]
    async fn cancel_now_stops_process_without_waiting_for_grace_period() {
        let scope = AgentRunScope::new(AgentRunPolicy {
            timeout: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(30),
        });
        let command = Command {
            argv: vec!["/bin/sleep".into(), "30".into()],
            cwd: None,
        };
        let _child = scope.start(&command, &Policy::default()).unwrap();
        assert_eq!(scope.active_processes(), 1);

        scope.cancel_now();
        tokio::time::timeout(Duration::from_secs(2), async {
            while scope.active_processes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancel_now should terminate registered processes immediately");
    }
}
