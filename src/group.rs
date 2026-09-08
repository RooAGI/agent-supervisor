use crate::process_control::ProcessContainer;
use crate::supervisor::{spawn_group_child, SupervisedChild};
use crate::{
    Enforcement, ExecutionRequest, ProcessGroupSnapshot, ProcessMember, ProcessSignal,
    ResourceStats, ResourceStatsSample, ResourceStatsSeries, SandboxError, ShutdownReport,
};
#[cfg(any(unix, windows))]
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A process adopted into a [`ProcessGroup`]. Adoption is deliberately
/// identity-checked; Unix requires a process-group leader while Windows uses
/// Job Object attachment.
pub struct AdoptedProcess {
    process_id: u32,
    container: Arc<ProcessContainer>,
    enforcement: Enforcement,
}

impl AdoptedProcess {
    pub fn id(&self) -> u32 {
        self.process_id
    }
    pub fn enforcement(&self) -> Enforcement {
        self.enforcement
    }
    pub fn is_alive(&self) -> bool {
        self.container.is_alive()
    }

    /// Returns a snapshot of the members currently visible to the backend.
    /// Backends with only process-group tracking may return the group leader.
    pub fn members(&self) -> Result<Vec<ProcessMember>, SandboxError> {
        self.container.members().map_err(SandboxError::execution)
    }

    pub fn stats(&self) -> Result<ResourceStats, SandboxError> {
        self.container.stats().map_err(SandboxError::execution)
    }

    pub fn snapshot(&self) -> Result<ProcessGroupSnapshot, SandboxError> {
        Ok(ProcessGroupSnapshot {
            enforcement: self.enforcement,
            members: self.members()?,
            stats: self.stats()?,
        })
    }
    pub fn terminate(&self) -> Result<(), SandboxError> {
        self.container.terminate().map_err(SandboxError::execution)
    }

    /// Collect a finite resource series. Sampling stops early when cancelled
    /// or when the group exits; it never creates an unbounded monitor task.
    pub async fn sample_stats(
        &self,
        interval: Duration,
        max_samples: usize,
        cancellation: &CancellationToken,
    ) -> Result<ResourceStatsSeries, SandboxError> {
        if max_samples == 0 {
            return Ok(ResourceStatsSeries {
                samples: Vec::new(),
            });
        }
        let started = tokio::time::Instant::now();
        let mut samples = Vec::with_capacity(max_samples);
        for index in 0..max_samples {
            if cancellation.is_cancelled() || (index > 0 && !self.is_alive()) {
                break;
            }
            samples.push(ResourceStatsSample {
                elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                stats: self.stats()?,
            });
            if index + 1 < max_samples {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        }
        Ok(ResourceStatsSeries { samples })
    }
}

/// A reusable kernel-backed process container shared by multiple children.
/// Child handles do not own the group; dropping this value terminates all
/// remaining members.
pub struct ProcessGroup {
    container: Arc<ProcessContainer>,
    group_id: Mutex<Option<u32>>,
    start_lock: Mutex<()>,
    enforcement: Enforcement,
}

impl ProcessGroup {
    pub fn new() -> Result<Self, SandboxError> {
        let container = Arc::new(ProcessContainer::new().map_err(SandboxError::spawn)?);
        let enforcement = container.enforcement();
        Ok(Self {
            container,
            group_id: Mutex::new(None),
            start_lock: Mutex::new(()),
            enforcement,
        })
    }

    pub fn enforcement(&self) -> Enforcement {
        self.enforcement
    }

    /// Returns whether at least one member is still alive.
    pub fn is_alive(&self) -> bool {
        self.container.is_alive()
    }

    /// Returns a point-in-time view of every process currently owned by this
    /// group, including identity fields where the host permits them.
    pub fn members(&self) -> Result<Vec<ProcessMember>, SandboxError> {
        self.container.members().map_err(SandboxError::execution)
    }

    pub fn stats(&self) -> Result<ResourceStats, SandboxError> {
        self.container.stats().map_err(SandboxError::execution)
    }

    pub fn snapshot(&self) -> Result<ProcessGroupSnapshot, SandboxError> {
        Ok(ProcessGroupSnapshot {
            enforcement: self.enforcement,
            members: self.members()?,
            stats: self.stats()?,
        })
    }

    /// Collect a finite resource series. Sampling stops early when cancelled
    /// or when the group exits; it never creates an unbounded monitor task.
    pub async fn sample_stats(
        &self,
        interval: Duration,
        max_samples: usize,
        cancellation: &CancellationToken,
    ) -> Result<ResourceStatsSeries, SandboxError> {
        if max_samples == 0 {
            return Ok(ResourceStatsSeries {
                samples: Vec::new(),
            });
        }
        let started = tokio::time::Instant::now();
        let mut samples = Vec::with_capacity(max_samples);
        for index in 0..max_samples {
            if cancellation.is_cancelled() || (index > 0 && !self.is_alive()) {
                break;
            }
            samples.push(ResourceStatsSample {
                elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                stats: self.stats()?,
            });
            if index + 1 < max_samples {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        }
        Ok(ResourceStatsSeries { samples })
    }

    pub fn start(&self, request: &ExecutionRequest) -> Result<SupervisedChild, SandboxError> {
        let _start_guard = self
            .start_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let group_id = *self
            .group_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if group_id.is_some() && !self.container.is_alive() {
            return Err(SandboxError::spawn(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "cannot add a child to a group whose leader is no longer alive",
            )));
        }
        let child = spawn_group_child(request, Arc::clone(&self.container), group_id)?;
        if group_id.is_none() {
            let mut current = self
                .group_id
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if current.is_none() {
                *current = child.id();
            }
        }
        Ok(child)
    }

    /// Adopt an already-running, independently-created process group.
    ///
    /// On Linux the process must lead its own process group. On Windows it is
    /// attached to a Job Object. When supplied, `expected_executable` is
    /// checked before attachment to reduce PID-reuse risk.
    #[cfg(any(unix, windows))]
    pub fn adopt(
        &self,
        process_id: u32,
        expected_executable: Option<&Path>,
    ) -> Result<AdoptedProcess, SandboxError> {
        let _start_guard = self
            .start_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut group_id = self
            .group_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if group_id.is_some() {
            return Err(SandboxError::spawn(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "process group already has a leader",
            )));
        }
        self.container
            .adopt_process(process_id, expected_executable)
            .map_err(SandboxError::spawn)?;
        *group_id = Some(process_id);
        Ok(AdoptedProcess {
            process_id,
            container: Arc::clone(&self.container),
            enforcement: self.enforcement,
        })
    }

    pub fn terminate(&self) {
        let _ = self.container.terminate();
    }

    pub fn signal(&self, signal: ProcessSignal) -> Result<(), SandboxError> {
        self.container
            .signal(signal)
            .map_err(SandboxError::execution)
    }

    pub fn suspend(&self) -> Result<(), SandboxError> {
        self.container.suspend().map_err(SandboxError::execution)
    }

    pub fn resume(&self) -> Result<(), SandboxError> {
        self.container.resume().map_err(SandboxError::execution)
    }

    /// Request graceful termination, wait for the group, then force cleanup.
    pub async fn shutdown(&self, grace: Duration) -> Result<ShutdownReport, SandboxError> {
        if !self.is_alive() {
            return Ok(ShutdownReport {
                graceful_requested: false,
                graceful_completed: true,
                forced: false,
            });
        }
        self.container
            .request_graceful_stop()
            .map_err(SandboxError::execution)?;
        let graceful_requested = true;
        let graceful_completed = tokio::time::timeout(grace, async {
            while self.is_alive() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        if !graceful_completed {
            self.container
                .terminate()
                .map_err(SandboxError::execution)?;
        }
        Ok(ShutdownReport {
            graceful_requested,
            graceful_completed,
            forced: !graceful_completed,
        })
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = self.container.terminate();
    }
}
