use crate::EnvironmentPolicy;
use crate::FilesystemPolicy;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Enforcement {
    Enforced,
    Degraded,
    Unavailable,
    Trusted,
}

/// Runtime capabilities of the host backend. Callers should inspect this
/// before requesting optional features; `Enforcement` alone does not describe
/// PTY, adoption, or parent-death behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct PlatformCapabilities {
    pub containment: Enforcement,
    /// Whether `NetworkMode::Host` is supported by the native process
    /// launcher. This is independent of filesystem containment: Windows
    /// AppContainers require explicit network capability setup.
    pub host_network: bool,
    /// Whether the native backend can deny IP networking for a child.
    pub network_isolation: bool,
    /// Whether network isolation requires an explicit filesystem policy.
    /// Windows uses an AppContainer for both restrictions.
    pub network_isolation_requires_filesystem: bool,
    pub graceful_shutdown: bool,
    pub process_group_cleanup: bool,
    pub parent_death_cleanup: bool,
    pub shared_process_groups: bool,
    pub external_adoption: bool,
    pub executable_identity_on_adoption: bool,
    pub pipelines: bool,
    pub readiness_probes: bool,
    pub pty: bool,
    pub memory_limits: bool,
    pub process_limits: bool,
    pub cpu_limits: bool,
    /// Whether a requested filesystem policy is enforced by the native backend.
    /// macOS uses the legacy Seatbelt launcher rather than signed App Sandbox
    /// entitlements, but still applies the requested profile to the child.
    pub filesystem_isolation: bool,
}

pub const fn platform_capabilities() -> PlatformCapabilities {
    PlatformCapabilities {
        containment: if cfg!(windows) || cfg!(target_os = "linux") {
            Enforcement::Enforced
        } else if cfg!(unix) {
            Enforcement::Degraded
        } else {
            Enforcement::Unavailable
        },
        host_network: cfg!(unix) || cfg!(windows),
        network_isolation: cfg!(target_os = "linux") || cfg!(target_os = "macos") || cfg!(windows),
        network_isolation_requires_filesystem: cfg!(windows),
        graceful_shutdown: cfg!(unix),
        process_group_cleanup: cfg!(unix) || cfg!(windows),
        parent_death_cleanup: cfg!(target_os = "linux") || cfg!(windows),
        shared_process_groups: cfg!(unix) || cfg!(windows),
        external_adoption: cfg!(unix) || cfg!(windows),
        executable_identity_on_adoption: cfg!(target_os = "linux")
            || cfg!(target_os = "macos")
            || cfg!(windows),
        pipelines: true,
        readiness_probes: true,
        pty: cfg!(unix) || cfg!(windows),
        memory_limits: cfg!(target_os = "linux") || cfg!(windows),
        process_limits: cfg!(target_os = "linux") || cfg!(windows),
        cpu_limits: cfg!(target_os = "linux") || cfg!(windows),
        filesystem_isolation: cfg!(target_os = "linux")
            || cfg!(target_os = "macos")
            || cfg!(windows),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementRequirement {
    BestEffort,
    Required,
}

/// Network connectivity mode for a child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    /// Share the host network. This is the compatibility default.
    #[default]
    Host,
    /// Deny IP network access while retaining local Unix IPC where supported.
    Disabled,
}

pub(crate) fn validate_network_mode(mode: NetworkMode) -> io::Result<()> {
    let _ = mode;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    pub timeout_ms: u64,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub stderr_bytes: usize,
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub max_processes: Option<u32>,
    /// CPU quota in microseconds per 100ms period. The native Linux cgroup
    /// and Windows Job Object backends both use this period.
    #[serde(default)]
    pub cpu_quota_micros: Option<u64>,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            timeout_ms: Duration::from_secs(5).as_millis() as u64,
            input_bytes: 64 * 1024,
            output_bytes: 64 * 1024,
            stderr_bytes: 16 * 1024,
            memory_bytes: None,
            max_processes: None,
            cpu_quota_micros: None,
        }
    }
}

/// Complete authority passed to a child-process backend.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxPolicy {
    pub environment: EnvironmentPolicy,
    pub filesystem: Option<FilesystemPolicy>,
    pub network: NetworkMode,
    pub limits: ResourceLimits,
    pub enforcement: EnforcementRequirement,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            environment: EnvironmentPolicy::default(),
            filesystem: None,
            network: NetworkMode::Host,
            limits: ResourceLimits::default(),
            enforcement: EnforcementRequirement::BestEffort,
        }
    }
}

impl ResourceLimits {
    pub fn has_kernel_limits(&self) -> bool {
        self.memory_bytes.is_some()
            || self.max_processes.is_some()
            || self.cpu_quota_micros.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub working_directory: Option<PathBuf>,
    pub policy: SandboxPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExecutableIdentity {
    pub requested_path: PathBuf,
    /// Resolved canonical path when the backend can verify it. Remote
    /// execution backends may not expose the path resolved inside the sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum TerminationReason {
    Exited {
        code: Option<i32>,
        signal: Option<i32>,
    },
    TimedOut,
    Cancelled,
    StdoutLimitExceeded,
    StderrLimitExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum ProcessSignal {
    Hangup,
    Interrupt,
    Terminate,
    Kill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct ShutdownReport {
    pub graceful_requested: bool,
    pub graceful_completed: bool,
    pub forced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum RestartPolicy {
    Never,
    OnCrash,
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct SupervisionConfig {
    pub policy: RestartPolicy,
    pub max_restarts: u32,
    pub backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub shutdown_grace_ms: u64,
}

impl Default for SupervisionConfig {
    fn default() -> Self {
        Self {
            policy: RestartPolicy::Never,
            max_restarts: 0,
            backoff_ms: 100,
            max_backoff_ms: 30_000,
            shutdown_grace_ms: 500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionOutcome {
    pub restarts: u32,
    pub last_receipt: ProcessReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProcessReceipt {
    pub executable: ExecutableIdentity,
    pub enforcement: Enforcement,
    pub started_at: SystemTime,
    pub finished_at: SystemTime,
    pub termination: TerminationReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProcessInfo {
    pub process_id: u32,
    pub alive: bool,
    pub parent_process_id: Option<u32>,
    pub start_time_ticks: Option<u64>,
    pub executable: Option<PathBuf>,
}

/// A point-in-time view of the processes owned by a sandbox group.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProcessMember {
    pub process_id: u32,
    pub parent_process_id: Option<u32>,
    pub start_time_ticks: Option<u64>,
    pub executable: Option<PathBuf>,
    pub alive: bool,
}

/// Resource counters are optional because operating systems expose different
/// levels of whole-tree accounting. `None` means the backend cannot provide
/// that counter, not that its value is zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct ResourceStats {
    pub active_processes: Option<u64>,
    pub memory_current_bytes: Option<u64>,
    pub memory_peak_bytes: Option<u64>,
    pub cpu_usage_micros: Option<u64>,
}

/// A bounded time series of resource observations for one process group.
/// `elapsed_ms` is measured from the beginning of the sampling operation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ResourceStatsSample {
    pub elapsed_ms: u64,
    pub stats: ResourceStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ResourceStatsSeries {
    pub samples: Vec<ResourceStatsSample>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ProcessGroupSnapshot {
    pub enforcement: Enforcement,
    pub members: Vec<ProcessMember>,
    pub stats: ResourceStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum LifecycleEvent {
    Started {
        process_id: u32,
        executable: ExecutableIdentity,
    },
    Exited {
        process_id: u32,
        termination: TerminationReason,
    },
    Restarting {
        attempt: u32,
        delay_ms: u64,
        reason: TerminationReason,
    },
    ShutdownRequested,
}
