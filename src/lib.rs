//! Policy-driven process isolation and supervision.
//!
//! This crate deliberately has no knowledge of graphs, hooks, MCP, models, or
//! authentication. Callers resolve authority into an [`ExecutionRequest`]; the
//! selected backend validates and enforces that request without expanding it.

mod environment;
mod error;
mod events;
mod filesystem;
mod group;
mod network;
mod pipeline;
mod policy;
mod probes;
mod process_control;
mod pty;
mod runner;
mod supervisor;
#[cfg(windows)]
mod windows_filesystem;

pub use environment::{apply_environment_policy, validate_environment_policy, EnvironmentPolicy};
pub use error::{SandboxError, SandboxErrorCode, SandboxPhase};
pub use events::{ProcessEvent, ProcessEventStream};
pub use filesystem::{FilesystemAccess, FilesystemGrant, FilesystemPolicy};
pub use group::{AdoptedProcess, ProcessGroup};
pub use pipeline::{Pipeline, PipelineError, PipelineFailureKind};
pub use policy::{
    platform_capabilities, Enforcement, EnforcementRequirement, ExecutableIdentity,
    ExecutionRequest, LifecycleEvent, NetworkMode, PlatformCapabilities, ProcessGroupSnapshot,
    ProcessInfo, ProcessMember, ProcessReceipt, ProcessSignal, ResourceLimits, ResourceStats,
    ResourceStatsSample, ResourceStatsSeries, RestartPolicy, SandboxPolicy, ShutdownReport,
    SupervisionConfig, SupervisionOutcome, TerminationReason,
};
pub use probes::{wait_for_http, wait_for_port, wait_for_tcp, ProbeError};
pub use pty::{PtyDimensions, PtySession};
pub use runner::{execute_with_runner, NativeRunner, ProcessRunner};
pub use supervisor::{
    execute, execute_with_cancellation, spawn, spawn_with_supervisor, wait_all, wait_any,
    ExecutionOutput, SupervisedChild, Supervisor,
};
