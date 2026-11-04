//! Policy-driven process isolation and supervision.
//!
//! The crate provides process supervision and policy enforcement. Its MCP
//! helper manages local stdio server processes and applies configured MCP
//! tool-call controls.

use std::path::PathBuf;

mod agent_run;
mod environment;
mod error;
mod events;
mod filesystem;
mod group;
pub mod mcp;
mod network;
mod pipeline;
mod policy;
mod probes;
mod process_control;
mod pty;
mod runner;
mod supervisor;
#[cfg(feature = "wasm")]
mod wasm;
#[cfg(windows)]
mod windows_filesystem;

pub use agent_run::{AgentRunPolicy, AgentRunScope};
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
    execute, execute_with_cancellation, run, spawn, spawn_with_supervisor, wait_all, wait_any,
    ExecutionOutput, SupervisedChild, Supervisor,
};

/// High-level command description shared with the Python API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// Executable followed by its arguments, passed directly without a shell.
    pub argv: Vec<String>,
    /// Optional child working directory.
    pub cwd: Option<PathBuf>,
}

impl Command {
    pub(crate) fn request(&self, policy: &Policy) -> ExecutionRequest {
        let mut argv = self.argv.iter();
        let executable = argv.next().map(PathBuf::from).unwrap_or_default();
        ExecutionRequest {
            executable,
            args: argv.cloned().collect(),
            working_directory: self.cwd.clone(),
            policy: policy.clone(),
        }
    }
}

/// Process policy shared by the Rust and Python high-level APIs.
pub type Policy = SandboxPolicy;
#[cfg(feature = "wasm")]
pub use wasm::{WasmRunner, WasmRunnerConfig};
