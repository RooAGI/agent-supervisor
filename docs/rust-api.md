# Rust API

This page covers the Rust crate's public API for supervising agent runs and
their local tool processes. Your agent framework still runs the agent and
makes model requests; `AgentRunScope` adds a deadline and cleanup boundary,
while `run()` and `Supervisor` manage the commands your tool functions start.
For the shared concepts and Rust/Python terminology, see the [Core
Concept](api-reference.md).

## Install

Add the crate to your project:

```bash
cargo add agent-supervisor
```

The API uses Tokio for asynchronous process I/O. Ensure the Tokio runtime
features required by your application are enabled (`process`, `io-util`,
`time`, and a runtime such as `rt-multi-thread`).

## Supervise an agent run

`AgentRunScope` wraps one async call to an agent framework. Create a scope for
that call, pass a clone to the tool callbacks, and start local tool commands
through that clone. This applies one deadline to the framework future and
registers tool processes for cleanup. The Rust scope is explicit: it does not
automatically attach itself to unrelated tasks or subprocesses.

### `AgentRunPolicy`

```rust
pub struct AgentRunPolicy {
    pub timeout: Duration,
    pub shutdown_grace: Duration,
}
```

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `timeout` | `Duration` | 300 seconds | Maximum wall time measured from scope creation. |
| `shutdown_grace` | `Duration` | 5 seconds | Time to let registered processes exit during cleanup before terminating them. |

```rust
use agent_supervisor::AgentRunPolicy;
use std::time::Duration;

let policy = AgentRunPolicy {
    timeout: Duration::from_secs(180),
    shutdown_grace: Duration::from_secs(3),
};
```

### `AgentRunScope`

```rust
pub struct AgentRunScope { /* shared, cloneable run owner */ }
```

| Method | Signature | Returns and behavior |
| --- | --- | --- |
| `new` | `AgentRunScope::new(policy: AgentRunPolicy) -> AgentRunScope` | Creates the shared scope and starts its deadline immediately. |
| `run` | `async fn run<T, F>(&self, operation: F) -> Result<T, SandboxError>` where `F: Future<Output = T>` | Returns the future's value on success. On deadline or requested cancellation, drops the future, cleans registered processes, and returns `timed_out` or `execution_failed`. |
| `run_command` | `async fn run_command(&self, command: &Command, policy: &Policy, input: &[u8]) -> Result<ExecutionOutput, SandboxError>` | Runs one tool command, bounded by its `Policy` and the remaining agent deadline. |
| `start` | `fn start(&self, command: &Command, policy: &Policy) -> Result<SupervisedChild, SandboxError>` | Starts a persistent tool process registered for scope cleanup. |
| `cancellation_token` | `fn cancellation_token(&self) -> CancellationToken` | Returns a clone for framework or app APIs that accept a Tokio cancellation token. |
| `request_cancel` | `fn request_cancel(&self)` | Signals the token. Call `cancel().await` to wait for registered process cleanup. |
| `cancel` | `async fn cancel(&self)` | Signals cancellation, waits for the configured grace period, then terminates remaining registered process groups. Call this after normal completion, too. |
| `cancel_now` | `fn cancel_now(&self)` | Signals cancellation and immediately requests termination of registered process groups; it does not wait for cleanup. |
| `active_processes` | `fn active_processes(&self) -> usize` | Returns the number of persistent child processes currently registered. |
| `supervisor` | `fn supervisor(&self) -> Arc<Supervisor>` | Returns the scope's shared process supervisor. Use `run_command()` when a one-shot tool must also obey the remaining run deadline. |

If the operation passed to `scope.run()` returns a `Result`, the scope result is
nested: the outer `Result` reports run timeout or cancellation, and the inner
one reports the operation's result. This lets the caller distinguish an SDK run
ending from a tool execution error.

```rust
use agent_supervisor::{AgentRunPolicy, AgentRunScope, Command, Policy};
use std::time::Duration;

let run_scope = AgentRunScope::new(AgentRunPolicy::default());
let tool_scope = run_scope.clone();

// Register a callback like this with the framework's agent. The callback
// receives the clone and starts its local command through the run scope.
let operation = async move {
    let command = Command {
        argv: vec!["/usr/bin/printf".into(), "tests passed\n".into()],
        cwd: None,
    };
    let mut tool_policy = Policy::default();
    tool_policy.limits.timeout_ms = 120_000;
    tool_scope.run_command(&command, &tool_policy, b"").await
};

// Replace this future with the framework's async agent invocation.
let result = run_scope.run(operation).await;
// Close persistent tools when the framework returns, including on success.
run_scope.cancel().await;
let output = result??;
println!("{:?}: exit={:?}", output.termination, output.exit_code);
```

On timeout, Tokio drops the in-flight framework future. A framework that accepts
a cancellation token can also observe `scope.cancellation_token()`. These
controls apply to the Rust future and processes started through the scope; they
do not sandbox model requests or terminate the application process.

For an interactive tool process, use `start()` and keep the returned
`SupervisedChild` for its stdin, output, and shutdown lifecycle:

```rust
let child = tool_scope.start(&command, &Policy::default())?;
let mut process = child.into_event_stream()?;
process.write(b"task\n").await?;
process.close_stdin().await?;
let receipt = process.wait().await?;
println!("{:?}", receipt.termination);
```

When the agent scope is cancelled or `cancel().await` is called, the scope
cleans up persistent children started through it.

## `Command`

`Command` describes the executable, its arguments, and an optional working
directory. The argument vector is passed directly; a shell is not inserted.

```rust
pub struct Command {
    pub argv: Vec<String>,
    pub cwd: Option<PathBuf>,
}
```

The executable and any working directory must be absolute paths. A `Command`
does not include its policy: pass a separate `Policy` to `run()` or
`Supervisor::start()`.

```rust
use agent_supervisor::Command;

let command = Command {
    argv: vec!["/usr/bin/echo".into(), "hello".into()],
    cwd: None,
};
```

## `Policy`

`Policy` is an alias for `SandboxPolicy`. `Policy::default()` uses a cleared
child environment, no filesystem restriction, host networking, best-effort
enforcement, a five-second timeout, a 64 KiB stdin limit, a 64 KiB stdout
limit, and a 16 KiB stderr limit. Kernel memory, process-count, and CPU limits
are unset.

| Field | Type | Purpose |
| --- | --- | --- |
| `environment` | `EnvironmentPolicy` | Allowlist inherited variables, add explicit variables, and optionally set executable search paths. |
| `filesystem` | `Option<FilesystemPolicy>` | `None` requests no filesystem restriction; `Some(policy)` sets directory grants. |
| `network` | `NetworkMode` | `Host` or `Disabled`. |
| `limits` | `ResourceLimits` | Runtime and byte limits, with optional kernel resource limits. |
| `enforcement` | `EnforcementRequirement` | `BestEffort` or `Required`. |

Filesystem grants use `FilesystemGrant { root, access }` with
`FilesystemAccess::{Read, Write, Append}`. An explicit empty policy denies all
filesystem access. Append must be paired with write. Network and filesystem
enforcement vary by operating system; inspect `platform_capabilities()` and
select `Required` when the command must not start without the requested native
controls.

```rust
use agent_supervisor::{
    EnforcementRequirement, FilesystemAccess, FilesystemGrant, FilesystemPolicy,
    NetworkMode, Policy,
};

let policy = Policy {
    filesystem: Some(FilesystemPolicy::new(vec![FilesystemGrant {
        root: "/workspace/project".into(),
        access: vec![FilesystemAccess::Read, FilesystemAccess::Write],
    }])),
    network: NetworkMode::Disabled,
    enforcement: EnforcementRequirement::Required,
    ..Policy::default()
};
```

`EnvironmentPolicy` has three fields: `inherit` is a set of parent variable
names to copy, `variables` is a map of explicit child values, and
`executable_search_paths` is a list of absolute directories used to set `PATH`.
All other ambient environment variables are cleared.

`ResourceLimits` fields are `timeout_ms`, `input_bytes`, `output_bytes`,
`stderr_bytes`, `memory_bytes`, `max_processes`, and `cpu_quota_micros`. Its
default is 5,000 ms, 65,536 stdin bytes, 65,536 stdout bytes, 16,384 stderr
bytes, and no optional kernel limits. The timeout applies to one-shot
execution; a persistent process is stopped explicitly through `shutdown()`.

## One-shot execution

`run()` executes with empty stdin. `execute()` takes stdin bytes and uses the
lower-level `ExecutionRequest` type, which combines an executable path,
arguments, working directory, and policy.

```rust
pub async fn run(
    command: &Command,
    policy: &Policy,
) -> Result<ExecutionOutput, SandboxError>

pub async fn execute(
    request: &ExecutionRequest,
    input: &[u8],
) -> Result<ExecutionOutput, SandboxError>
```

```rust
use agent_supervisor::{run, Command, Policy};

let output = run(&command, &Policy::default()).await?;
println!("{}", String::from_utf8_lossy(&output.stdout));
println!("success: {}", output.success);
```

`ExecutionOutput` contains `success`, `stdout` and `stderr` as bytes,
`enforcement`, the requested and canonical executable identity, and
`termination`. Nonzero exits, timeouts, cancellation, and output-limit
termination are represented in the output. Invalid requests, required
controls that cannot be applied, and launch or I/O failures return
`SandboxError`.

For cancellable one-shot execution, pass a Tokio cancellation token:

```rust
pub async fn execute_with_cancellation(
    request: &ExecutionRequest,
    input: &[u8],
    cancellation: CancellationToken,
) -> Result<ExecutionOutput, SandboxError>
```

```rust
use agent_supervisor::{execute_with_cancellation, ExecutionRequest, Policy};
use tokio_util::sync::CancellationToken;

let request = ExecutionRequest {
    executable: "/bin/sleep".into(),
    args: vec!["30".into()],
    working_directory: None,
    policy: Policy::default(),
};
let cancellation = CancellationToken::new();
let cancel_later = cancellation.clone();
tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    cancel_later.cancel();
});
let output = execute_with_cancellation(&request, b"", cancellation).await?;
```

Cancellation is reported as `TerminationReason::Cancelled` in the output.

## Persistent processes

Use `Supervisor::start()` when the child needs to remain interactive. The
supervisor tracks the child so it can clean up its process group. Convert the
child into a `ProcessEventStream` to write stdin and receive output events.

```rust
let supervisor = Supervisor::new();
let child = supervisor.start(&command, &policy)?;
let mut process = child.into_event_stream()?;
```

| Method | Arguments | Return |
| --- | --- | --- |
| `Supervisor::new()` | none | `Supervisor` |
| `Supervisor::start(&command, &policy)` | `&Command`, `&Policy` | `Result<SupervisedChild, SandboxError>` |
| `SupervisedChild::into_event_stream()` | consumes child | `Result<ProcessEventStream, SandboxError>` |
| `ProcessEventStream::write(bytes)` | `&[u8]` | `Result<(), SandboxError>` |
| `ProcessEventStream::close_stdin()` | none | `Result<(), SandboxError>` |
| `ProcessEventStream::next()` | none | `Result<Option<ProcessEvent>, SandboxError>` |
| `ProcessEventStream::wait()` | none | `Result<ProcessReceipt, SandboxError>` |
| `ProcessEventStream::shutdown(grace)` | `Duration` | `Result<ProcessReceipt, SandboxError>` |

Write and close stdin, then read events through `Exited`. `Stdout` and
`Stderr` carry bytes; `Started` contains process identity. The process receipt
contains the executable identity, enforcement outcome, start and finish times,
and termination reason.

```rust
use agent_supervisor::{Command, Policy, ProcessEvent, Supervisor};

let supervisor = Supervisor::new();
let command = Command {
    argv: vec!["/bin/cat".into()],
    cwd: None,
};
let mut process = supervisor
    .start(&command, &Policy::default())?
    .into_event_stream()?;

process.write(b"hello\n").await?;
process.close_stdin().await?;

while let Some(event) = process.next().await? {
    match event {
        ProcessEvent::Stdout(bytes) => print!("{}", String::from_utf8_lossy(&bytes)),
        ProcessEvent::Stderr(bytes) => eprint!("{}", String::from_utf8_lossy(&bytes)),
        ProcessEvent::Exited(receipt) => println!("{:?}", receipt.termination),
        ProcessEvent::Started { .. } => {}
    }
}
```

If you do not need to read events manually, call `wait()` on the child to
collect its receipt. To stop a running child, call `shutdown(grace)`. It closes
stdin, gives the process the grace period to exit, then terminates the process
tree if it remains alive.

```rust
let child = supervisor.start(&command, &policy)?;
let receipt = child
    .shutdown(std::time::Duration::from_secs(2))
    .await?;
```

`SupervisedChild::wait_with_cancellation(token, grace)` can cancel a wait;
provide `tokio_util::sync::CancellationToken` and the shutdown grace period.

## Errors and platform support

`SandboxError` exposes `code()`, `phase()`, `message()`, and `retryable()`;
optional `executable()`, `termination()`, and `stderr_tail()` provide more
context. Branch on `code()` rather than parsing the message. Common codes
include `relative_executable`, `relative_working_directory`,
`sandbox_enforcement_unavailable`, `spawn_failed`, `timed_out`, and output or
input limit errors.

`platform_capabilities()` returns a `PlatformCapabilities` value describing
containment, network and filesystem enforcement, cleanup, PTY, and kernel
resource-limit support. Values differ by operating system. See the
[security model](security-model.md) for the meaning of each enforcement
outcome.

For the crate's Rust MCP stdio server helper and configuration, see the
[MCP server guide](mcp.md#adopt-with-the-official-mcp-sdk).
