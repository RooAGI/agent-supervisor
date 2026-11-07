# Rust API

`Supervisor` is the top-level owner of agent runs and local child processes.
Create each `AgentRun` through `Supervisor::start_agent_run()`; run-owned tools
are registered with the same supervisor and stop with their run. Use
`Supervisor::start_background()` for jobs that can continue between agent
calls. Both the agent framework and its model requests remain outside Agent
Supervisor's process control. For shared terms, see [Core concepts](api-reference.md).

For application startup and shutdown, see the [Supervisor guide](supervisor-lifecycle.md#application-setup).

## Install

Add the crate to your project:

```bash
cargo add agent-supervisor
```

The API uses Tokio for asynchronous process I/O. Ensure the Tokio runtime
features required by your application are enabled (`process`, `io-util`,
`time`, and a runtime such as `rt-multi-thread`).

## Supervisor

The supervisor owns agent runs, background jobs, and every process it tracks.

### Supervisor manager

`Supervisor` is the top-level owner of agent runs and local processes. Create
an `AgentRun` through `start_agent_run()` for each framework call. The
supervisor also starts background jobs that can continue between agent runs,
and it cleans up all its runs and processes when the application shuts down.

`Supervisor::start()` returns a `SupervisedChild`. Convert it with
`into_event_stream()` when the caller should read output while supervisor
watchers see the same chunks. Use `take_stdout()` or `take_stderr()` when an
integration needs direct ownership of raw pipes; those bytes cannot also be
observed by `Supervisor::stream()`. The supervisor tracks either form for
cleanup, and dropping the child stops the process.

### Manager-owned background jobs

Use `start_background()` when you want `Supervisor` to manage a job's input,
output, and completion. It returns a `SupervisorProcessId` for checking,
communicating with, or stopping that job later. This mode needs a running
Tokio runtime so the manager can handle the job while your code continues.

`start()` and `start_background()` register jobs with the same `Supervisor`,
so it can clean them up when the application shuts down. Convert a `start()`
child with `into_event_stream()` to read output while publishing the same
chunks to supervisor watchers, or take the raw pipes for direct ownership.
`start_background()` keeps output draining and reaping in the manager and
makes the newest output and final receipt available by ID. `shutdown_all()`
requests stops for both kinds and returns results for background jobs.

```rust
pub struct SupervisorProcessId { /* supervisor UUID + local sequence */ }

pub enum ManagedProcessStatus {
    Running,
    Completed,
    Failed,
}

pub struct ManagedProcessResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub receipt: ProcessReceipt,
    pub error: Option<String>,
}
```

| Method | Signature | Returns and behavior |
| --- | --- | --- |
| `start_agent_run` | `fn start_agent_run(&self, policy: AgentRunPolicy) -> AgentRun` | Creates and tracks one agent-framework invocation under this supervisor. |
| `active_agent_runs` | `fn active_agent_runs(&self) -> usize` | Returns the number of runs currently owned by this supervisor. |
| `start` | `fn start(&self, command: &Command, policy: &Policy) -> Result<SupervisedChild, SandboxError>` | Starts a child with raw pipes owned by the caller. |
| `start` | `fn start(&self, command: &Command, policy: &Policy) -> Result<SupervisedChild, SandboxError>` | Starts a child; convert it to an event stream for caller output plus supervisor watcher fanout, or take the raw pipes. |
| `stream` | `fn stream(&self, scope: StreamScope<'_>) -> EventStream` | Watches work without affecting it. See [Stream output](#stream-output). |
| `start_background` | `fn start_background(&self, command: &Command, policy: &Policy) -> Result<SupervisorProcessId, SandboxError>` | Starts a manager-owned job. Requires a live Tokio runtime. |
| `process_status` | `fn process_status(&self, id: SupervisorProcessId) -> Option<ManagedProcessStatus>` | `None` for an unknown or forgotten ID. |
| `process_result` | `fn process_result(&self, id: SupervisorProcessId) -> Option<ManagedProcessResult>` | Snapshot of a completed result; `None` while active or unknown. |
| `wait_process` | `async fn wait_process(&self, id: SupervisorProcessId) -> Result<Option<ManagedProcessResult>, SandboxError>` | Waits for output collection and process completion; `Ok(None)` means unknown or forgotten. |
| `take_process_result` | `fn take_process_result(&self, id: SupervisorProcessId) -> Option<ManagedProcessResult>` | Returns and removes a completed result. |
| `forget_process` | `fn forget_process(&self, id: SupervisorProcessId) -> bool` | Removes a completed or failed record; active jobs remain registered. |
| `write_process_stdin` | `async fn write_process_stdin(&self, id: SupervisorProcessId, bytes: &[u8]) -> Result<(), SandboxError>` | Writes input while output continues to drain. |
| `close_process_stdin` | `async fn close_process_stdin(&self, id: SupervisorProcessId) -> Result<(), SandboxError>` | Closes stdin and delivers EOF. |
| `shutdown_process` | `async fn shutdown_process(&self, id: SupervisorProcessId, grace: Duration) -> Result<ProcessStopResult, SandboxError>` | Stops one process tree without stopping siblings. Returns the stop request outcome, not an exit claim. |
| `shutdown_all` | `async fn shutdown_all(&self, grace: Duration) -> Result<Vec<(SupervisorProcessId, ManagedProcessResult)>, SandboxError>` | Cancels active agent runs, stops registered processes, then collects manager-owned job results. For interactive `start()` children, the caller still waits through the `SupervisedChild`. |

`SupervisorProcessId` includes a random supervisor identity and a local
sequence number. Its versioned token (`v1:<supervisor-uuid>:<sequence>`) can
be stored as a string and parsed with `str::parse`; Serde encodes it as that
same string. It is not an operating-system PID and does not restore a process
after the owning supervisor exits. `ProcessStopResult` is one of
`GracefulStopRequested`, `ForceStopRequested`, `AlreadyStopping`,
`AlreadyExited`, or `NotFound`. These describe the stop request or registry
state; use `wait_process()` to observe completion and retrieve the receipt.
Use `take_process_result()` to return and remove a completed result, or
`forget_process()` to remove a completed or monitor-failed record, so a
long-lived supervisor does not retain every result indefinitely.

The `Policy` passed to `start_background()` still enforces the configured
stdout and stderr byte limits. `ResourceLimits::timeout_ms` is for one-shot
execution; it does not automatically stop a persistent background job. Use
`shutdown_process()` or `shutdown_all()` to stop one. On Unix, shutdown sends a
graceful process-group signal and force-stops the group if it remains alive
after `grace`. The Windows backend has no graceful signal, so it force-stops
immediately and does not use the grace period.

Dropping `Supervisor` synchronously requests termination for its registered
process trees. Background monitors can finish draining/reaping while the Tokio
runtime remains alive. Prefer `shutdown_all().await` when you need final output
and receipts before the runtime exits; `Drop` is a cleanup fallback.

```rust
use agent_supervisor::{Command, ManagedProcessStatus, Policy, Supervisor};
use std::time::Duration;

let supervisor = Supervisor::new();
let report = Command {
    argv: vec!["/usr/bin/printf".into(), "job finished\\n".into()],
    cwd: None,
};
let policy = Policy::default();
let report_id = supervisor.start_background(&report, &policy)?;
let long_job = Command {
    argv: vec!["/bin/sleep".into(), "30".into()],
    cwd: None,
};
let stop_id = supervisor.start_background(&long_job, &policy)?;
let sibling_id = supervisor.start_background(&long_job, &policy)?;

let report_result = supervisor.wait_process(report_id).await?.unwrap();
println!("{}", String::from_utf8_lossy(&report_result.stdout));

let stop = supervisor
    .shutdown_process(stop_id, Duration::from_secs(1))
    .await?;
println!("stop request: {stop:?}");
let stopped_result = supervisor.wait_process(stop_id).await?.unwrap();
println!("stopped: {:?}", stopped_result.receipt.termination);
assert_eq!(supervisor.process_status(sibling_id), Some(ManagedProcessStatus::Running));

// Await cleanup and collect the remaining sibling's result before the Tokio
// runtime shuts down. On Windows, this requests immediate force-stop.
let cleaned = supervisor.shutdown_all(Duration::from_secs(1)).await?;
for (id, result) in cleaned {
    println!("{id:?}: {:?}", result.receipt.termination);
}
```

The code above uses `start_background()` inside an active Tokio runtime. The
complete, portable executable example is
[`examples/rust/supervisor-background`](https://github.com/RooAGI/agent-supervisor/blob/main/examples/rust/supervisor-background/README.md).

#### Input and output

Choose the process mode by how you need to talk to the process:

| Mode | Send input | Receive output | Live streaming |
| --- | --- | --- | --- |
| `run` / `execute` | One byte slice, then stdin closes. | Bounded `stdout` and `stderr` in `ExecutionOutput` when the command ends. | `AgentRun::run_command()` only, with `StreamScope::Run` |
| `Supervisor::start()` then `into_event_stream()` | `write(bytes)` and `close_stdin()` at any time. | `ProcessEvent::Stdout` / `Stderr` from `next()`; also published to `stream()`. | Yes |
| `Supervisor::start_background()` | `write_process_stdin(id, bytes)` and `close_process_stdin(id)`. | The newest `stdout` and `stderr`, returned in `ManagedProcessResult` by `wait_process(id)` or `process_result(id)` after the job finishes. | With `StreamScope::Process(id)` |

One-shot output and output read through `ProcessEventStream` are bounded by the
command's `ResourceLimits`; exceeding a limit stops that process. If you take
the raw pipes from `SupervisedChild`, your code owns draining and bounding
them. A background job keeps running and retains its newest output, and
`ManagedProcessResult::output_truncated` is `true` if older bytes were
discarded. To watch any of them live, use
[`Supervisor::stream()`](#stream-output).

**Stream output and send input to a supervised process:**

```rust
let mut process = supervisor
    .start(&command, &Policy::default())?
    .into_event_stream()?;
process.write(b"start\n").await?;
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

**Send input to a background job and read its output:**

```rust
let id = supervisor.start_background(&command, &Policy::default())?;
supervisor.write_process_stdin(id, b"start\n").await?;
supervisor.close_process_stdin(id).await?;

if let Some(result) = supervisor.wait_process(id).await? {
    println!("{}", String::from_utf8_lossy(&result.stdout));
}
```

#### Save and resume background jobs

Your framework or application stores its own checkpoint. To find an
application-owned background job after a framework invocation resumes, store
the serializable `SupervisorProcessId` beside that checkpoint. This works only
while the same application `Supervisor` remains alive. It does not resume a
run-owned tool process after that `AgentRun` has ended, restore an OS process
after an application restart, or replay the command automatically.

```rust
use agent_supervisor::{
    AgentRunPolicy, Command, ManagedProcessStatus, Policy, Supervisor,
    SupervisorProcessId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

#[derive(Serialize, Deserialize)]
struct SavedWork {
    framework_checkpoint: Value,
    background_process: SupervisorProcessId,
}

async fn start_job_and_save(
    supervisor: &Supervisor,
    framework_checkpoint: Value,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let command = Command {
        argv: vec!["/usr/bin/sleep".into(), "60".into()],
        cwd: None,
    };
    let background_process =
        supervisor.start_background(&command, &Policy::default())?;

    // Store this serialized value with the framework's native checkpoint.
    let saved = SavedWork {
        framework_checkpoint,
        background_process,
    };
    Ok(serde_json::to_string(&saved)?)
}

async fn resume(
    supervisor: &Supervisor,
    saved_record: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let saved: SavedWork = serde_json::from_str(saved_record)?;
    let run = supervisor.start_agent_run(AgentRunPolicy::default());

    match supervisor.process_status(saved.background_process) {
        Some(status) => {
            let framework_call =
                resume_framework(&saved.framework_checkpoint, status);
            let result = run.run(framework_call).await;
            run.cancel().await;
            result??;
        }
        None => {
            // The manager no longer has this job. Decide how to recover from
            // the framework checkpoint; do not start the old command again.
            eprintln!("background job is unavailable; not replaying it");
            run.cancel().await;
        }
    }
    Ok(())
}

async fn resume_framework(
    _checkpoint: &Value,
    _job_status: ManagedProcessStatus,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Replace with the framework's native resume/invoke call.
    Ok(())
}
```

Persist the returned string in your app's checkpoint store. `SupervisorProcessId`
Serde encodes the full supervisor-scoped token; do not save only its local
sequence or an OS PID. The example returns `None` from `process_status()` when
the ID is unknown, forgotten, or belongs to a different `Supervisor`.

### Stream output

```rust
pub fn stream(&self, scope: StreamScope<'_>) -> EventStream
```

`Supervisor::stream()` watches work as one ordered stream of events. It only
observes: launch work with `Supervisor::start()`,
`Supervisor::start_background()`, or an `AgentRun`. Dropping a watcher stops
nothing, and a slow watcher never slows a process.

| Scope | Shows | Ends when |
| --- | --- | --- |
| `StreamScope::Process(id)` | One process or background job. | It exits. |
| `StreamScope::Run(&run)` | One agent run: published agent events and its tools. | The run ends. |
| `StreamScope::All` | Everything on this supervisor. | The supervisor is dropped. |

```rust
use agent_supervisor::{Policy, StreamScope, Supervisor, SupervisorEventKind};

let id = supervisor.start_background(&command, &Policy::default())?;
let mut events = supervisor.stream(StreamScope::Process(id));
while let Some(event) = events.next().await {
    match event.kind {
        SupervisorEventKind::Stdout(bytes) => print!("{}", String::from_utf8_lossy(&bytes)),
        SupervisorEventKind::ProcessExited(receipt) => println!("{receipt:?}"),
        _ => {}
    }
}
```

Each `SupervisorEvent` has `seq`, `run_id`, `process_id`, and `kind`:

| `SupervisorEventKind` | Meaning |
| --- | --- |
| `RunStarted`, `RunEnded` | An agent run began or finished. `RunEnded` needs `AgentRun::cancel()` after normal completion. |
| `ProcessStarted { executable }` | A process started. |
| `Stdout(bytes)`, `Stderr(bytes)` | Bytes the process wrote. |
| `ProcessExited(receipt)` | A process finished. `None` if its handle was dropped before the exit was observed. |
| `Agent(payload)` | An event from `AgentRun::publish_agent_event()`. |
| `Lagged { dropped }` | This watcher fell behind and lost entries. |

Events are in the order they were recorded. stdout and stderr are read
separately, so they are not ordered against each other. A run from another
supervisor, or an unknown process ID, gives an empty stream.

**Publishing agent events.** A custom integration can add its framework's events
to the run's stream. The payload is an `Arc<dyn Any + Send + Sync>`, carried
unchanged, and never reaches the model:

```rust
run.publish_agent_event(std::sync::Arc::new(framework_event));
```

**What is retained.** Process output in the log is limited to about the newest
8 MiB per supervisor. Opaque agent framework payloads have a separate
256-event retention limit because their sizes cannot be measured. A watcher
starts at the oldest retained event, so a late watcher sees only what remains;
it receives `Lagged` when earlier entries have been evicted. Output in this log
stays in memory until it is replaced, so treat it as you would the process
output itself. `start()` output appears both in its returned
`ProcessEventStream` and supervisor watchers after conversion with
`into_event_stream()`. Pipes taken with `take_stdout()`, such as MCP server
traffic, remain caller-owned and are not published.

## Agent

One `AgentRun` covers one framework call and the tools that call starts.

### Agent runs

`Supervisor::start_agent_run()` creates an `AgentRun` for one framework call.
Pass a clone to tool callbacks and start local commands through the run. The
parent `Supervisor` tracks those processes and can stop every active run and
job during application shutdown. A run's cancellation only stops tools
associated with that run; it leaves sibling runs and supervisor-owned
background jobs alone.

The framework still runs the agent, makes model requests, and chooses tools.
`AgentRun::run()` applies the run deadline to the framework future. Direct Rust
callers must call `cancel().await` after normal completion to stop any
persistent run-owned tools. Framework integrations do this cleanup for you.
It does not forcibly kill agent code running in the application process.

```text
Application
└── Supervisor
    ├── AgentRun: one framework call and its run-owned tools
    └── Background jobs: can continue between agent calls
```

#### `AgentRunPolicy`

```rust
pub struct AgentRunPolicy {
    pub timeout: Duration,
    pub shutdown_grace: Duration,
}
```

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `timeout` | `Duration` | 300 seconds | Maximum wall time measured from `AgentRun` creation. |
| `shutdown_grace` | `Duration` | 5 seconds | Time to let registered processes exit during cleanup before terminating them. |

```rust
use agent_supervisor::AgentRunPolicy;
use std::time::Duration;

let policy = AgentRunPolicy {
    timeout: Duration::from_secs(180),
    shutdown_grace: Duration::from_secs(3),
};
```

#### `AgentRun`

```rust
pub struct AgentRun { /* cloneable handle created by Supervisor */ }
```

| Method | Signature | Returns and behavior |
| --- | --- | --- |
| `run` | `async fn run<T, F>(&self, operation: F) -> Result<T, SandboxError>` where `F: Future<Output = T>` | Returns the future's value on success. On deadline or requested cancellation, drops the future, cleans registered processes, and returns `timed_out` or `execution_failed`. |
| `run_command` | `async fn run_command(&self, command: &Command, policy: &Policy, input: &[u8]) -> Result<ExecutionOutput, SandboxError>` | Runs one tool command, bounded by its `Policy` and the remaining agent deadline. |
| `start` | `fn start(&self, command: &Command, policy: &Policy) -> Result<SupervisedChild, SandboxError>` | Starts a process for this run and returns its per-process handle. The parent supervisor tracks it and the run can stop it during cleanup. |
| `cancellation_token` | `fn cancellation_token(&self) -> CancellationToken` | Returns a clone for framework or app APIs that accept a Tokio cancellation token. |
| `request_cancel` | `fn request_cancel(&self)` | Signals the token. Call `cancel().await` to wait for registered process cleanup. |
| `cancel` | `async fn cancel(&self)` | Signals cancellation, waits for the configured grace period, then terminates remaining registered process groups. Call this after normal completion, too. |
| `cancel_now` | `fn cancel_now(&self)` | Signals cancellation and immediately requests termination of registered process groups; it does not wait for cleanup. |
| `active_processes` | `fn active_processes(&self) -> usize` | Returns the number of persistent child processes registered to this run. |

If the operation passed to `AgentRun::run()` returns a `Result`, the run result is
nested: the outer `Result` reports run timeout or cancellation, and the inner
one reports the operation's result. This lets the caller distinguish an SDK run
ending from a tool execution error.

```rust
use agent_supervisor::{AgentRunPolicy, Command, Policy, Supervisor};
use std::time::Duration;

let supervisor = Supervisor::new();
let run = supervisor.start_agent_run(AgentRunPolicy::default());
let tool_run = run.clone();

// Register a callback like this with the framework's agent. The callback
// receives the clone and starts its local command through the agent run.
let operation = async move {
    let command = Command {
        argv: vec!["/usr/bin/printf".into(), "tests passed\n".into()],
        cwd: None,
    };
    let mut tool_policy = Policy::default();
    tool_policy.limits.timeout_ms = 120_000;
    tool_run.run_command(&command, &tool_policy, b"").await
};

// Replace this future with the framework's async agent invocation.
let result = run.run(operation).await;
// Close persistent tools when the framework returns, including on success.
run.cancel().await;
let output = result??;
println!("{:?}: exit={:?}", output.termination, output.exit_code);
```

On timeout, Tokio drops the in-flight framework future. A framework that accepts
a cancellation token can also observe `run.cancellation_token()`. These
controls apply to the Rust future and processes started through the run; they
do not sandbox model requests or terminate the application process.

For an interactive tool process, use `start()` and keep the returned
`SupervisedChild` for its stdin, output, and shutdown lifecycle:

```rust
let child = tool_run.start(&command, &Policy::default())?;
let mut process = child.into_event_stream()?;
process.write(b"task\n").await?;
process.close_stdin().await?;
let receipt = process.wait().await?;
println!("{:?}", receipt.termination);
```

When the `AgentRun` is cancelled or `cancel().await` is called, the run
cleans up persistent children started through it.

### Resume a framework invocation

Each resumed invocation gets a **new** `AgentRun`; a finished run cannot be
reused. Load the framework's checkpoint, call `start_agent_run()` again, and
find any background job through its saved `SupervisorProcessId` on the same live
`Supervisor`. See [Save and resume background jobs](#save-and-resume-background-jobs)
for the complete Rust example and its limits after a restart.

```rust
let supervisor = Supervisor::new(); // Keep this one alive across both invocations.

// Invocation 1: run the agent, start a job that outlives the run, save state.
let run = supervisor.start_agent_run(AgentRunPolicy::default());
let checkpoint = run.run(agent_step_one()).await??; // framework's own state
run.cancel().await; // stops this run's tools, not the background job
let saved_record = start_job_and_save(&supervisor, checkpoint).await?;
save_to_application_store(&saved_record).await?;

// Invocation 2, same live Supervisor: `resume` starts a new AgentRun, looks the
// job up by its saved SupervisorProcessId, and does not replay it if missing.
let saved_record = load_from_application_store().await?;
resume(&supervisor, &saved_record).await?;
```

`start_job_and_save()` and `resume()` are defined in
[Save and resume background jobs](#save-and-resume-background-jobs).
`agent_step_one()`, `save_to_application_store()` and
`load_from_application_store()` stand for your framework and storage calls.


## Tool

Commands and processes started by a run or by the supervisor, with their policy, results, and errors.

### `Command`

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

### `Policy`

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
execution; persistent processes started with `Supervisor::start()` or
`Supervisor::start_background()` do not stop automatically when this timeout
elapses. Stop them explicitly with the process handle or supervisor API.

### One-shot execution

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

### Persistent processes

Use `Supervisor::start()` when the child needs to remain interactive. Convert
the returned child with `into_event_stream()` when its output should be visible
to both the caller and supervisor watchers. Take the raw pipes when an
integration needs direct ownership; the supervisor tracks either form so it
can clean up the process group.

```rust
let supervisor = Supervisor::new();
let mut process = supervisor.start(&command, &policy)?.into_event_stream()?;
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

### Errors and platform support

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
