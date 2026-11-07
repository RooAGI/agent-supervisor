# Supervisor

`Supervisor` is the in-process owner of agent runs and local child processes.
It is not a daemon and does not keep jobs alive independently of the host
application. The application owns the agent framework and one `Supervisor`;
the framework runs the model loop while the supervisor manages processes
started through its APIs.

## Application setup

Create the supervisor when the application starts and pass it to framework
adapter calls. Each framework invocation gets its own `AgentRun` and cleanup
boundary. Use `start_background()` for jobs that should continue between
agent runs. At application shutdown, call `shutdown_all()` while the async
runtime is still running to stop active work and collect background results.

=== "Python"

    ```python
    from agent_supervisor import Supervisor
    from agent_supervisor.openai import Runner

    supervisor = Supervisor()
    result = await Runner.run(agent, prompt, supervisor=supervisor)

    # Before the application's event loop exits:
    results = await supervisor.shutdown_all()
    ```

=== "Rust"

    ```rust
    use agent_supervisor::Supervisor;
    use std::time::Duration;

    let supervisor = Supervisor::new();

    // Create an AgentRun for each framework call and route its tools through
    // that run. Start cross-run jobs with start_background().

    // Before the Tokio runtime exits:
    let results = supervisor.shutdown_all(Duration::from_secs(5)).await?;
    ```

Framework state and checkpoints remain owned by the framework or application.
Agent Supervisor does not run the agent, issue model requests, or contain
arbitrary application code. See [Agent runs and SDK adapters](api-boundaries.md)
for framework-specific integration, and the [security model](security-model.md)
for enforcement details.

When the host drops the supervisor or exits, the supervisor requests cleanup
of its registered process trees using the containment support available on
that platform.

## Interactive and manager-owned processes

Both APIs use the same process supervision backend, but assign output and
completion ownership differently:

- `Supervisor::start()` returns a per-process handle in both languages. In
  Rust, convert the `SupervisedChild` with `into_event_stream()` to send input
  and read output; those chunks are also published to `Supervisor::stream()`
  watchers. Taking the raw stdout or stderr pipes keeps them caller-owned, so
  their bytes are not visible to supervisor stream watchers. In Python, the
  returned `Process` uses the managed event reader, so its output is also
  published to supervisor watchers. The supervisor tracks each process for
  cleanup in either language.
- `Supervisor::start_background()` returns a stable supervisor-scoped process
  ID. The supervisor drains stdout and stderr, observes exit, reaps the child,
  and retains its newest output plus the final receipt/result for lookup. A
  job is not stopped for writing more than its output limits. Watch it live
  with `stream()`; see the [Rust](rust-api.md#stream-output) and
  [Python](python-api.md#stream-output) references.

`Supervisor::stream()` is observational: dropping a watcher never stops a
process. The supervisor retains a bounded event history for late watchers and
reports a `lagged` event if a watcher falls behind. It does not make raw pipe
reads observable; use the managed event reader when output should reach both
the caller and supervisor watchers.

Use `shutdown_process(id, grace)` to stop one job and leave its siblings
running. Use `shutdown_all(grace)` when the host is finishing. It requests
stops for all registered processes, then awaits output draining and reaping
for manager-owned background jobs and returns their final results. For
interactive processes started with `start()`, `shutdown_all()` only requests
the stop; their caller-owned handles must still finish reading/draining and
waiting for the final receipt/result. Completed results can be taken or
forgotten to keep long-lived supervisors' result registries bounded.

```text
start job → drain output, keep newest → observe exit → reap → retain result
    │
    ├─ shutdown one ID → graceful request → grace period → force-stop if needed
    └─ shutdown all → stop jobs → drain and reap → return final results
```

## Shutdown behavior and limits

On Unix, explicit shutdown sends a graceful signal to the process group and
force-stops it if it remains alive after the requested grace period. The
Windows backend has no graceful process signal, so it immediately requests a
force-stop and does not use the grace period. The returned stop outcome says
which stop request was sent; call `wait_process()` or `shutdown_all()` to
observe completion and retrieve the exit result.

The synchronous `Drop` path requests process-tree termination but cannot await
output draining or reaping. For final output and receipts, call the async
`shutdown_all()` method while the Tokio runtime is still alive. Background
jobs require an active Tokio runtime because they use managed stdin and output
tasks.

Process stdout and stderr are bounded when the supervisor or managed event
reader drains them. Rust configures these with `ResourceLimits.output_bytes`
and `ResourceLimits.stderr_bytes`; a Rust caller that takes raw pipes owns
draining and bounding them. Python uses `Command.max_stdout_bytes` and
`Command.max_stderr_bytes`. The Rust
`ResourceLimits.timeout_ms` and Python `Command.timeout` apply to one-shot
execution. A persistent process started with `start()` or
`start_background()` continues until it exits or the host explicitly stops
it.

Process groups or native job containers let the host stop descendants that
remain inside the managed process tree. Controls vary by operating system and
cannot contain descendants that deliberately detach or move outside the
managed boundary. See the [security model](security-model.md) for platform
limits.

For long-lived MCP servers, see the [MCP guide](mcp.md). For the API methods,
see the [Rust API](rust-api.md#manager-owned-background-jobs) and
[Python API](python-api.md#supervisor-manager).
