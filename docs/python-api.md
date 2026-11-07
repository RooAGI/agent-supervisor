# Python API

`Supervisor` is the top-level owner of agent runs and local child processes.
Create an `AgentRun` through `Supervisor.start_agent_run()` for each framework
call. The supervisor tracks that run's tools and can also own background jobs
that continue between calls. The framework still runs the agent and makes
model requests; Agent Supervisor does not sandbox Python code or model calls.

For the shared concepts and Rust/Python type mapping, see [Core concepts](api-reference.md). For framework-specific examples, see [Agent SDK
integrations](api-boundaries.md).

For application startup and shutdown, see the [Supervisor guide](supervisor-lifecycle.md#application-setup).

## Install

Install the published Python package with:

```bash
python -m pip install agent-supervisor
```

Persistent-process support and the run adapters described below are included
in the repository source. To use them with a framework dependency, clone the
repository and install the matching extra from the `python` directory. For
example:

```bash
git clone https://github.com/RooAGI/agent-supervisor.git
cd agent-supervisor/python
python -m pip install -e '.[openai]'
```

Replace `openai` with `microsoft`, `langgraph`, `pydantic-ai`, or `google-adk`
for another adapter. The package requires Python 3.9 or newer. Source builds
also need Rust and your platform's native build tools. See the
[quickstart](quickstart.md) for platform setup.

## Supervisor

The supervisor owns agent runs, background jobs, and every process it tracks.

### Supervisor manager

Use `Supervisor.start_agent_run()` to create a run owned by this supervisor.
Use `Supervisor.start_background()` when the host needs to start multiple
independent jobs and manage each by a stable ID. The supervisor drains each
job's stdout and stderr, watches for exit, reaps the process, and keeps its
newest output and final result available. This API requires an active Tokio
runtime, which the Python extension starts for its async calls.

The existing `Supervisor.start()` is unchanged. It returns a `Process` object
for that one child. Your code uses it to send stdin, read output events, wait,
or shut down the process; the `Supervisor` still tracks the process for
cleanup. With `start_background()`, the manager handles output and waiting
internally, and your code refers to the job by its `SupervisorProcessId`.

```text
start_agent_run(policy=None) -> AgentRun
active_agent_runs() -> int
async def start_background(command, *, policy=None) -> SupervisorProcessId
process_status(process_id) -> "running" | "completed" | "failed" | None
process_result(process_id) -> RunResult | None
async def wait_process(process_id) -> RunResult | None
take_process_result(process_id) -> RunResult | None
forget_process(process_id) -> bool
async def write_process_stdin(process_id, data: bytes) -> None
async def close_process_stdin(process_id) -> None
async def shutdown_process(process_id, *, grace=5.0) -> str
async def shutdown_all(*, grace=5.0) -> list[tuple[SupervisorProcessId, RunResult]]
```

`SupervisorProcessId` is a value-comparable object with `.value` and
`int(process_id)` accessors. Use `.token` to store the complete manager-scoped
identity as a string and `SupervisorProcessId.from_token(token)` to restore
that value later. `.value` is only the local sequence number; do not persist it
alone. `process_status()` returns `None` for an unknown or forgotten ID.
`process_result()` returns a snapshot for a completed job;
`wait_process()` waits for an active job to finish draining and reaping.
`take_process_result()` returns and removes its completed result, while
`forget_process()` removes completed or failed state without returning it.

`shutdown_process()` stops only that process tree and returns the stop request
outcome as one of `graceful_stop_requested`, `force_stop_requested`,
`already_stopping`, `already_exited`, or `not_found`. The result reports which
stop request was issued; call `wait_process()` to observe the exit and obtain
its `RunResult`. On Windows, graceful signaling is unavailable and the
supervisor immediately requests a force-stop. On other supported platforms,
the supervisor allows `grace` seconds before escalating.

`shutdown_all()` cancels active agent runs, stops registered processes, and
awaits background output collection. It returns and removes each background
job's result. Interactive `Process` handles still own their final output and
wait operation.

The `Command.timeout` value does not automatically stop a persistent
background job. It applies to one-shot `run()`. Stop a long-running background
job explicitly with `shutdown_process()` or `shutdown_all()`.

A background job may write any amount of output. `max_stdout_bytes` and
`max_stderr_bytes` set how much of the newest output its result keeps, and
`RunResult.output_truncated` is `True` if older bytes were discarded. Exceeding
them does not stop the job. Other process modes stop at the limit.

Dropping the Python `Supervisor` releases its Rust owner and requests
termination of its remaining process trees. Keep the application Tokio runtime
alive until background monitors can reap them. Prefer `await
supervisor.shutdown_all()` when the application needs final results before
runtime shutdown; owner drop is the cleanup fallback.

```python
import asyncio
import sys

from agent_supervisor import Command, Policy, Supervisor


def python_job(code: str) -> Command:
    return Command([sys.executable, "-u", "-c", code], timeout=30)


async def main() -> None:
    supervisor = Supervisor()
    policy = Policy()
    report_id = await supervisor.start_background(
        python_job("print('report ready')"), policy=policy
    )
    stop_id = await supervisor.start_background(
        python_job("import time; time.sleep(30)"), policy=policy
    )
    sibling_id = await supervisor.start_background(
        python_job("import time; time.sleep(30); print('sibling finished')"),
        policy=policy,
    )

    report = await supervisor.wait_process(report_id)
    print(report.stdout.decode("utf-8", errors="replace"), end="")

    stop = await supervisor.shutdown_process(stop_id, grace=1.0)
    print("stop request:", stop)
    stopped = await supervisor.wait_process(stop_id)
    print("stopped:", stopped.termination, stopped.exit_code, stopped.signal)
    sibling_status = supervisor.process_status(sibling_id)
    assert sibling_status == "running"
    print("sibling status:", sibling_status)

    # Await shutdown before the Tokio runtime exits and inspect remaining jobs.
    # Windows performs an immediate force-stop because it has no graceful signal.
    for process_id, result in await supervisor.shutdown_all(grace=1.0):
        print(int(process_id), result.termination, result.exit_code)


asyncio.run(main())
```

See the runnable [Python background-job example](https://github.com/RooAGI/agent-supervisor/blob/main/examples/python/supervisor_background_jobs.py).

#### Input and output

Choose the process mode by how you need to talk to the process:

| Mode | Send input | Receive output | Live streaming |
| --- | --- | --- | --- |
| `run(command, input=...)` | One `bytes` value, then stdin closes. | Bounded `stdout` and `stderr` in the `RunResult` when the command ends. | Inside an agent run, with `stream(run=...)` |
| `Supervisor.start(command)` | `process.write(data)` and `process.close_stdin()` at any time. | `OutputEvent`s from `async for event in process`. | Yes, or with `stream()` |
| `Supervisor.start_background(command)` | `write_process_stdin(id, data)` and `close_process_stdin(id)`. | The newest `stdout` and `stderr`, read after the job finishes with `wait_process(id)` or `process_result(id)`. | With `stream(process=id)` |

Output is bounded by the command's `max_stdout_bytes` and `max_stderr_bytes`. A
one-shot or interactive process stops at the limit; a background job keeps
running and retains its newest output. To watch any of them live, use
[`Supervisor.stream()`](#stream-output).

**Stream output from a supervised process.** `Process` is an async iterator of
events (`started`, `stdout`, `stderr`, `exited`):

```python
process = await supervisor.start(Command([sys.executable, "-u", "worker.py"]))
await process.write(b"start\n")
await process.close_stdin()

async for event in process:
    if event.kind in ("stdout", "stderr"):
        print(event.data.decode("utf-8", errors="replace"), end="")
    elif event.kind == "exited":
        print(event.result.termination, event.result.exit_code)
```

**Send input to a background job and read its output.**

```python
job_id = await supervisor.start_background(Command([sys.executable, "-u", "worker.py"]))
await supervisor.write_process_stdin(job_id, b"start\n")
await supervisor.close_process_stdin(job_id)  # send EOF

result = await supervisor.wait_process(job_id)  # waits for exit and output
print(result.stdout_text("utf-8", "replace"))
```

#### Save and resume background jobs

Your framework or application stores its own session or checkpoint. To find
an application-owned background job after a framework invocation resumes,
store the job's `SupervisorProcessId.token` beside that state. Each invocation
gets a new `AgentRun`; the background job remains managed by the same live
application `Supervisor`.

This does not restore an OS process after an application restart, or resume a
run-owned tool process after its `AgentRun` has ended. If lookup returns
`None`, decide how to recover from the framework checkpoint; do not replay the
old command automatically.

```python
import json
import sys

from agent_supervisor import (
    AgentRunPolicy,
    Command,
    Supervisor,
    SupervisorProcessId,
)


async def start_job_and_save(supervisor, framework_checkpoint):
    process_id = await supervisor.start_background(
        Command([sys.executable, "-u", "-c", "import time; time.sleep(60)"])
    )

    # Store this JSON with the framework's native session/checkpoint data.
    saved_record = json.dumps({
        "framework_checkpoint": framework_checkpoint,
        "background_process": process_id.token,
    })
    await save_to_application_store(saved_record)
    return process_id


async def resume(supervisor, saved_record):
    record = json.loads(saved_record)
    process_id = SupervisorProcessId.from_token(record["background_process"])
    run = supervisor.start_agent_run(AgentRunPolicy())

    async with run:
        status = supervisor.process_status(process_id)
        if status is None:
            # The original manager is gone or no longer retains this job.
            # Resume app/framework state according to your own recovery policy.
            return {"job": "unavailable", "checkpoint": record["framework_checkpoint"]}

        return await run.run(
            resume_framework(record["framework_checkpoint"], status)
        )
```

`save_to_application_store()` and `resume_framework()` stand for your
application's storage and framework-specific resume calls. Keep the same
`Supervisor` alive between invocations if the background job must continue.
The token can be parsed after restart, but the new `Supervisor` will not find
the old process and will not start it again.

### Stream output

```text
Supervisor.stream(*, run=None, process=None) -> async iterator of SupervisorEvent
```

`Supervisor.stream()` watches work as one ordered stream of events. It only
observes: launch work with `run()`, `Supervisor.start()`,
`Supervisor.start_background()`, or a framework adapter. Stopping a watcher
stops nothing, and a slow watcher never slows a process.

| Call | Shows | Ends when |
| --- | --- | --- |
| `supervisor.stream(process=job_id)` | One process or background job. | It exits. |
| `supervisor.stream(run=run)` | One agent run: framework events and its tools. | The run ends. |
| `supervisor.stream()` | Everything on this supervisor. | The supervisor is dropped. Stop reading when you are done. |

```python
job_id = await supervisor.start_background(Command([sys.executable, "-u", "worker.py"]))

async for event in supervisor.stream(process=job_id):
    if event.kind in ("stdout", "stderr"):
        print(event.data.decode("utf-8", errors="replace"), end="")
    elif event.kind == "exited":
        print(event.result.termination if event.result else "handle dropped")
```

Each `SupervisorEvent` has `seq`, `kind`, `run_id`, and `process_id`:

| `kind` | Extra fields | Meaning |
| --- | --- | --- |
| `"run_started"`, `"run_ended"` | | An agent run began or finished. |
| `"started"` | | A process started. |
| `"stdout"`, `"stderr"` | `data` | Bytes the process wrote. |
| `"exited"` | `result` | A process finished. `result` has no output, and is `None` if its handle was dropped before the exit was observed. |
| `"agent"` | `payload` | The framework's own event object, unchanged. |
| `"lagged"` | `dropped` | This watcher fell behind and lost entries. |

Events are in the order they were recorded. stdout and stderr are read
separately, so they are not ordered against each other.

**Watching an agent.** Framework adapters publish the agent's events to the
run. Each adapter's result has an `agent_run` to watch:

```python
result = await Runner.run_streamed(agent, prompt, supervisor=supervisor)

async for event in supervisor.stream(run=result.agent_run):
    if event.kind == "agent":
        handle(event.payload)              # framework-specific object
    elif event.kind == "stdout":
        show(event.process_id, event.data)  # a tool's live output
```

This needs a streaming call, such as `run_streamed`, LangGraph `stream`, or
Microsoft `stream=True`. A non-streaming call shows only its tool processes.
You do not have to read the framework's own stream: the adapter reads it in the
background, and it still delivers every event if you do. That means the agent
starts as soon as you call the adapter, not when you first read the stream.
PydanticAI events come through its `event_stream_handler`, so
`run_stream()` returns a context manager with `agent_run` on it. Payloads are
not normalized between frameworks, and nothing in this stream reaches the model.

**What is retained.** Process output in the log is limited to about the newest
8 MiB per supervisor. Opaque agent framework payloads have a separate
256-event retention limit because their sizes cannot be measured. A watcher
starts at the oldest retained event, so a late watcher sees only what remains;
it receives a `lagged` event when earlier entries have been evicted. Interactive
`Process` output appears as your code reads it. MCP server traffic is not
published, only the server's start and exit.

## Agent

One `AgentRun` covers one framework call and the tools that call starts.

### Agent runs

Create each `AgentRun` through the top-level `Supervisor`. Pass the supervisor
to a framework adapter with `supervisor=supervisor`; the adapter creates the
run, associates tool processes with it, and cleans them up when the call ends.
For a custom integration, call `supervisor.start_agent_run(policy)`, run the
framework call through `run.run(...)`, and call `await run.cancel()` after
normal completion.

A run's cleanup stops only that run's processes. Background jobs started
directly through `Supervisor.start_background()` can continue between agent
calls. The supervisor stops both when the application shuts down.

### Resume a framework invocation

Each resumed invocation gets a **new** `AgentRun`; a finished run cannot be
reused. Load the framework's checkpoint, call `start_agent_run()` again, and
find any background job through its saved `SupervisorProcessId` on the same live
`Supervisor`. See [Save and resume background jobs](#save-and-resume-background-jobs)
for the complete Python example and its limits after a restart.

```python
import json
import sys

from agent_supervisor import (
    AgentRunPolicy, Command, Supervisor, SupervisorProcessId,
)

supervisor = Supervisor()  # Keep this one alive across both invocations.


async def first_invocation():
    run = supervisor.start_agent_run(AgentRunPolicy())
    async with run:
        checkpoint = await run.run(agent_step_one())  # framework's own state
        job = await supervisor.start_background(
            Command([sys.executable, "-u", "-c", "import time; time.sleep(60)"])
        )
        await save_to_application_store(json.dumps({
            "framework_checkpoint": checkpoint,
            "background_process": job.token,
        }))
    # The run has ended and its tools are stopped. The job keeps running.


async def second_invocation():
    record = json.loads(await load_from_application_store())
    job_id = SupervisorProcessId.from_token(record["background_process"])

    run = supervisor.start_agent_run(AgentRunPolicy())  # a new run
    async with run:
        status = supervisor.process_status(job_id)
        if status is None:
            # Job is gone (for example, the app restarted). Recover from the
            # checkpoint; do not replay the old command.
            return record["framework_checkpoint"]
        return await run.run(
            agent_step_two(record["framework_checkpoint"], status)
        )
```

`agent_step_one()`, `agent_step_two()`, `save_to_application_store()` and
`load_from_application_store()` stand for your framework and storage calls.


### Agent run APIs

An agent run adapter wraps one invocation of your framework. It applies an
overall deadline, makes Agent Supervisor tool processes started during that
invocation join the corresponding `AgentRun`, and cleans those processes up
when the invocation finishes. The framework still controls the agent loop, tool selection, model
requests, and result types. The adapter does not put Python agent code in a
separate process.

#### Shared arguments and lifecycle

Every adapter accepts the top-level `supervisor` and a `supervision` policy
as keyword arguments. Other arguments are passed to the corresponding
framework method. Pass the same supervisor to multiple adapter calls to let it
own and shut them down together.

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `supervisor` | `Supervisor` or `None` | `None` | Parent manager for the run. Pass your application supervisor to manage multiple calls together; if omitted, the adapter creates one for this call. |
| `supervision` | Dictionary or `None` | `None` | Run controls. Accepted keys are `timeout` and `shutdown_grace`. |
| `supervision["timeout"]` | positive finite seconds | `300.0` | Overall time allowed for the framework invocation and streamed output. |
| `supervision["shutdown_grace"]` | seconds from `0` through `60` | `5.0` | Time to let registered tool processes exit before stopping remaining processes. |

```python
supervisor = Supervisor()
result = await Runner.run(
    agent,
    "Summarize the project",
    supervisor=supervisor,
    supervision={"timeout": 180, "shutdown_grace": 3},
)
```

When the deadline expires, async adapters cancel the SDK operation and raise
`asyncio.TimeoutError` after cleaning up registered tool processes. Cancelling
the caller task propagates `asyncio.CancelledError` after cleanup. Whether an
in-flight model request stops depends on the framework's cancellation support;
the Python application itself is not killed.

While `AgentRun.run()` or a framework adapter is active, calls to
`agent_supervisor.run()` and `Supervisor.start()`
register their local processes with that run. Direct `subprocess` calls do
not.

#### `AgentRunPolicy` and `AgentRun`

Use `AgentRun` for a custom framework integration. It is created by the
application's `Supervisor`; it is not a separate top-level manager. The run
supplies a deadline and cancellation signal and tracks local commands started
through it.

```text
supervisor.start_agent_run(policy=None) -> AgentRun
AgentRunPolicy(timeout=300.0, shutdown_grace=5.0)
```

| Type or method | Arguments | Returns and behavior |
| --- | --- | --- |
| `AgentRunPolicy` | `timeout`: positive finite seconds; `shutdown_grace`: 0–60 seconds | Defaults to 300 and 5 seconds, respectively. |
| `Supervisor.start_agent_run(policy)` | `AgentRunPolicy` or `None` | Creates a run owned by this supervisor. Its deadline starts when created. |
| `run.run(operation)` | An awaitable framework operation | Returns its value. Raises `asyncio.TimeoutError` at the deadline and `asyncio.CancelledError` when the run is cancelled. Both paths clean up registered tools. |
| `run.run_command(command, *, policy=None, input=None)` | `Command`; optional process `Policy`; optional `bytes` input | Returns `RunResult` for one local command, limited by both command policy and remaining run time. |
| `run.start(command, *, policy=None)` | `Command` and optional process `Policy` | Returns a persistent `Process` registered for run cleanup. |
| `run.cancellation_token()` | None | Returns a cloneable native `CancellationToken`. |
| `run.request_cancel()` | None | Signals cancellation without waiting for cleanup. |
| `await run.cancel()` | None | Signals cancellation and waits for the configured shutdown grace and cleanup. |
| `run.cancel_now()` | None | Signals cancellation and immediately requests process-group termination. |
| `run.active_processes()` | None | Returns the number of registered persistent processes. |
| `async with run` | None | Calls `run.cancel()` on exit, including when the block raises. |

Use `run.run()` around one agent invocation. Tool callbacks can use the
captured run directly, or call the module-level tool APIs while the run is
active; those calls join the same run.

```python
from agent_supervisor import AgentRunPolicy, Command, Supervisor

supervisor = Supervisor()
run = supervisor.start_agent_run(AgentRunPolicy(timeout=180, shutdown_grace=3))

async def run_agent():
    # Existing framework call; registered tool callbacks use this run.
    return await agent.run("Summarize the project")

async with run:
    result = await run.run(run_agent())
```

When the framework finishes normally, the run remains registered with its
`Supervisor` until `cancel()` or the async context exits. On timeout or cancellation,
`run.run()` cancels the framework awaitable and performs tool cleanup before
raising. This is the Python run API, backed by the same Rust lifecycle implementation.
The framework adapters below use these same public types.

#### OpenAI Agents SDK: `agent_supervisor.openai.Runner`

Replace the SDK's `Runner` import with the adapter. The `agent` and prompt
arguments stay the same, and successful calls return the SDK's normal result.

```python
from agent_supervisor import Supervisor
from agent_supervisor.openai import Runner

supervisor = Supervisor()
result = await Runner.run(agent, "Run the requested task", supervisor=supervisor)
print(result.final_output)
```

| Method | Signature | Returns |
| --- | --- | --- |
| `run` | `async Runner.run(*args, supervisor=None, supervision=None, **kwargs)` | The OpenAI Agents SDK run result. |
| `run_streamed` | `async Runner.run_streamed(*args, supervisor=None, supervision=None, **kwargs)` | A AgentRun-backed wrapper around the SDK streaming result. It forwards the SDK result's attributes and adds `stream_events()`. |
| `run_sync` | `Runner.run_sync(*args, supervisor=None, supervision=None, **kwargs)` | The OpenAI Agents SDK run result, synchronously. Call outside an already-running asyncio event loop. |

Consume the stream to its end, or close/cancel it, so the agent run can finish:

```python
streamed = await Runner.run_streamed(
    agent, "Run the requested task", supervisor=supervisor
)
async for event in streamed.stream_events():
    print(event)
```

#### Microsoft Agent Framework: `agent_supervisor.microsoft.run`

`run()` calls the existing agent's `agent.run(...)` method and returns its
result unchanged for non-streaming calls.

```python
from agent_supervisor.microsoft import run

result = await run(agent, "Run the requested task")
print(result.text)
```

| Signature | Return |
| --- | --- |
| `async run(agent, *args, supervisor=None, supervision=None, **kwargs)` | The framework's result. When `stream=True` returns an async response stream wrapper. |

For a streaming response, consume it or use the returned wrapper as an async
context manager. The wrapper forwards stream attributes, supports
`get_final_response()`, and closes the framework stream when closed.

```python
updates = await run(agent, "Run the requested task", stream=True)
async with updates:
    async for update in updates:
        print(update)
    final = await updates.get_final_response()
```

#### LangGraph: `agent_supervisor.langgraph`

`run()` calls `graph.ainvoke(inputs, ...)`. `stream()` calls
`graph.astream(inputs, ...)` and keeps the `AgentRun` open as you consume graph
events.

| Function | Signature | Returns |
| --- | --- | --- |
| `run` | `async run(graph, inputs, *args, supervisor=None, supervision=None, **kwargs)` | The graph's final state. |
| `stream` | `stream(graph, inputs, *args, supervisor=None, supervision=None, **kwargs)` | A AgentRun-backed async iterator of the graph's events. |

```python
from agent_supervisor.langgraph import run, stream

state = await run(graph, {"messages": ["Hello"]})
async for event in stream(graph, {"messages": ["Hello"]}):
    print(event)
```

Close the iterator or use it as an async context manager if you stop before the
graph finishes.

#### PydanticAI: `agent_supervisor.pydantic_ai`

`run()` forwards arguments to `agent.run(...)` and returns its `AgentRunResult`.
`run_stream()` wraps the framework's native stream context manager.

| Function | Signature | Returns |
| --- | --- | --- |
| `run` | `async run(agent, *args, supervisor=None, supervision=None, **kwargs)` | The PydanticAI `AgentRunResult`. |
| `run_stream` | `run_stream(agent, *args, supervisor=None, supervision=None, **kwargs)` | A AgentRun-backed async context manager yielding the framework's stream result. |

```python
from agent_supervisor.pydantic_ai import run, run_stream

result = await run(agent, "Hello")
print(result.output)

async with run_stream(agent, "Tell me a story") as streamed:
    async for text in streamed.stream_text():
        print(text, end="")
```

#### Google ADK: `agent_supervisor.google_adk.run`

`run()` calls `runner.run_async(...)` and returns a AgentRun-backed async iterator of ADK
events. It creates an `asyncio.Event` for `abort_signal` when you do not pass
one. On early close or failure, it sets that event so the ADK run can abort.

| Signature | Return |
| --- | --- |
| `run(runner, *args, supervisor=None, supervision=None, **kwargs)` | A AgentRun-backed async iterator of Google ADK events. |

```python
from agent_supervisor.google_adk import run

events = run(runner, user_id="user", session_id="session", new_message=message)
async for event in events:
    print(event)
```

Consume all events or close the iterator to finish cleanup. Use the framework
specific [Agent SDK integration guide](api-boundaries.md) for install commands
and complete setup examples.

These calls are separate from `agent_supervisor.run(command, ...)`, which
starts one local command. Run adapters require the repository source install
and their matching optional dependency extra.

## Tool

Commands and processes started by a run or by the supervisor, with their policy, results, and errors.

### `Command`

`Command` describes an executable and its arguments. The executable and
working directory must be absolute paths. The argument vector is passed
directly; the library does not insert a shell.

```text
Command(
    argv,
    *,
    cwd=None,
    env=None,
    inherit_env=None,
    timeout=5.0,
    max_stdin_bytes=65536,
    max_stdout_bytes=65536,
    max_stderr_bytes=16384,
)
```

| Argument | Meaning |
| --- | --- |
| `argv` | Nonempty list of strings. First item is an absolute executable path; remaining items are arguments. |
| `cwd` | Optional absolute working directory. |
| `env` | Variables to set in the child environment; default `{}`. |
| `inherit_env` | Parent environment variable names to copy; default `[]`. Other parent variables are not inherited. |
| `timeout` | Positive finite runtime limit in seconds; default `5.0`. Applies to `run()`. Persistent `Supervisor` processes require explicit shutdown. |
| `max_stdin_bytes` | Maximum input accepted by `run(input=...)`; default `65536`. |
| `max_stdout_bytes` | Maximum stdout bytes collected or streamed; default `65536`. |
| `max_stderr_bytes` | Maximum stderr bytes collected or streamed; default `16384`. |

```python
import sys
from pathlib import Path

from agent_supervisor import Command

command = Command(
    [sys.executable, "-c", "print('hello')"],
    cwd=str(Path.cwd()),
    timeout=10.0,
    max_stdout_bytes=128 * 1024,
)
```

The child starts with a cleared environment. Add only the values it needs with
`env` or allowlist selected parent variables with `inherit_env`.

### `Policy`

`Policy` controls filesystem access, network mode, and how the host handles
unavailable enforcement. Environment and byte/time limits are set on
`Command`.

```text
Policy(*, network="host", enforcement="best_effort", filesystem=None)
```

| Argument | Values and default |
| --- | --- |
| `network` | `"host"` or `"disabled"`; default `"host"`. |
| `enforcement` | `"best_effort"` or `"required"`; default `"best_effort"`. |
| `filesystem` | Optional list of `(absolute_path, access_list)` grants; default `None`. Access values are `"read"`, `"write"`, and `"append"`. |

`filesystem=None` requests no filesystem restriction. `filesystem=[]` requests
an explicit policy with no grants. Grants apply to their directory tree;
enforcement depends on the host platform. Append access must be paired with
write access.

```python
from agent_supervisor import Policy

policy = Policy(
    network="disabled",
    enforcement="required",
    filesystem=[("/workspace/project", ["read", "write"])],
)
```

### Run one command

```text
async def run(command, *, policy=None, input=None, cancellation=None) -> RunResult
```

`run()` collects bounded stdout and stderr. `input` must be `bytes`; omitted
input means empty stdin. If no policy is given, defaults are host networking,
best-effort enforcement, and no filesystem restriction.

```python
import sys
from agent_supervisor import Command, Policy, run

result = await run(
    Command(
        [sys.executable, "-c", "import sys; print(sys.stdin.read())"],
        timeout=10.0,
    ),
    policy=Policy(network="host"),
    input=b"hello",
)

print(result.stdout_text("utf-8", "replace"), end="")
print(result.termination, result.exit_code, result.enforcement)
```

#### `RunResult`

| Field or method | Meaning |
| --- | --- |
| `stdout`, `stderr` | Captured output as bounded `bytes`. |
| `success` | `True` when the process exited with code zero. |
| `termination` | `"exited"`, `"timed_out"`, `"cancelled"`, `"stdout_limit_exceeded"`, or `"stderr_limit_exceeded"`. |
| `exit_code`, `signal` | Exit status fields when available. |
| `enforcement` | `"enforced"`, `"degraded"`, `"unavailable"`, or `"trusted"`. |
| `requested_executable`, `canonical_executable` | Requested path and verified canonical path when available. |
| `error` | Optional error text when reading a background job's output failed. |
| `output_truncated` | `True` when a background job wrote more than its limits, so `stdout` or `stderr` holds only the newest bytes. |
| `stdout_text(encoding, errors)`, `stderr_text(encoding, errors)` | Decode output explicitly. |

Timeouts, cancellation, nonzero exits, and output-limit termination are
reported in the result. Invalid requests, unavailable required enforcement,
and launch or I/O failures raise `SupervisorError`.

### Persistent processes

Use `Supervisor.start()` when the child needs to stay interactive. A `Process`
is an async iterator of `OutputEvent`s and also provides stdin, wait, and
shutdown methods.

```text
async def start(command, *, policy=None) -> Process
```

```python
import sys
from agent_supervisor import Command, Policy, Supervisor

supervisor = Supervisor()
process = await supervisor.start(
    Command([
        sys.executable,
        "-u",
        "-c",
        "import sys; line=sys.stdin.buffer.readline(); "
        "sys.stdout.buffer.write(b'received:' + line)",
    ]),
    policy=Policy(),
)

await process.write(b"hello\\n")
await process.close_stdin()  # sends EOF

async for event in process:
    if event.kind == "stdout":
        print(event.data.decode("utf-8", errors="replace"), end="")
    elif event.kind == "exited":
        result = event.result
```

| Method | Behavior |
| --- | --- |
| `process.events()` / `async for event in process` | Receive `started`, output, and terminal events. |
| `await process.write(data)` | Write `bytes` to child stdin. |
| `await process.close_stdin()` | Close stdin and send EOF. |
| `await process.wait()` | Drain remaining events and return the final `RunResult`. |
| `await process.shutdown(grace=seconds)` | Close stdin, allow a bounded shutdown period, then stop the process tree if needed. Returns `RunResult`; `grace` must be between 0 and 60 seconds. |

Output events are subject to the stdout and stderr limits on `Command`.
Calling `wait()` after consuming events returns the same completed result.

### Cancellation

`CancellationToken` is a Python wrapper around the native cancellation token.
Pass it to `run()` and call `cancel()` from another task. A
cancelled one-shot command returns a result with `termination == "cancelled"`.

```python
import asyncio
import sys
from agent_supervisor import CancellationToken, Command, run

token = CancellationToken()
task = asyncio.create_task(
    run(
        Command([sys.executable, "-c", "import time; time.sleep(30)"]),
        cancellation=token,
    )
)
await asyncio.sleep(0.1)
token.cancel()
result = await task
print(result.termination)
```

Cancelling the Python task itself also triggers child cleanup before
`asyncio.CancelledError` propagates. For a persistent process, use
`process.shutdown(grace=...)`.

### Errors and platform capabilities

`SupervisorError` is raised for invalid policies, launch or I/O failures, and
required controls the platform cannot provide. It exposes `code`, `phase`,
`message`, and `retryable`; `executable`, `termination`, and `stderr_tail` may
also be present. Branch on `code` rather than parsing `message`.

Call `platform_capabilities()` to inspect host support before requesting
optional controls:

```python
from agent_supervisor import platform_capabilities

capabilities = platform_capabilities()
print(capabilities["containment"], capabilities["filesystem_isolation"])
```

The dictionary includes `containment`, `host_network`, `network_isolation`,
`network_isolation_requires_filesystem`, `graceful_shutdown`,
`process_group_cleanup`, `parent_death_cleanup`, `filesystem_isolation`,
`memory_limits`, `process_limits`, and `cpu_limits`. Capabilities vary by
operating system. Use `enforcement="required"` when launch must fail rather
than run without a requested control.

### MCP server adapters

The `agent_supervisor.mcp` and `agent_supervisor.fastmcp` modules provide stdio
adapters for the official MCP Python SDK and Prefect FastMCP. Install the
matching extra with `python -m pip install 'agent-supervisor[mcp]'` or
`python -m pip install 'agent-supervisor[fastmcp]'`. See the [MCP guide](mcp.md)
for examples and configuration.
