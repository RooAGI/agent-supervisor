# Python API

The Python API supervises agent runs and the local commands their tool
functions start. Framework adapters add a deadline and cleanup lifecycle around
one SDK invocation. `run()`, `stream()`, and `Supervisor` manage individual
local processes. The agent framework still runs the agent and makes model
requests; these APIs do not sandbox Python code or model calls.

For the shared concepts and Rust/Python type mapping, see [Core
Concept](api-reference.md). For framework-specific examples, see [Agent SDK
integrations](api-boundaries.md).

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

## Agent run APIs

An agent run adapter wraps one invocation of your framework. It applies an
overall deadline, makes Agent Supervisor tool processes started during that
invocation join the run scope, and cleans those processes up when the invocation
finishes. The framework still controls the agent loop, tool selection, model
requests, and result types. The adapter does not put Python agent code in a
separate process.

### Shared arguments and lifecycle

Every adapter accepts `supervision` as a keyword argument. Other arguments are
passed to the corresponding framework method.

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `supervision` | Dictionary or `None` | `None` | Run controls. Accepted keys are `timeout` and `shutdown_grace`. |
| `supervision["timeout"]` | positive finite seconds | `300.0` | Overall time allowed for the framework invocation and streamed output. |
| `supervision["shutdown_grace"]` | seconds from `0` through `60` | `5.0` | Time to let registered tool processes exit before stopping remaining processes. |

```python
result = await Runner.run(
    agent,
    "Summarize the project",
    supervision={"timeout": 180, "shutdown_grace": 3},
)
```

When the deadline expires, async adapters cancel the SDK operation and raise
`asyncio.TimeoutError` after cleaning up registered tool processes. Cancelling
the caller task propagates `asyncio.CancelledError` after cleanup. Whether an
in-flight model request stops depends on the framework's cancellation support;
the Python application itself is not killed.

While `AgentRunScope.run()` or a framework adapter is active, calls to
`agent_supervisor.run()`, `agent_supervisor.stream()`, and `Supervisor.start()`
register their local processes with that scope. Direct `subprocess` calls do
not.

### `AgentRunPolicy` and `AgentRunScope`

Use the public run scope when you have a custom framework or want to manage the
lifecycle yourself. The framework call still runs in your application process;
the scope supplies a shared deadline, cancellation signal, and cleanup for
local commands started through Agent Supervisor.

```text
AgentRunPolicy(timeout=300.0, shutdown_grace=5.0)
AgentRunScope(policy=None)
```

| Type or method | Arguments | Returns and behavior |
| --- | --- | --- |
| `AgentRunPolicy` | `timeout`: positive finite seconds; `shutdown_grace`: 0–60 seconds | Defaults to 300 and 5 seconds, respectively. |
| `AgentRunScope` | `policy`: an `AgentRunPolicy`; omitted uses defaults | Creates the native run scope. Its deadline starts when the scope is created. |
| `scope.run(operation)` | An awaitable framework operation | Returns its value. Raises `asyncio.TimeoutError` at the deadline and `asyncio.CancelledError` when the scope is cancelled. Both paths clean up registered tools. |
| `scope.run_command(command, *, policy=None, input=None)` | `Command`; optional process `Policy`; optional `bytes` input | Returns `RunResult` for one local command, limited by both command policy and remaining run time. |
| `scope.start(command, *, policy=None)` | `Command` and optional process `Policy` | Returns a persistent `Process` registered for run cleanup. |
| `scope.cancellation_token()` | None | Returns a cloneable native `CancellationToken`. |
| `scope.request_cancel()` | None | Signals cancellation without waiting for cleanup. |
| `await scope.cancel()` | None | Signals cancellation and waits for the configured shutdown grace and cleanup. |
| `scope.cancel_now()` | None | Signals cancellation and immediately requests process-group termination. |
| `scope.active_processes()` | None | Returns the number of registered persistent processes. |
| `async with scope` | None | Calls `scope.cancel()` on exit, including when the block raises. |

Use `scope.run()` around one agent invocation. Tool callbacks can use the
captured scope directly, or call the module-level tool APIs while the run is
active; those calls join the same scope.

```python
from agent_supervisor import AgentRunPolicy, AgentRunScope, Command

scope = AgentRunScope(AgentRunPolicy(timeout=180, shutdown_grace=3))

async def run_agent():
    # Existing framework call; registered tool callbacks use this scope.
    return await agent.run("Summarize the project")

async with scope:
    result = await scope.run(run_agent())
```

When the framework finishes normally, the scope remains available until
`cancel()` or the async context exits. On timeout or cancellation,
`scope.run()` cancels the framework awaitable and performs tool cleanup before
raising. This is the reusable Python counterpart to Rust's
`AgentRunPolicy` and `AgentRunScope`. The framework adapters below use these
same public types.

### OpenAI Agents SDK: `agent_supervisor.openai.Runner`

Replace the SDK's `Runner` import with the adapter. The `agent` and prompt
arguments stay the same, and successful calls return the SDK's normal result.

```python
from agent_supervisor.openai import Runner

result = await Runner.run(agent, "Run the requested task")
print(result.final_output)
```

| Method | Signature | Returns |
| --- | --- | --- |
| `run` | `async Runner.run(*args, supervision=None, **kwargs)` | The OpenAI Agents SDK run result. |
| `run_streamed` | `async Runner.run_streamed(*args, supervision=None, **kwargs)` | A scoped wrapper around the SDK streaming result. It forwards the SDK result's attributes and adds `stream_events()`. |
| `run_sync` | `Runner.run_sync(*args, supervision=None, **kwargs)` | The OpenAI Agents SDK run result, synchronously. Call outside an already-running asyncio event loop. |

Consume the stream to its end, or close/cancel it, so the run scope can finish:

```python
streamed = await Runner.run_streamed(agent, "Run the requested task")
async for event in streamed.stream_events():
    print(event)
```

### Microsoft Agent Framework: `agent_supervisor.microsoft.run`

`run()` calls the existing agent's `agent.run(...)` method and returns its
result unchanged for non-streaming calls.

```python
from agent_supervisor.microsoft import run

result = await run(agent, "Run the requested task")
print(result.text)
```

| Signature | Return |
| --- | --- |
| `async run(agent, *args, supervision=None, **kwargs)` | The framework's result. When `stream=True` returns an async response stream wrapper. |

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

### LangGraph: `agent_supervisor.langgraph`

`run()` calls `graph.ainvoke(inputs, ...)`. `stream()` calls
`graph.astream(inputs, ...)` and keeps the run scope open as you consume graph
events.

| Function | Signature | Returns |
| --- | --- | --- |
| `run` | `async run(graph, inputs, *args, supervision=None, **kwargs)` | The graph's final state. |
| `stream` | `stream(graph, inputs, *args, supervision=None, **kwargs)` | A scoped async iterator of the graph's events. |

```python
from agent_supervisor.langgraph import run, stream

state = await run(graph, {"messages": ["Hello"]})
async for event in stream(graph, {"messages": ["Hello"]}):
    print(event)
```

Close the iterator or use it as an async context manager if you stop before the
graph finishes.

### PydanticAI: `agent_supervisor.pydantic_ai`

`run()` forwards arguments to `agent.run(...)` and returns its `AgentRunResult`.
`run_stream()` wraps the framework's native stream context manager.

| Function | Signature | Returns |
| --- | --- | --- |
| `run` | `async run(agent, *args, supervision=None, **kwargs)` | The PydanticAI `AgentRunResult`. |
| `run_stream` | `run_stream(agent, *args, supervision=None, **kwargs)` | A scoped async context manager yielding the framework's stream result. |

```python
from agent_supervisor.pydantic_ai import run, run_stream

result = await run(agent, "Hello")
print(result.output)

async with run_stream(agent, "Tell me a story") as streamed:
    async for text in streamed.stream_text():
        print(text, end="")
```

### Google ADK: `agent_supervisor.google_adk.run`

`run()` calls `runner.run_async(...)` and returns a scoped async iterator of ADK
events. It creates an `asyncio.Event` for `abort_signal` when you do not pass
one. On early close or failure, it sets that event so the ADK run can abort.

| Signature | Return |
| --- | --- |
| `run(runner, *args, supervision=None, **kwargs)` | A scoped async iterator of Google ADK events. |

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

## `Command`

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
| `timeout` | Positive finite runtime limit in seconds; default `5.0`. |
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

## `Policy`

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

## Run one command

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

### `RunResult`

| Field or method | Meaning |
| --- | --- |
| `stdout`, `stderr` | Captured output as bounded `bytes`. |
| `success` | `True` when the process exited with code zero. |
| `termination` | `"exited"`, `"timed_out"`, `"cancelled"`, `"stdout_limit_exceeded"`, or `"stderr_limit_exceeded"`. |
| `exit_code`, `signal` | Exit status fields when available. |
| `enforcement` | `"enforced"`, `"degraded"`, `"unavailable"`, or `"trusted"`. |
| `requested_executable`, `canonical_executable` | Requested path and verified canonical path when available. |
| `stdout_text(encoding, errors)`, `stderr_text(encoding, errors)` | Decode output explicitly. |

Timeouts, cancellation, nonzero exits, and output-limit termination are
reported in the result. Invalid requests, unavailable required enforcement,
and launch or I/O failures raise `SupervisorError`.

## Stream output

```text
stream(command, *, policy=None, cancellation=None, capture=False)
```

`stream()` returns an async context manager that yields an async iterator of
`OutputEvent`s. It closes child stdin immediately, so use it for commands that
do not need input. Use `run(input=...)` or a persistent process for stdin.

```python
import sys
from agent_supervisor import Command, stream

async with stream(
    Command([sys.executable, "-c", "print('live', flush=True)"]),
    capture=True,
) as events:
    async for event in events:
        if event.kind in ("stdout", "stderr"):
            print(event.data.decode("utf-8", errors="replace"), end="")
        elif event.kind == "exited":
            result = event.result
```

Events have `kind` (`"started"`, `"stdout"`, `"stderr"`, or `"exited"`),
`data` bytes for output events, and `result` on the terminal `"exited"` event.
`capture=False` (the default) leaves output fields empty in that result;
`capture=True` also collects output within the command limits. Leaving the
context early cancels and cleans up the child. Stream output-limit failures
raise `SupervisorError` after cleanup.

## Persistent processes

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

## Cancellation

`CancellationToken` is a Python wrapper around the native cancellation token.
Pass it to `run()` or `stream()` and call `cancel()` from another task. A
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

## Errors and platform capabilities

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

## MCP server adapters

The `agent_supervisor.mcp` and `agent_supervisor.fastmcp` modules provide stdio
adapters for the official MCP Python SDK and Prefect FastMCP. Install the
matching extra with `python -m pip install 'agent-supervisor[mcp]'` or
`python -m pip install 'agent-supervisor[fastmcp]'`. See the [MCP guide](mcp.md)
for examples and configuration.
