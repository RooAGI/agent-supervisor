# Python API

This document describes the first Python interface for `agent-supervisor`.
The source binding is implemented in `python/`, but it has not been published
to PyPI. It wraps the native backend and preserves the Rust library's process
cleanup and policy semantics.

## Goals

- Give Python applications a safe argv-based way to run native tool processes.
- Keep output bounded and apply a deadline to every invocation.
- Return a structured result that says how execution ended and what enforcement
  the backend actually provided.
- Make asynchronous Python applications the primary integration path.
- Reject policy requests the active platform cannot enforce.

The library remains a process supervisor. Authentication, agent identity,
authorization decisions, tool schemas, and provider credentials stay in the
calling application.

## Package and module

The distribution will be named `agent-supervisor` and imported as
`agent_supervisor`. Build the native extension with PyO3 and maturin from the
repository checkout. The package has not been published to PyPI yet; platform
wheels and a source distribution can be published after native CI covers the
supported operating systems. Prefer PyPI Trusted Publishing from GitHub Actions
over a long-lived upload token.

The Python package is versioned with the native crate. PyPI publication is a
separate release step and has not happened yet.

## First release: one-shot async execution

```python
from agent_supervisor import Command, Policy, run

result = await run(
    Command(
        argv=["/usr/bin/rg", "TODO", "src"],
        cwd="/workspace/project",
        timeout=10.0,
        max_stdout_bytes=256 * 1024,
        max_stderr_bytes=32 * 1024,
        env={"LANG": "C.UTF-8"},
    ),
    policy=Policy(network="host"),
    input=b"",
)

print(result.stdout.decode("utf-8", errors="replace"))
print(result.termination, result.exit_code, result.enforcement)
```

`Command.argv` is a non-empty sequence of strings. The first item must be an
absolute executable path; arguments are passed directly to the operating
system, and the library never inserts a shell. `cwd` and executable paths are
resolved and validated according to the native API. `env` means the explicit
environment additions/replacements; ambient
inheritance remains denied by default. An explicit `inherit_env` allowlist can
be offered for callers that need selected host variables.

Implemented `Command` options:

| Option | Meaning |
|---|---|
| `argv` | Executable and arguments, with no implicit shell |
| `cwd` | Optional working directory |
| `env` | Explicit environment variables to release to the child |
| `inherit_env` | Optional names to copy from the host environment |
| `timeout` | Positive deadline in seconds |
| `max_stdin_bytes` | Maximum input accepted by `run(input=...)` |
| `max_stdout_bytes` | Bound for collected stdout |
| `max_stderr_bytes` | Bound for collected stderr |

`Policy` maps to the current Rust policy: `network`, optional filesystem grants,
and `enforcement` (`best_effort` or `required`). Resource limits are exposed
only where the corresponding backend capability exists.

## Result and error contract

`run()` returns a `RunResult` when the child ran to completion, hit its
deadline, was cancelled through an explicit `CancellationToken`, or exceeded
an output limit. It raises a structured
`SupervisorError` for invalid requests, unavailable required enforcement,
spawn/I/O failures, and other operational errors.

`RunResult` should expose:

- `stdout` and `stderr` as bounded `bytes`;
- `success` as a convenience derived from a zero exit;
- `exit_code` and `signal`, when available;
- `termination` as one of `exited`, `timed_out`, `cancelled`,
  `stdout_limit_exceeded`, or `stderr_limit_exceeded`;
- `enforcement` (`enforced`, `degraded`, or `trusted`);
- requested and canonical executable identity; and
- monotonic duration plus wall-clock start/finish timestamps.

Do not make `stdout` implicitly decode to text. A helper such as
`result.stdout_text(encoding="utf-8", errors="replace")` and the matching
`stderr_text()` helper are provided.

## Streaming output

`stream()` yields bounded byte chunks as they arrive. Set `capture=True` to
also collect those chunks, within the same stdout and stderr limits, and
include them in the terminal event's `result`. Streaming without capture keeps
the binding from accumulating a second copy of the output.

```python
from agent_supervisor import Command, Policy, stream

async with stream(
    Command(
        argv=["/usr/bin/tool", "--verbose"],
        timeout=30.0,
        max_stdout_bytes=512 * 1024,
        max_stderr_bytes=64 * 1024,
    ),
    policy=Policy(network="host"),
    capture=True,
) as events:
    async for event in events:
        if event.kind == "stdout":
            print(event.data.decode("utf-8", errors="replace"), end="")
        elif event.kind == "stderr":
            report(event.data)
        elif event.kind == "exited":
            result = event.result
```

`stream()` is for output streaming from a command that does not need stdin; it
closes the child's stdin immediately. Use `run(..., input=...)` for bounded
one-shot input. Interactive stdin and persistent process handles remain a
later phase. If a consumer pauses, output queues apply backpressure while the
deadline and explicit cancellation remain active. Dropping the stream closes
its receiver and triggers child cleanup. An output-limit breach raises
`SupervisorError` after cleanup.

Use the async context manager even when consuming only part of the stream.
Leaving the context calls `aclose()` and waits for process cleanup. Cancelling
the task also exits the context and performs that cleanup before cancellation
propagates.

`SupervisorError` retains stable machine-readable fields matching the
Rust error: `code`, `phase`, `message`, `retryable`, optional `executable`,
optional `termination`, and optional bounded `stderr_tail`. Python exception
callers should branch on
`code`, not parse the message.

## Async cancellation

Expose a small `CancellationToken` with `cancel()` and `cancelled()`. Passing a
token to `run(..., cancellation=token)` and calling `token.cancel()` returns a
normal `RunResult` with `termination="cancelled"` after cleanup and reaping.
Cancelling the Python task itself must instead terminate the owned process
group, wait for cleanup/reaping, then propagate `asyncio.CancelledError`. The
binding must not simply drop the Rust future or stop reading output. If callers
need a termination receipt they can inspect, use the explicit cancellation
token or the process-handle API's `terminate()` method.

## Later phase: persistent process handle and interactive streaming

Persistent processes should have an explicit async context manager so ordinary
scope exit cleans up the child:

```python
async with await supervisor.spawn(
    Command(argv=["/usr/bin/my-tool", "--interactive"])
) as process:
    await process.write_stdin(b"request\n")
    await process.close_stdin()

    async for event in process.events():
        if event.kind == "stdout":
            consume(event.data)
        elif event.kind == "stderr":
            report(event.data)
        elif event.kind == "exited":
            receipt = event.receipt

    result = await process.wait()
```

The handle should also offer `terminate(grace=...) -> ProcessReceipt`,
`wait() -> RunResult`, and an idempotent `close()`. Output event payloads are
bytes and share the configured output bounds. Ending an event iterator does not
mean the child was terminated; termination must be explicit or happen through
the context manager's cleanup.

## Scope boundaries for v1

- Start with one-shot execution and output streaming; add persistent handles
  after cancellation, stream backpressure, and cleanup behavior are verified
  on Linux, macOS, and Windows.
- Do not initially expose supervision restart policies, process adoption,
  pipelines, or PTYs. Add these after their ownership and error models have
  idiomatic Python designs.
- Do not expose OpenShell as if it were a production Python backend. Its current
  adapter is developer-only and command execution in an existing managed
  sandbox has distinct cancellation and policy limits.
- Do not silently downgrade required filesystem, network, or resource
  enforcement. Return a structured error when the selected platform cannot
  satisfy it.

## Implementation outline

1. Add a `python/` package built with maturin and an ABI strategy chosen for the
   supported Python versions.
2. Bind the stable request/result/error types and async `run()` first.
3. Add Python unit tests for request conversion, error conversion, byte output,
   and cancellation cleanup, plus native subprocess integration tests per OS.
4. Build and inspect wheels for Linux, macOS, and Windows in CI; test a source
   install as well.
5. Publish to PyPI through a configured Trusted Publisher only after the API
   and platform matrix are green.
