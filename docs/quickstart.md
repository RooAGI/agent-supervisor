---
hide:
  - toc
---

# Quickstart

Keep the agent framework you already use. This walkthrough adds Agent
Supervisor one layer at a time, from your application down to the tool:

| Layer | What you add |
| --- | --- |
| **Application** | Create one `Supervisor` and shut it down at exit. |
| **Supervisor** | Pass it to the framework call; it owns the run and its tool processes. |
| **Agent** | Use the supervised `Runner` so the agent run has a deadline and cleanup. |
| **Tool** | Use `run_tool_command` where a tool starts a local command. |

The framework still runs the agent and makes model requests. Cancellation of
in-process agent code and model calls is cooperative; Agent Supervisor manages
the separate local tool processes it starts.

## Install

The framework adapter is included in the repository source. Install it with the
OpenAI Agents SDK extra:

```bash
git clone https://github.com/RooAGI/agent-supervisor.git
cd agent-supervisor/python
python -m pip install -e '.[openai]'
```

Set `OPENAI_API_KEY` before running the example. Set `PROJECT_WORKSPACE` to the
project directory whose tests the tool should run; it defaults to the current
directory.

## 1. Application: create the supervisor

Create one `Supervisor` in your application. It lives in your process and is
not a daemon.

```python
from agent_supervisor import Supervisor

supervisor = Supervisor()
```

## 2. Supervisor: pass it to the agent run

The supervisor owns each agent run and the tool processes started during it.
Pass it to the framework adapter with `supervisor=supervisor` (next step).

## 3. Agent: use the supervised `Runner`

If your app already imports `Runner` from the OpenAI SDK, make this one-line
change:

```diff
- from agents import Runner
+ from agent_supervisor.openai import Runner
```

```python
result = await Runner.run(agent, prompt, supervisor=supervisor)
```

The supervised `Runner` gives the agent run an overall deadline. When the run
ends, Agent Supervisor cleans up the tool processes it started. Keep `Agent`
and `function_tool` from `agents`.

## 4. Tool: run commands through Agent Supervisor

In the tool function, route the approved command through `run_tool_command`.
It applies limits to the local command and returns its result:

```python
result = await run_tool_command(Command(argv=argv))
```

## Complete example

The complete example adds a working directory, limits, an explicit network
policy, and shutdown:

```python
import asyncio
import os
import sys

from agents import Agent, function_tool
from agent_supervisor import Command, Policy, Supervisor, run as run_tool_command
from agent_supervisor.openai import Runner  # Replaces `from agents import Runner`.

WORKSPACE = os.path.abspath(os.environ.get("PROJECT_WORKSPACE", "."))


@function_tool
async def run_project_tests() -> str:
    """Run the project's tests and return a bounded summary."""
    result = await run_tool_command(
        Command(
            argv=[sys.executable, "-m", "pytest", "-q"],
            cwd=WORKSPACE,
            timeout=120,
            max_stdout_bytes=8192,
            max_stderr_bytes=4096,
        ),
        policy=Policy(network="disabled"),
    )
    output = (result.stdout + b"\n" + result.stderr).decode(
        "utf-8", errors="replace"
    )
    return f"{result.termination}; exit={result.exit_code}\n{output}"


async def main() -> None:
    agent = Agent(
        name="Project helper",
        instructions="Run the project tests when asked and summarize the result.",
        tools=[run_project_tests],
    )
    supervisor = Supervisor()
    result = await Runner.run(
        agent, "Run the project tests", supervisor=supervisor
    )
    print(result.final_output)
    # Stop any remaining runs and jobs before the application exits.
    await supervisor.shutdown_all()


asyncio.run(main())
```

The example gives the agent one test-running tool. Agent Supervisor sets the
agent-run deadline to five minutes by default; each tool command has its own
120-second timeout and output limits. `Policy(network="disabled")` requests
network denial where the operating system supports it. Configure filesystem
access explicitly if the command needs narrower access; defaults do not create
a complete sandbox.

## Tool runs: one-shot and background

A tool can start a local command in two ways. Choose by how long the process
should live.

### One-shot tool run

Use `run_tool_command` inside a tool function for a command that finishes
while the agent waits. It applies the command's timeout and output limits, then
returns a bounded result. The process belongs to the active agent run, so it is
stopped if the run ends or is cancelled first.

```python
import shutil

from agent_supervisor import Command, Policy, run as run_tool_command

result = await run_tool_command(
    Command(
        argv=[shutil.which("git"), "status", "--short"],  # Must be an absolute path.
        cwd=WORKSPACE,
        timeout=30,
        max_stdout_bytes=4096,
    ),
    policy=Policy(network="disabled"),
)
print(result.termination, result.exit_code, result.stdout.decode())
```

### Background tool run

Use `start_background` for a process that should keep running after the tool
call returns, such as a dev server or a long build. The `Supervisor` owns it,
not the agent run, so it continues between agent runs. You refer to it by its
`SupervisorProcessId`:

```python
job_id = await supervisor.start_background(
    Command(
        argv=[shutil.which("npm"), "run", "dev"],
        cwd=WORKSPACE,
        inherit_env=["PATH", "HOME"],  # The child's environment starts empty.
    ),
    policy=Policy(network="host"),
)

supervisor.process_status(job_id)       # "running", "completed", "failed", or None
await supervisor.write_process_stdin(job_id, b"reload\n")   # optional input

# Later, from any tool call or a new agent run on the same Supervisor:
await supervisor.shutdown_process(job_id, grace=2.0)   # stop just this job
result = await supervisor.wait_process(job_id)         # final RunResult
print(result.termination, result.exit_code)
```

`Command.timeout` does not end a background job; it runs until it exits or you
stop it. It can write any amount of output, and its result keeps the newest
bytes up to the command's byte limits. `shutdown_all()` stops any jobs still
running when the application exits. See the
[Python API](python-api.md#supervisor-manager) for every job method.

### Watch it live

`Supervisor.stream()` watches work without affecting it. Use it for one job, or
for a whole agent run, which also shows the agent's own events:

```python
async for event in supervisor.stream(process=job_id):
    if event.kind == "stdout":
        print(event.data.decode("utf-8", errors="replace"), end="")

result = await Runner.run_streamed(agent, prompt, supervisor=supervisor)
async for event in supervisor.stream(run=result.agent_run):
    print(event.kind)   # agent, stdout, exited, run_ended, ...
```

See [Stream output](python-api.md#stream-output).

## Start the run yourself

The `Runner` adapter calls `start_agent_run()` for you. For a framework without
an adapter, or when you need the run object, create it directly. Run the
framework call through it, and the run applies the deadline and cleans up tool
processes when the call ends or is cancelled:

```python
from agent_supervisor import AgentRunPolicy

run = supervisor.start_agent_run(AgentRunPolicy(timeout=180, shutdown_grace=3))

async def run_agent():
    # Your existing framework call. Tools started with `run_tool_command`
    # while it runs join this run.
    return await agent.run("Summarize the project")

async with run:
    result = await run.run(run_agent())
```

## Resume after a pause

Agent Supervisor does not store or restore framework state. To resume:

1. Save the framework's own session or checkpoint in your application store.
2. If a background job should continue, start it with
   `supervisor.start_background(command)` and save `process_id.token` beside
   the checkpoint.
3. When the workflow resumes, load the checkpoint and start a **new**
   `AgentRun` with `supervisor.start_agent_run()`.
4. Look the job up with `SupervisorProcessId.from_token(token)` and
   `supervisor.process_status(process_id)`, using the same live `Supervisor`.

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

If lookup returns `None`, the job is gone, for example after an application
restart. Recover from the checkpoint; do not replay the old command, because it
may already have had side effects. The complete resume example is in the
[Python API](python-api.md#save-and-resume-background-jobs) and the
[Rust API](rust-api.md#save-and-resume-background-jobs).

## Next steps

- [Core concepts](api-reference.md): how commands, policies, run results, and
  process events fit together.
- [Python API](python-api.md): Python arguments, return values, streaming, and
  persistent processes.
- [Rust API](rust-api.md): Rust types, methods, and examples.
- [Use with agent SDKs](api-boundaries.md): examples for OpenAI Agents SDK,
  Microsoft Agent Framework, LangGraph, PydanticAI, and Google ADK.
- [MCP servers](mcp.md): supervise an existing Rust or Python stdio MCP server.
