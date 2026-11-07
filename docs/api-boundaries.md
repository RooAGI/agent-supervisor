---
hide:
  - toc
---

# Connect Agent Supervisor to an agent SDK

Your application creates one top-level `Supervisor`. It owns each `AgentRun`
and the local processes started by that run. The agent framework still runs
the agent, calls the model, and chooses tools. Your tool functions check
authorization and start approved local processes through Agent Supervisor.

```text
Application → Supervisor → AgentRun → agent framework → tool function → local process
```

Pass the same `supervisor=supervisor` argument to framework adapters across
calls when you want the application manager to own and shut down those runs
together. Each `AgentRun` still cleans up only the processes started for its
own invocation.

The examples below each define their own tool function. There is no shared
tool module: copy the function into the tool adapter for the SDK you use, then
register that function with your agent.

## Supervise one run from start to finish

Agent Supervisor follows a **run**, meaning one invocation of an agent
framework—not the lifetime of an `Agent` object and not a separate agent
process. The framework continues to run the agent and make model requests.
Agent Supervisor provides the lifecycle around that invocation:

1. **Start:** `Supervisor` creates an `AgentRun` and starts its deadline.
2. **Run:** let the framework execute its turns, handoffs, and tool calls.
   Tool functions start local commands through the run, and `Supervisor`
   tracks those processes under that `AgentRun`.
3. **End:** observe normal completion, an error, caller cancellation, or the
   deadline.
4. **Clean up:** on cancellation or deadline, request cancellation from the
   framework run. Then allow tracked tool processes a shutdown grace period
   and stop any that remain. Return or propagate the framework's outcome.

At a deadline, cancellation is cooperative: it does not pause or forcibly kill
agent code running in your application process. The framework and app code
must respond to cancellation. Agent Supervisor can stop the separate local
processes started through its APIs.

Python exposes `Supervisor.start_agent_run()` and `AgentRun` for custom
integrations. The OpenAI Agents SDK, Microsoft Agent Framework, LangGraph,
PydanticAI, and Google ADK adapters accept the top-level `supervisor` and
create an `AgentRun` through it.
All five adapters use a 300 second deadline and a 5 second shutdown grace
period by default. Override them per call with
`supervision={"timeout": 180, "shutdown_grace": 3}`.

## Install

The framework run adapters are in the current 0.3.0 repository source. Install
the adapter from a checkout as shown in its section below. The table lists
each framework's SDK dependency:

| SDK | Install |
|---|---|
| OpenAI Agents SDK | `python -m pip install openai-agents` |
| LangGraph | `python -m pip install langgraph langchain` |
| PydanticAI | `python -m pip install pydantic-ai` |
| Google ADK | `python -m pip install google-adk` |
| Microsoft Agent Framework | `python -m pip install agent-framework-openai` |

From a repository checkout, install Agent Supervisor and the matching adapter
with `cd python && python -m pip install -e '.[langgraph]'` (or replace
`langgraph` with `openai`, `pydantic-ai`, `google-adk`, or `microsoft`).

The examples run `pytest -q` in a project directory. Set
`PROJECT_WORKSPACE` to that directory before starting your application. Adjust
the argument list and policy to match the one command your application has
authorized.

## OpenAI Runner: add run-level supervision

If your app already calls the OpenAI Agents SDK `Runner`, install the optional
adapter and change the `Runner` import:

From a checkout of the current repository source, install the adapter and SDK:

```bash
cd python
python -m pip install -e '.[openai]'
```

Replace this one import line:

```diff
- from agents import Runner
+ from agent_supervisor.openai import Runner
```

Keep your other OpenAI imports, such as `from agents import Agent`, unchanged.

Keep the rest of the `Agent` and `Runner.run(...)` code as it is. The adapter
delegates the agent loop to the official OpenAI Runner and adds a 300 second
default deadline with a 5 second cleanup grace period. Configure these per
run with `supervision={"timeout": 120, "shutdown_grace": 2}` and pass the
application's manager as `supervisor=supervisor`.

The adapter manages commands started through `agent_supervisor.run()` or
`Supervisor.start()` while the agent run is active. It stops those local processes when the run finishes, times out, or is
cancelled. The OpenAI agent loop and model calls remain in your Python process;
this adapter does not intercept arbitrary subprocess calls, Python code, or
network requests made directly by your tools. Use the command APIs and their
`Policy` for those controls.

## OpenAI Agents SDK

Register the decorated function with an `Agent`. `Runner.run` owns the agent
loop; the tool function calls Agent Supervisor when the model selects it.

```python
import os
import sys

from agent_supervisor import Command, Policy, Supervisor, run
from agents import Agent
from agent_supervisor.openai import Runner
from agents.decorators import tool

WORKSPACE = os.path.abspath(os.environ.get("PROJECT_WORKSPACE", "."))


@tool
async def run_project_tests() -> str:
    """Run the approved test command in the project workspace."""
    result = await run(
        Command(
            argv=[sys.executable, "-m", "pytest", "-q"],
            cwd=WORKSPACE,
            env={"PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},
            timeout=60.0,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=16 * 1024,
        ),
        policy=Policy(network="host"),
    )
    output = (result.stdout + b"\n" + result.stderr).decode(
        "utf-8", errors="replace"
    )
    return f"Finished: {result.termination}; exit code: {result.exit_code}\n{output}"


agent = Agent(
    name="Project helper",
    instructions="Run the project tests when asked.",
    tools=[run_project_tests],
)

# In your async application:
# supervisor = Supervisor()
# result = await Runner.run(agent, "Run the project tests", supervisor=supervisor)
```

## LangGraph

LangGraph exposes Python functions as tools and provides a `ToolNode` to run
them. Connect this node to the model node in your graph.

```python
import os
import sys

from agent_supervisor import Command, Policy, Supervisor, run
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode

WORKSPACE = os.path.abspath(os.environ.get("PROJECT_WORKSPACE", "."))


@tool
async def run_project_tests() -> str:
    """Run the approved test command in the project workspace."""
    result = await run(
        Command(
            argv=[sys.executable, "-m", "pytest", "-q"],
            cwd=WORKSPACE,
            env={"PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},
            timeout=60.0,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=16 * 1024,
        ),
        policy=Policy(network="host"),
    )
    output = (result.stdout + b"\n" + result.stderr).decode(
        "utf-8", errors="replace"
    )
    return f"Finished: {result.termination}; exit code: {result.exit_code}\n{output}"


tools = [run_project_tests]
tool_node = ToolNode(tools)
# Add tool_node to your StateGraph and connect the model's tool calls to it.

# After compiling the graph, wrap its normal async invocation:
from agent_supervisor.langgraph import run as run_graph

supervisor = Supervisor()
result = await run_graph(
    graph,
    {"messages": [{"role": "user", "content": "Run the project tests."}]},
    supervisor=supervisor,
    supervision={"timeout": 180, "shutdown_grace": 3},
)
```

For graph streaming, use the companion `stream` adapter. Closing the iterator
also closes the `AgentRun`:

```python
from agent_supervisor.langgraph import stream as stream_graph

async with stream_graph(graph, inputs) as updates:
    async for update in updates:
        print(update)
```

## PydanticAI

Pass the async function to `Agent` as a tool. PydanticAI validates the tool
arguments and calls it as part of its agent loop.

```python
import os
import sys

from agent_supervisor import Command, Policy, Supervisor, run
from pydantic_ai import Agent

WORKSPACE = os.path.abspath(os.environ.get("PROJECT_WORKSPACE", "."))


async def run_project_tests() -> str:
    """Run the approved test command in the project workspace."""
    result = await run(
        Command(
            argv=[sys.executable, "-m", "pytest", "-q"],
            cwd=WORKSPACE,
            env={"PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},
            timeout=60.0,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=16 * 1024,
        ),
        policy=Policy(network="host"),
    )
    output = (result.stdout + b"\n" + result.stderr).decode(
        "utf-8", errors="replace"
    )
    return f"Finished: {result.termination}; exit code: {result.exit_code}\n{output}"


agent = Agent(
    "google:gemini-2.5-flash",
    instructions="Run the project tests when asked.",
    tools=[run_project_tests],
)

# Replace await agent.run(...) with the supervised adapter:
from agent_supervisor.pydantic_ai import run as run_agent

supervisor = Supervisor()
result = await run_agent(
    agent, "Run the project tests.",
    supervisor=supervisor,
    supervision={"timeout": 180, "shutdown_grace": 3},
)
```

For streaming, keep PydanticAI's streamed result inside the supervised context:

```python
from agent_supervisor.pydantic_ai import run_stream as stream_agent

async with stream_agent(agent, "Summarize the test results.") as response:
    async for text in response.stream_text():
        print(text, end="")
```

## Google ADK

ADK accepts a Python function as an `LlmAgent` tool. Keep the tool function in
your application so it can apply your authorization rules before execution.

```python
import os
import sys

from agent_supervisor import Command, Policy, Supervisor, run
from google.adk.agents import LlmAgent

WORKSPACE = os.path.abspath(os.environ.get("PROJECT_WORKSPACE", "."))


async def run_project_tests() -> str:
    """Run the approved test command in the project workspace."""
    result = await run(
        Command(
            argv=[sys.executable, "-m", "pytest", "-q"],
            cwd=WORKSPACE,
            env={"PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},
            timeout=60.0,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=16 * 1024,
        ),
        policy=Policy(network="host"),
    )
    output = (result.stdout + b"\n" + result.stderr).decode(
        "utf-8", errors="replace"
    )
    return f"Finished: {result.termination}; exit code: {result.exit_code}\n{output}"


agent = LlmAgent(
    name="project_helper",
    model="gemini-flash-latest",
    instruction="Run the project tests when asked.",
    tools=[run_project_tests],
)

# Wrap ADK Runner's normal run_async event stream:
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types
from agent_supervisor.google_adk import run as run_adk

session_service = InMemorySessionService()
await session_service.create_session(
    app_name="project_helper", user_id="user", session_id="session"
)
runner = Runner(agent=agent, app_name="project_helper", session_service=session_service)
async for event in run_adk(
    runner,
    user_id="user",
    session_id="session",
    new_message=types.Content(role="user", parts=[types.Part(text="Run the tests.")]),
    supervision={"timeout": 180, "shutdown_grace": 3},
):
    if event.is_final_response():
        print(event)
```

ADK's adapter supplies an `asyncio.Event` as `abort_signal` unless you pass
one. Closing or timing out the event stream sets the signal and cleans up
Agent Supervisor tool processes.

## Microsoft Agent Framework

Install the optional adapter and Microsoft OpenAI provider (Python 3.10+):

From a checkout of the current repository source, install the adapter and
Microsoft OpenAI provider:

```bash
cd python
python -m pip install -e '.[microsoft]'
```

Wrap one framework invocation with `agent_supervisor.microsoft.run`. Pass the
application's `Supervisor` so it owns the run. Keep your existing Microsoft
`Agent`, tools, and model client. The timeout and cleanup
grace are 300 and 5 seconds by default. A timed out invocation receives
cooperative asyncio cancellation; local commands started by its tools through
`agent_supervisor` are cleaned up by the active `AgentRun`.

The Microsoft Agent Framework also accepts a Python function as a tool. Add
the function to your `Agent`; the framework handles model interaction and
tool selection.

```python
import os
import sys

from agent_supervisor import Command, Policy, Supervisor, run
from agent_supervisor.microsoft import run as run_supervised_agent
from agent_framework import Agent
from agent_framework.openai import OpenAIChatClient

WORKSPACE = os.path.abspath(os.environ.get("PROJECT_WORKSPACE", "."))


async def run_project_tests() -> str:
    """Run the approved test command in the project workspace."""
    result = await run(
        Command(
            argv=[sys.executable, "-m", "pytest", "-q"],
            cwd=WORKSPACE,
            env={"PYTHONPATH": os.pathsep.join(p for p in sys.path if p)},
            timeout=60.0,
            max_stdout_bytes=64 * 1024,
            max_stderr_bytes=16 * 1024,
        ),
        policy=Policy(network="host"),
    )
    output = (result.stdout + b"\n" + result.stderr).decode(
        "utf-8", errors="replace"
    )
    return f"Finished: {result.termination}; exit code: {result.exit_code}\n{output}"


client = OpenAIChatClient(model="gpt-4o-mini")
agent = Agent(
    client=client,
    name="Project helper",
    instructions="Run the project tests when asked.",
    tools=[run_project_tests],
)

# Run the Microsoft agent under the application's Supervisor.
supervisor = Supervisor()
result = await run_supervised_agent(
    agent,
    "Run the project tests and report the result.",
    supervisor=supervisor,
    supervision={"timeout": 180, "shutdown_grace": 3},
)
print(result.text)
```

This `AgentRun` supervises the **agent call inside the application process** and
processes started through Agent Supervisor's tool APIs. It does not put the
agent code or model API calls in a separate OS process. For actual process
isolation of the agent runtime, the agent framework and its client must run
inside a separately launched worker process.

## What this example does and does not isolate

Each function above sends a fixed executable and argument list to
`agent_supervisor`; the model cannot supply an arbitrary shell command. The
example uses `Policy(network="host")` and does not grant or restrict
filesystem access, so it does not isolate the command from the host. Add an
authorization check, choose a policy that matches the platform controls you
need, and check the returned enforcement details. See the
[security model](security-model.md) before relying on any particular
filesystem, network, or resource guarantee.

For the full Python request and result API, see [Python API](python-api.md).
For the Rust integration, start with the [quickstart](quickstart.md).
