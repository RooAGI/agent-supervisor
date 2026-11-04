---
hide:
  - toc
---

# Quickstart

Keep the agent framework you already use. Put Agent Supervisor around the
agent run, then use it inside the tool function to manage the local command.

## Add supervision to your agent app

You can keep your agent and its instructions as they are. Change two lines in
your app: use Agent Supervisor's `Runner` to start the agent, and use
`run_tool_command` where your tool starts a local command.

The supervised `Runner` gives the agent run an overall deadline. The tool
function's `run_tool_command` call applies limits to the local command and
returns its result. When the agent run ends, Agent Supervisor cleans up the
tool processes it started.

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

## One example

If your app already imports `Runner` from the OpenAI SDK, make this one-line
change:

```diff
- from agents import Runner
+ from agent_supervisor.openai import Runner
```

Keep `Agent` and `function_tool` from `agents`. In the tool function, run the
approved command through Agent Supervisor. The tool-side change is to route
the command through `run_tool_command`:

```python
result = await run_tool_command(Command(argv=argv))
```

The complete example below adds a working directory, limits, and an explicit
network policy:

```python
import asyncio
import os
import sys

from agents import Agent, function_tool
from agent_supervisor import Command, Policy, run as run_tool_command
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
    result = await Runner.run(agent, "Run the project tests")
    print(result.final_output)


asyncio.run(main())
```

The example gives the agent one test-running tool. Agent Supervisor sets the
agent-run deadline to five minutes by default; each tool command has its own
120-second timeout and output limits. `Policy(network="disabled")` requests
network denial where the operating system supports it. Configure filesystem
access explicitly if the command needs narrower access; defaults do not create
a complete sandbox.

## Next steps

- [Core Concept](api-reference.md): how commands, policies, run results, and
  process events fit together.
- [Python API](python-api.md): Python arguments, return values, streaming, and
  persistent processes.
- [Rust API](rust-api.md): Rust types, methods, and examples.
- [Use with agent SDKs](api-boundaries.md): examples for OpenAI Agents SDK,
  Microsoft Agent Framework, LangGraph, PydanticAI, and Google ADK.
- [MCP servers](mcp.md): supervise an existing Rust or Python stdio MCP server.
