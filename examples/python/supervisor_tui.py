"""Run from the repository root after installing the Python package."""

import asyncio
import os
import sys
from pathlib import Path

from agent_supervisor import Command, Policy, Supervisor


def long_job() -> Command:
    return Command([sys.executable, "-u", "-c", "import time; time.sleep(120)"])


async def main() -> None:
    tui_path = os.environ.get("AGENT_SUPERVISOR_TUI")
    if not tui_path:
        raise RuntimeError("set AGENT_SUPERVISOR_TUI to the TUI executable path")

    supervisor = Supervisor()
    policy = Policy()
    openai_id = await supervisor.start_background(
        long_job(),
        policy=policy,
        framework="OpenAI Agents SDK",
        run_label="support-agent",
    )
    langgraph_id = await supervisor.start_background(
        long_job(),
        policy=policy,
        framework="LangGraph",
        run_label="research-agent",
    )

    await supervisor.start_tui(Path(tui_path))
    print("Select one labeled job in the TUI, press s, then type y to stop it.")

    deadline = asyncio.get_running_loop().time() + 120
    stopped_id = None
    while asyncio.get_running_loop().time() < deadline:
        if supervisor.process_status(openai_id) != "running":
            stopped_id = openai_id
            break
        if supervisor.process_status(langgraph_id) != "running":
            stopped_id = langgraph_id
            break
        await asyncio.sleep(0.1)

    if stopped_id is None:
        raise TimeoutError("no managed process was stopped from the TUI")

    sibling_id = langgraph_id if stopped_id == openai_id else openai_id
    result = await supervisor.wait_process(stopped_id)
    print("stopped result:", result.termination, result.exit_code)
    sibling_status = supervisor.process_status(sibling_id)
    assert sibling_status == "running", "stopping one job must leave its sibling running"
    print("sibling status:", sibling_status)

    for process_id, final in await supervisor.shutdown_all(grace=1.0):
        print(int(process_id), final.termination, final.exit_code)


if __name__ == "__main__":
    asyncio.run(main())
