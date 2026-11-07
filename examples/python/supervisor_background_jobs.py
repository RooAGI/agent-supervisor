"""Run this from the repository root after installing the Python package."""

import asyncio
import sys

from agent_supervisor import Command, Policy, Supervisor


def python_job(code: str) -> Command:
    return Command([sys.executable, "-u", "-c", code], timeout=30)


async def main() -> None:
    supervisor = Supervisor()
    policy = Policy()

    # start_background needs the extension's Tokio runtime. The supervisor
    # owns output draining and final results for each stable process ID.
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

    # Python Command.timeout applies to run(); persistent jobs run
    # until explicitly stopped. Command.max_stdout_bytes/max_stderr_bytes
    # still bound the output retained by the manager.
    report = await supervisor.wait_process(report_id)
    print("report:", report.stdout.decode("utf-8", errors="replace").rstrip())
    print("report exit:", report.termination, report.exit_code)

    stop_outcome = await supervisor.shutdown_process(stop_id, grace=0.5)
    print("stop request:", stop_outcome)
    stopped = await supervisor.wait_process(stop_id)
    print("stopped result:", stopped.termination, stopped.exit_code, stopped.signal)
    sibling_status = supervisor.process_status(sibling_id)
    assert sibling_status == "running"
    print("sibling status:", sibling_status)

    # Await this before the host's Tokio runtime exits. On Windows the backend
    # force-stops immediately because graceful process signals are unavailable.
    for process_id, result in await supervisor.shutdown_all(grace=1.0):
        print(int(process_id), result.termination, result.exit_code)

    # Dropping the Supervisor is a process-tree cleanup fallback. If the app
    # exits abruptly, the host process owns this supervisor; it is not a daemon.


if __name__ == "__main__":
    asyncio.run(main())
