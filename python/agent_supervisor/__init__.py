"""Python bindings for agent-supervisor."""

import asyncio

from ._native import (
    CancellationToken,
    Command,
    OutputEvent,
    Policy,
    Process,
    RunResult,
    SupervisorProcessId,
    SupervisorError,
    SupervisorEvent,
    SupervisorHandle,
    platform_capabilities,
    run as _run_native,
)
from .agent_run import AgentRun, AgentRunPolicy


class Supervisor:
    """Own agent runs and local child processes for the application.

    Use ``start_agent_run`` to create a deadline and cleanup boundary for one
    framework invocation. The supervisor also owns background jobs that can
    continue between agent runs.
    """

    def __init__(self):
        self._native = SupervisorHandle()

    def start_agent_run(self, policy=None):
        """Create one agent run owned by this supervisor."""
        if policy is None:
            policy = AgentRunPolicy()
        if not isinstance(policy, AgentRunPolicy):
            raise TypeError("policy must be an AgentRunPolicy")
        native = self._native.start_agent_run(policy.timeout, policy.shutdown_grace)
        return AgentRun(native, self, policy)

    def active_agent_runs(self):
        """Return the number of agent runs currently owned by this supervisor."""
        return self._native.active_agent_runs()

    def active_processes(self):
        """Return the number of child processes registered with this supervisor."""
        return self._native.active_processes()

    async def start(self, command, *, policy=None):
        """Start a persistent child and return its interactive Process handle."""
        run = _current_run_context()
        if run is not None:
            return await run.start(command, policy=policy)
        return await self._native.start(command, policy=policy)

    def stream(self, *, run=None, process=None):
        """Watch this supervisor's work as one ordered stream of events.

        ``async for event in supervisor.stream(...)``. This only observes:
        launch work with :func:`run`, :meth:`start`, :meth:`start_background`,
        or a framework adapter. Stopping the watcher stops nothing, and a slow
        watcher never slows a process.

        - ``stream()`` watches everything, until the supervisor is dropped.
        - ``stream(run=run)`` watches one agent run, including its framework
          events, and ends when the run ends.
        - ``stream(process=process_id)`` watches one process or background job
          and ends when it exits.

        A watcher starts with the oldest retained event, so opening one just
        after starting work does not miss it.
        """
        native_run = run._native if run is not None else None
        return self._native.stream(run=native_run, process=process)

    async def start_background(self, command, *, policy=None):
        """Start a manager-owned job and return its stable SupervisorProcessId.

        Persistent jobs are stopped explicitly with ``shutdown_process`` or
        ``shutdown_all``; ``Command.timeout`` does not end a background job.
        The command's stdout and stderr byte limits still apply.
        """
        return await self._native.start_background(command, policy=policy)

    def process_status(self, process_id):
        """Return ``running``, ``completed``, ``failed``, or ``None`` if unknown."""
        return self._native.process_status(process_id)

    def process_result(self, process_id):
        """Return a completed RunResult snapshot, or ``None`` while active/unknown."""
        return self._native.process_result(process_id)

    async def wait_process(self, process_id):
        """Wait for a background job; return ``None`` if its ID is unknown."""
        return await self._native.wait_process(process_id)

    def take_process_result(self, process_id):
        """Remove and return a completed result, or ``None`` if not completed."""
        return self._native.take_process_result(process_id)

    def forget_process(self, process_id):
        """Forget a completed/failed result. Active jobs cannot be forgotten."""
        return self._native.forget_process(process_id)

    async def write_process_stdin(self, process_id, data):
        """Write bytes to a managed job's stdin."""
        return await self._native.write_process_stdin(process_id, data)

    async def close_process_stdin(self, process_id):
        """Close a managed job's stdin and deliver EOF."""
        return await self._native.close_process_stdin(process_id)

    async def shutdown_process(self, process_id, *, grace=5.0):
        """Stop one process tree; return the stop request outcome string."""
        return await self._native.shutdown_process(process_id, grace=grace)

    async def shutdown_all(self, *, grace=5.0):
        """Stop all jobs and return ``[(process_id, RunResult), ...]`` results."""
        return await self._native.shutdown_all(grace=grace)


def _current_run_context():
    from ._run_context import current_run
    return current_run()


async def run(command, *, policy=None, input=None, cancellation=None):
    """Run one command, collecting bounded stdout and stderr bytes."""
    token = cancellation if cancellation is not None else CancellationToken()
    run = _current_run_context()
    operation = asyncio.ensure_future(
        run.run_command(command, policy=policy, input=input) if run is not None
        else _run_native(command, policy=policy, input=input, cancellation=token)
    )
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        if run is not None:
            await run.cancel()
        else:
            token.cancel()
        try:
            await asyncio.shield(operation)
        except SupervisorError:
            pass
        raise


__all__ = [
    "CancellationToken",
    "AgentRunPolicy",
    "AgentRun",
    "Command",
    "OutputEvent",
    "Policy",
    "Process",
    "RunResult",
    "SupervisorProcessId",
    "SupervisorError",
    "SupervisorEvent",
    "Supervisor",
    "platform_capabilities",
    "run",
]
