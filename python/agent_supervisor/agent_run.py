"""Public Python API for a shared agent-run deadline and tool lifecycle."""

import asyncio
import inspect
import math
import time
from dataclasses import dataclass
from typing import Optional

from ._native import AgentRunHandle
from ._run_context import active_run


@dataclass(frozen=True)
class AgentRunPolicy:
    """Deadline and process cleanup settings for one agent invocation."""

    timeout: float = 300.0
    shutdown_grace: float = 5.0

    def __post_init__(self):
        if not isinstance(self.timeout, (int, float)) or not math.isfinite(self.timeout) or self.timeout <= 0:
            raise ValueError("timeout must be a finite positive number of seconds")
        if (
            not isinstance(self.shutdown_grace, (int, float))
            or not math.isfinite(self.shutdown_grace)
            or not 0 <= self.shutdown_grace <= 60
        ):
            raise ValueError("shutdown_grace must be between 0 and 60 seconds")
        object.__setattr__(self, "timeout", float(self.timeout))
        object.__setattr__(self, "shutdown_grace", float(self.shutdown_grace))


class AgentRun:
    """One agent call created and owned by an application ``Supervisor``."""

    def __init__(self, native, supervisor, policy):
        self.policy = policy
        self._native = native
        self._supervisor = supervisor
        self._deadline = time.monotonic() + policy.timeout

    async def run(self, operation):
        """Await a framework operation within this run's overall deadline.

        The operation's result or exception is passed through. Deadline expiry
        raises ``asyncio.TimeoutError``; run cancellation raises
        ``asyncio.CancelledError``. Both paths clean up registered tool
        processes before returning.
        """
        if not inspect.isawaitable(operation):
            raise TypeError("operation must be an awaitable")
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            await self.cancel()
            raise asyncio.TimeoutError("agent run exceeded its deadline")

        run_token = active_run.set(self)
        operation_task = asyncio.ensure_future(operation)
        cancellation_task = asyncio.ensure_future(self._native.wait_cancelled())
        try:
            done, _ = await asyncio.wait(
                (operation_task, cancellation_task),
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if operation_task in done:
                return await operation_task

            operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)
            deadline_expired = time.monotonic() >= self._deadline
            await self.cancel()
            if cancellation_task in done and not deadline_expired:
                raise asyncio.CancelledError
            raise asyncio.TimeoutError("agent run exceeded its deadline")
        except asyncio.CancelledError:
            if not operation_task.done():
                operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)
            await self.cancel()
            raise
        finally:
            if not cancellation_task.done():
                cancellation_task.cancel()
            await asyncio.gather(cancellation_task, return_exceptions=True)
            active_run.reset(run_token)

    async def run_command(self, command, *, policy=None, input=None):
        """Run one bounded local command within the remaining run deadline."""
        return await self._native.run(command, policy=policy, input=input)

    async def start(self, command, *, policy=None):
        """Start a persistent local process owned by this agent run."""
        return await self._native.start(command, policy=policy)

    @property
    def id(self):
        """This run's ID, as seen in ``SupervisorEvent.run_id``."""
        return self._native.id

    def publish_agent_event(self, event):
        """Show a framework event to watchers of this run.

        The object is passed through unchanged and never reaches the model.
        Framework adapters call this for you.
        """
        self._native.publish_agent_event(event)

    def cancellation_token(self):
        """Return a cloneable native token for framework cancellation APIs."""
        return self._native.cancellation_token()

    def request_cancel(self):
        """Signal cancellation; use ``await cancel()`` to finish cleanup."""
        self._native.cancel()

    async def cancel(self):
        """Request cancellation and wait for the configured process cleanup."""
        self._native.cancel()
        await self._native.wait_cleanup()

    def cancel_now(self):
        """Signal cancellation and immediately stop registered process groups."""
        self._native.cancel_now()

    def active_processes(self):
        """Return the number of persistent processes still registered."""
        return self._native.active_processes()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.cancel()
