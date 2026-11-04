"""Python bindings for agent-supervisor."""

import asyncio
from contextlib import asynccontextmanager

from ._native import (
    CancellationToken,
    Command,
    OutputEvent,
    Policy,
    Process,
    RunResult,
    SupervisorError,
    SupervisorHandle,
    platform_capabilities,
    run as _run_native,
    stream as _stream_native,
)
from .agent_run import AgentRunPolicy, AgentRunScope


class Supervisor:
    """Owns persistent child processes and provides supervisor-wide cleanup."""

    def __init__(self):
        self._native = SupervisorHandle()

    async def start(self, command, *, policy=None):
        """Start a persistent child and return its interactive Process handle."""
        scope = _current_run_scope()
        if scope is not None:
            return await scope.start(command, policy=policy)
        return await self._native.start(command, policy=policy)


def _current_run_scope():
    from ._run_scope import current_scope
    return current_scope()


async def run(command, *, policy=None, input=None, cancellation=None):
    """Run one command, collecting bounded stdout and stderr bytes."""
    token = cancellation if cancellation is not None else CancellationToken()
    scope = _current_run_scope()
    operation = asyncio.ensure_future(
        scope.run_command(command, policy=policy, input=input) if scope is not None
        else _run_native(command, policy=policy, input=input, cancellation=token)
    )
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        if scope is not None:
            await scope.cancel()
        else:
            token.cancel()
        try:
            await asyncio.shield(operation)
        except SupervisorError:
            pass
        raise


@asynccontextmanager
async def stream(command, *, policy=None, cancellation=None, capture=False):
    """Yield output events and clean up the child on context exit."""
    token = cancellation if cancellation is not None else CancellationToken()
    scope = _current_run_scope()
    if scope is not None:
        process = await scope.start(command, policy=policy)
        await process.close_stdin()
        finished = False

        async def scoped_events():
            nonlocal finished
            try:
                async for event in process.events():
                    if event.kind == "exited":
                        finished = True
                    yield event
            finally:
                if not finished:
                    await process.shutdown(grace=0)

        events = scoped_events()
        try:
            yield events
        finally:
            cleanup = asyncio.ensure_future(events.aclose())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await scope.cancel()
                await asyncio.shield(cleanup)
                raise
        return

    opening = asyncio.ensure_future(
        _stream_native(
            command,
            policy=policy,
            cancellation=token,
            capture=capture,
        )
    )
    try:
        events = await asyncio.shield(opening)
    except asyncio.CancelledError:
        token.cancel()
        try:
            events = await asyncio.shield(opening)
        except Exception:
            raise
        cleanup = asyncio.ensure_future(events.aclose())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(cleanup)
            raise
        raise

    try:
        yield events
    finally:
        cleanup = asyncio.ensure_future(events.aclose())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await asyncio.shield(cleanup)
            raise

__all__ = [
    "CancellationToken",
    "AgentRunPolicy",
    "AgentRunScope",
    "Command",
    "OutputEvent",
    "Policy",
    "Process",
    "RunResult",
    "SupervisorError",
    "Supervisor",
    "platform_capabilities",
    "run",
    "stream",
]
