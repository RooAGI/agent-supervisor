"""Python bindings for agent-supervisor."""

import asyncio
from contextlib import asynccontextmanager

from ._native import (
    CancellationToken,
    Command,
    OutputEvent,
    Policy,
    RunResult,
    SupervisorError,
    platform_capabilities,
    run as _run_native,
    stream as _stream_native,
)


async def run(command, *, policy=None, input=None, cancellation=None):
    """Run one command, collecting bounded stdout and stderr bytes."""
    token = cancellation if cancellation is not None else CancellationToken()
    operation = asyncio.ensure_future(
        _run_native(command, policy=policy, input=input, cancellation=token)
    )
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
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
    "Command",
    "OutputEvent",
    "Policy",
    "RunResult",
    "SupervisorError",
    "platform_capabilities",
    "run",
    "stream",
]
