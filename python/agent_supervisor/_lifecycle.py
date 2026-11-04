"""Shared lifecycle helpers for optional agent framework adapters."""

import asyncio
import inspect
import math

from .agent_run import AgentRunPolicy, AgentRunScope
from ._run_scope import active_scope


def supervision_settings(supervision):
    if supervision is None:
        return 300.0, 5.0
    if not isinstance(supervision, dict):
        raise TypeError("supervision must be a dict or None")
    unknown = set(supervision) - {"timeout", "shutdown_grace"}
    if unknown:
        raise TypeError(f"unexpected supervision option: {sorted(unknown)[0]}")
    timeout = supervision.get("timeout", 300.0)
    grace = supervision.get("shutdown_grace", 5.0)
    if (
        not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be a finite positive number of seconds")
    if not isinstance(grace, (int, float)) or not 0 <= grace <= 60:
        raise ValueError("shutdown_grace must be between 0 and 60")
    return float(timeout), float(grace)


def new_scope(supervision):
    timeout, grace = supervision_settings(supervision)
    return AgentRunScope(AgentRunPolicy(timeout, grace))


async def invoke_scoped(operation, supervision=None):
    """Call an awaitable-producing function within one public run scope."""
    scope = new_scope(supervision)
    try:
        return await scope.run(operation())
    finally:
        await asyncio.shield(scope.cancel())


def scoped_iterator(iterator, supervision=None, on_close=None):
    """Keep a native run scope alive while an async iterator is consumed."""
    timeout, grace = supervision_settings(supervision)
    scope = AgentRunScope(AgentRunPolicy(timeout, grace))
    return ScopedAsyncIterator(iterator, scope, timeout, on_close)


def scoped_context_manager(context_manager, supervision=None):
    timeout, grace = supervision_settings(supervision)
    return ScopedAsyncContextManager(
        context_manager, AgentRunScope(AgentRunPolicy(timeout, grace)), timeout
    )


class ScopedAsyncIterator:
    def __init__(self, iterator, scope, timeout, on_close=None):
        self._iterator = iterator.__aiter__()
        self._scope = scope
        self._deadline = asyncio.get_running_loop().time() + timeout
        self._on_close = on_close
        self._closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        token = active_scope.set(self._scope)
        try:
            remaining = self._deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise asyncio.TimeoutError("agent run exceeded its deadline")
            return await asyncio.wait_for(self._iterator.__anext__(), remaining)
        except StopAsyncIteration:
            await self.aclose(completed=True)
            raise
        except BaseException as exc:
            await self.aclose(exc)
            raise
        finally:
            active_scope.reset(token)

    async def aclose(self, exc=None, *, completed=False):
        if self._closed:
            return
        self._closed = True
        token = active_scope.set(self._scope)
        try:
            if self._on_close is not None:
                result = self._on_close(exc, completed)
                if inspect.isawaitable(result):
                    await result
            close = getattr(self._iterator, "aclose", None)
            if close is not None:
                await close()
        finally:
            await asyncio.shield(self._scope.cancel())
            active_scope.reset(token)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.aclose(exc)


class ScopedAsyncContextManager:
    """Keep the scope active across a native async context manager's body."""

    def __init__(self, context_manager, scope, timeout):
        self._context_manager = context_manager
        self._scope = scope
        self._timeout = timeout
        self._token = None
        self._timeout_context = None
        self._entered = False
        self._task = None
        self._timer = None
        self._expired = False

    async def __aenter__(self):
        self._token = active_scope.set(self._scope)
        self._task = asyncio.current_task()
        self._timer = asyncio.get_running_loop().call_later(
            self._timeout, self._expire
        )
        try:
            result = await self._context_manager.__aenter__()
            self._entered = True
            return result
        except BaseException as exc:
            await self._finish(type(exc), exc, exc.__traceback__)
            raise

    async def __aexit__(self, exc_type, exc, traceback):
        return await self._finish(exc_type, exc, traceback)

    async def _finish(self, exc_type, exc, traceback):
        suppress = False
        try:
            if self._entered:
                suppress = await self._context_manager.__aexit__(exc_type, exc, traceback)
        finally:
            if self._timer is not None:
                self._timer.cancel()
            if self._token is not None:
                active_scope.reset(self._token)
                self._token = None
            await asyncio.shield(self._scope.cancel())
        if self._expired and exc_type is asyncio.CancelledError:
            raise asyncio.TimeoutError("agent run exceeded its deadline") from None
        return suppress

    def _expire(self):
        self._expired = True
        if self._task is not None:
            self._task.cancel()
