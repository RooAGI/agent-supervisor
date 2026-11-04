"""Run Microsoft Agent Framework agents with Agent Supervisor lifecycle."""

import asyncio
import inspect
from contextvars import Token

from ._lifecycle import new_scope, supervision_settings
from ._run_scope import active_scope


async def run(agent, *args, supervision=None, **kwargs):
    """Run one Microsoft Agent Framework invocation inside a shared scope.

    Local commands started through :func:`agent_supervisor.run`,
    :func:`agent_supervisor.stream`, or :class:`agent_supervisor.Supervisor`
    during this invocation are registered with its native process supervisor.
    ``supervision`` accepts ``timeout`` (default 300 seconds) and
    ``shutdown_grace`` (default 5 seconds).

    For ``stream=True``, this returns a scoped async response stream. Consume it
    fully or use it as an async context manager so early exits clean up.
    """
    scope = new_scope(supervision)
    try:
        method = getattr(agent, "run", None)
        if method is None or not callable(method):
            raise TypeError("agent must provide a callable run() method")

        async def invoke():
            result = method(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        result = await scope.run(invoke())

        if kwargs.get("stream", False) and hasattr(result, "__aiter__"):
            timeout, _ = _settings(supervision)
            return _ScopedResponseStream(result, scope, timeout)

        await scope.cancel()
        return result
    except BaseException:
        await asyncio.shield(scope.cancel())
        raise


def _settings(supervision):
    return supervision_settings(supervision)


class _ScopedResponseStream:
    """Keep a run scope alive while a Microsoft ResponseStream is consumed."""

    def __init__(self, stream, scope, timeout):
        self._stream = stream
        self._iterator = stream.__aiter__()
        self._scope = scope
        self._deadline = asyncio.get_running_loop().time() + timeout
        self._closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        token = active_scope.set(self._scope)
        try:
            remaining = self._remaining()
            return await asyncio.wait_for(self._iterator.__anext__(), remaining)
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException as exc:
            await self.aclose(exc)
            raise
        finally:
            active_scope.reset(token)

    async def get_final_response(self):
        method = getattr(self._stream, "get_final_response", None)
        if method is None:
            raise AttributeError("response stream has no get_final_response()")
        token = active_scope.set(self._scope)
        try:
            result = await asyncio.wait_for(method(), self._remaining())
            await self.aclose()
            return result
        except BaseException as exc:
            await self.aclose(exc)
            raise
        finally:
            active_scope.reset(token)

    async def aclose(self, exc=None):
        if self._closed:
            return
        self._closed = True
        token: Token = active_scope.set(self._scope)
        try:
            close = getattr(self._stream, "close", None)
            if close is not None:
                result = close(exc) if exc is not None else close()
                if inspect.isawaitable(result):
                    await result
        finally:
            await asyncio.shield(self._scope.cancel())
            active_scope.reset(token)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.aclose(exc)

    def _remaining(self):
        remaining = self._deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError("agent run exceeded its deadline")
        return remaining

    def __getattr__(self, name):
        return getattr(self._stream, name)
