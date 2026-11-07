"""Run Microsoft Agent Framework agents with Agent Supervisor lifecycle."""

import asyncio
import inspect
from contextvars import Token

from ._lifecycle import AgentEventPump, new_run, supervision_settings
from ._run_context import active_run


async def run(agent, *args, supervision=None, supervisor=None, **kwargs):
    """Run one Microsoft Agent Framework invocation inside a shared run.

    Local commands started through :func:`agent_supervisor.run`,
    :meth:`agent_supervisor.Supervisor.stream`, or :class:`agent_supervisor.Supervisor`
    during this invocation are registered with its native process supervisor.
    ``supervision`` accepts ``timeout`` (default 300 seconds) and
    ``shutdown_grace`` (default 5 seconds).

    For ``stream=True``, this returns a AgentRun-backed async response stream. Consume it
    fully or use it as an async context manager so early exits clean up.
    """
    run = new_run(supervision, supervisor)
    try:
        method = getattr(agent, "run", None)
        if method is None or not callable(method):
            raise TypeError("agent must provide a callable run() method")

        async def invoke():
            result = method(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        result = await run.run(invoke())

        if kwargs.get("stream", False) and hasattr(result, "__aiter__"):
            timeout, _ = _settings(supervision)
            return _AgentRunResponseStream(result, run, timeout)

        await run.cancel()
        return result
    except BaseException:
        await asyncio.shield(run.cancel())
        raise


def _settings(supervision):
    return supervision_settings(supervision)


class _AgentRunResponseStream:
    """Keep an agent run alive while a Microsoft ResponseStream is consumed.

    ``agent_run`` can be passed to ``Supervisor.stream(run=...)``.
    """

    def __init__(self, stream, run, timeout):
        self._stream = stream
        self._run = run
        self.agent_run = run
        self._closed = False
        self._deadline = asyncio.get_running_loop().time() + timeout
        self._pump = AgentEventPump(
            stream.__aiter__(), run, self._deadline, self._finish
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self._pump.get()
        except StopAsyncIteration:
            raise
        except BaseException as exc:
            await self.aclose(exc)
            raise

    async def get_final_response(self):
        method = getattr(self._stream, "get_final_response", None)
        if method is None:
            raise AttributeError("response stream has no get_final_response()")
        # The background reader consumes the stream, so let it finish first.
        await self._pump.wait()
        token = active_run.set(self._run)
        try:
            return await asyncio.wait_for(method(), self._remaining())
        except BaseException as exc:
            await self.aclose(exc)
            raise
        finally:
            active_run.reset(token)

    async def aclose(self, exc=None):
        await self._pump.stop(exc)

    async def _finish(self, exc, completed):
        if self._closed:
            return
        self._closed = True
        token: Token = active_run.set(self._run)
        try:
            close = getattr(self._stream, "close", None)
            if close is not None:
                result = close(exc) if exc is not None else close()
                if inspect.isawaitable(result):
                    await result
        finally:
            await asyncio.shield(self._run.cancel())
            active_run.reset(token)

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
