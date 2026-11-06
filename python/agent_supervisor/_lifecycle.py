"""Shared lifecycle helpers for optional agent framework adapters."""

import asyncio
import inspect
import math

from .agent_run import AgentRunPolicy
from ._run_context import active_run


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


def new_run(supervision, supervisor=None):
    timeout, grace = supervision_settings(supervision)
    if supervisor is None:
        from . import Supervisor

        supervisor = Supervisor()
    return supervisor.start_agent_run(AgentRunPolicy(timeout, grace))


async def invoke_run(operation, supervision=None, supervisor=None):
    """Call a framework operation within a run owned by ``supervisor``."""
    run = new_run(supervision, supervisor)
    try:
        return await run.run(operation())
    finally:
        await asyncio.shield(run.cancel())


_DONE = object()


class AgentEventPump:
    """Read a framework's stream in the background.

    Each event is published to watchers of the run and queued for the caller,
    so watchers see agent events whether or not the caller reads the stream.
    The queue is unbounded because dropping events would change what the caller
    receives; the run's deadline limits how long it can grow.

    ``on_finish(exc, completed)`` runs once when reading stops, in this task.
    """

    def __init__(self, iterator, run, deadline, on_finish):
        self._iterator = iterator
        self._run = run
        self._deadline = deadline
        self._on_finish = on_finish
        self._queue = asyncio.Queue()
        self._stop_exc = None
        self._task = asyncio.ensure_future(self._read())

    async def _read(self):
        token = active_run.set(self._run)
        loop = asyncio.get_running_loop()
        failure = None
        completed = False
        try:
            while True:
                remaining = self._deadline - loop.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError("agent run exceeded its deadline")
                try:
                    event = await asyncio.wait_for(self._next_event(), remaining)
                except StopAsyncIteration:
                    completed = True
                    break
                self._run.publish_agent_event(event)
                self._queue.put_nowait(event)
        except asyncio.CancelledError:
            failure = self._stop_exc
        except BaseException as exc:
            failure = exc
        finally:
            active_run.reset(token)
        try:
            await self._on_finish(failure, completed)
        except BaseException as exc:
            failure = failure or exc
        self._queue.put_nowait(_Failure(failure) if failure is not None else _DONE)

    async def _next_event(self):
        # `wait_for` advances the iterator in its own task. Set the run in
        # that task explicitly so tools called by async generators always
        # inherit this invocation's process scope.
        token = active_run.set(self._run)
        try:
            return await self._iterator.__anext__()
        finally:
            active_run.reset(token)

    async def get(self):
        """Return the next event, or raise when the stream has ended."""
        item = await self._queue.get()
        if item is _DONE or isinstance(item, _Failure):
            # Keep the end marker so later reads end the same way.
            self._queue.put_nowait(item)
            if item is _DONE:
                raise StopAsyncIteration
            raise item.exc
        return item

    async def wait(self):
        """Wait until the framework's stream has been fully read."""
        await asyncio.gather(self._task, return_exceptions=True)

    async def stop(self, exc=None):
        """Stop reading early. Cleanup runs in the reading task."""
        if not self._task.done():
            self._stop_exc = exc
            self._task.cancel()
        await self.wait()


class _Failure:
    def __init__(self, exc):
        self.exc = exc


def run_iterator(iterator, supervision=None, on_close=None, supervisor=None):
    """Keep a native agent run alive while a framework's stream is read."""
    timeout, grace = supervision_settings(supervision)
    if supervisor is None:
        from . import Supervisor

        supervisor = Supervisor()
    run = supervisor.start_agent_run(AgentRunPolicy(timeout, grace))
    return AgentRunAsyncIterator(iterator, run, timeout, on_close)


def run_context_manager(open_context, supervision=None, supervisor=None):
    """Keep a run active across ``open_context(run)``'s context manager."""
    timeout, grace = supervision_settings(supervision)
    run = new_run({"timeout": timeout, "shutdown_grace": grace}, supervisor)
    return AgentRunAsyncContextManager(open_context(run), run, timeout)


class AgentRunAsyncIterator:
    def __init__(self, iterator, run, timeout, on_close=None):
        self._iterator = iterator.__aiter__()
        self._run = run
        self.agent_run = run
        self._on_close = on_close
        self._closed = False
        deadline = asyncio.get_running_loop().time() + timeout
        self._pump = AgentEventPump(self._iterator, run, deadline, self._finish)

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

    async def aclose(self, exc=None):
        await self._pump.stop(exc)

    async def _finish(self, exc, completed):
        if self._closed:
            return
        self._closed = True
        token = active_run.set(self._run)
        try:
            if self._on_close is not None:
                result = self._on_close(exc, completed)
                if inspect.isawaitable(result):
                    await result
            close = getattr(self._iterator, "aclose", None)
            if close is not None:
                await close()
        finally:
            await asyncio.shield(self._run.cancel())
            active_run.reset(token)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.aclose(exc)


class AgentRunAsyncContextManager:
    """Keep the run active across a native async context manager's body."""

    def __init__(self, context_manager, run, timeout):
        self._context_manager = context_manager
        self._run = run
        self.agent_run = run
        self._timeout = timeout
        self._token = None
        self._timeout_context = None
        self._entered = False
        self._task = None
        self._timer = None
        self._expired = False

    async def __aenter__(self):
        self._token = active_run.set(self._run)
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
                active_run.reset(self._token)
                self._token = None
            await asyncio.shield(self._run.cancel())
        if self._expired and exc_type is asyncio.CancelledError:
            raise asyncio.TimeoutError("agent run exceeded its deadline") from None
        return suppress

    def _expire(self):
        self._expired = True
        if self._task is not None:
            self._task.cancel()
