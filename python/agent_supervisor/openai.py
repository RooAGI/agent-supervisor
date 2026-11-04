"""Drop-in run deadline and local process cleanup for OpenAI Agents SDK."""

import asyncio
from agents import Runner as _OpenAIRunner

from ._lifecycle import new_scope, supervision_settings
from ._run_scope import active_scope

_active_scope = active_scope


class Runner:
    """OpenAI Runner adapter with a run deadline and child-process cleanup.

    Calls the official Runner methods unchanged. Local processes started through
    this package during a run are stopped when the run finishes or is cancelled.
    """

    @classmethod
    async def run(cls, *args, supervision=None, **kwargs):
        return await cls._invoke(_OpenAIRunner.run, *args, supervision=supervision, **kwargs)

    @classmethod
    async def run_streamed(cls, *args, supervision=None, **kwargs):
        timeout, _ = cls._settings(supervision)
        scope = new_scope(supervision)
        try:
            result = await scope.run(_OpenAIRunner.run_streamed(*args, **kwargs))
        except BaseException:
            await scope.cancel()
            raise
        return _ScopedStreamingResult(result, scope, timeout)

    @classmethod
    def run_sync(cls, *args, supervision=None, **kwargs):
        # The official sync entry point wraps Runner.run() in asyncio.run().
        # Doing that here keeps the same synchronous contract while retaining
        # the Rust scope for the full run.
        return asyncio.run(
            cls._invoke(_OpenAIRunner.run, *args, supervision=supervision, **kwargs)
        )

    @classmethod
    async def _invoke(cls, method, *args, supervision=None, **kwargs):
        scope = new_scope(supervision)
        try:
            return await scope.run(method(*args, **kwargs))
        finally:
            await scope.cancel()

    @staticmethod
    def _settings(supervision):
        return supervision_settings(supervision)


class _ScopedStreamingResult:
    """Keep the Rust run scope alive while an OpenAI streaming result is read."""

    def __init__(self, result, scope, timeout):
        self._result = result
        self._scope = scope
        self._deadline = asyncio.get_running_loop().time() + timeout
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._result, name)

    def cancel(self, *args, **kwargs):
        result = self._result.cancel(*args, **kwargs)
        self._scope.cancel_now()
        return result

    def __del__(self):
        if not self._closed:
            self._scope.cancel_now()

    async def stream_events(self):
        if self._closed:
            return
        token = _active_scope.set(self._scope)
        iterator = self._result.stream_events().__aiter__()
        try:
            while True:
                remaining = self._deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                try:
                    event = await asyncio.wait_for(iterator.__anext__(), remaining)
                except StopAsyncIteration:
                    break
                yield event
        except BaseException:
            self._result.cancel()
            raise
        finally:
            _active_scope.reset(token)
            self._closed = True
            await self._scope.cancel()


def current_scope():
    """Return this task's native run scope, if called from an adapted run."""
    return _active_scope.get()
