"""Drop-in run deadline and local process cleanup for OpenAI Agents SDK."""

import asyncio
from agents import Runner as _OpenAIRunner

from ._lifecycle import AgentEventPump, new_run, supervision_settings
from ._run_context import active_run

_active_run = active_run


class Runner:
    """OpenAI Runner adapter with a run deadline and child-process cleanup.

    Calls the official Runner methods unchanged. Local processes started through
    this package during a run are stopped when the run finishes or is cancelled.
    """

    @classmethod
    async def run(cls, *args, supervision=None, supervisor=None, **kwargs):
        return await cls._invoke(
            _OpenAIRunner.run, *args, supervision=supervision,
            supervisor=supervisor, **kwargs
        )

    @classmethod
    async def run_streamed(cls, *args, supervision=None, supervisor=None, **kwargs):
        timeout, _ = cls._settings(supervision)
        run = new_run(supervision, supervisor)
        try:
            result = await run.run(_OpenAIRunner.run_streamed(*args, **kwargs))
        except BaseException:
            await run.cancel()
            raise
        return _AgentRunStreamingResult(result, run, timeout)

    @classmethod
    def run_sync(cls, *args, supervision=None, supervisor=None, **kwargs):
        # The official sync entry point wraps Runner.run() in asyncio.run().
        # Doing that here keeps the same synchronous contract while retaining
        # the Rust run for the full run.
        return asyncio.run(
            cls._invoke(
                _OpenAIRunner.run, *args, supervision=supervision,
                supervisor=supervisor, **kwargs
            )
        )

    @classmethod
    async def _invoke(cls, method, *args, supervision=None, supervisor=None, **kwargs):
        run = new_run(supervision, supervisor)
        try:
            return await run.run(method(*args, **kwargs))
        finally:
            await run.cancel()

    @staticmethod
    def _settings(supervision):
        return supervision_settings(supervision)


class _AgentRunStreamingResult:
    """Keep the Rust agent run alive while an OpenAI streaming result is read.

    ``agent_run`` can be passed to ``Supervisor.stream(run=...)``.
    """

    def __init__(self, result, run, timeout):
        self._result = result
        self._run = run
        self.agent_run = run
        self._closed = False
        deadline = asyncio.get_running_loop().time() + timeout
        self._pump = AgentEventPump(
            result.stream_events().__aiter__(), run, deadline, self._finish
        )

    def __getattr__(self, name):
        return getattr(self._result, name)

    def cancel(self, *args, **kwargs):
        result = self._result.cancel(*args, **kwargs)
        self._run.cancel_now()
        return result

    def __del__(self):
        if not self._closed:
            self._run.cancel_now()

    async def _finish(self, exc, completed):
        if self._closed:
            return
        self._closed = True
        if not completed:
            self._result.cancel()
        await asyncio.shield(self._run.cancel())

    async def stream_events(self):
        try:
            while True:
                try:
                    event = await self._pump.get()
                except StopAsyncIteration:
                    return
                yield event
        finally:
            await self._pump.stop()


def current_run():
    """Return this task's native agent run, if called from an adapted run."""
    return _active_run.get()
