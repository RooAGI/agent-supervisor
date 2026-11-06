"""Run Google ADK Runner invocations within an Agent Supervisor run."""

import asyncio

from ._lifecycle import run_iterator


def run(runner, *args, supervision=None, supervisor=None, **kwargs):
    """Return the ADK event stream with run deadline and child cleanup.

    Iterate the returned async iterator or use it as an async context manager.
    An ``asyncio.Event`` is supplied as ADK's ``abort_signal`` unless the
    caller provides one; it is set when the iterator closes.
    """
    abort_signal = kwargs.get("abort_signal")
    if abort_signal is None:
        abort_signal = asyncio.Event()
        kwargs["abort_signal"] = abort_signal

    return run_iterator(
        runner.run_async(*args, **kwargs),
        supervision,
        on_close=lambda exc, completed: (
            None if completed and exc is None else abort_signal.set()
        ),
        supervisor=supervisor,
    )
