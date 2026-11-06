"""Run PydanticAI agents within an Agent Supervisor run."""

import inspect

from ._lifecycle import invoke_run, run_context_manager


async def run(agent, *args, supervision=None, supervisor=None, **kwargs):
    """Run one PydanticAI agent invocation with a deadline and cleanup."""
    return await invoke_run(
        lambda: agent.run(*args, **kwargs), supervision, supervisor
    )


def run_stream(agent, *args, supervision=None, supervisor=None, **kwargs):
    """Return a AgentRun-backed async context manager for PydanticAI ``run_stream``.

    The returned object's ``agent_run`` can be passed to
    ``Supervisor.stream(run=...)``. Agent events reach watchers through
    PydanticAI's ``event_stream_handler``, chaining any handler you configured.
    """

    def open_context(run):
        if _accepts_event_handler(agent.run_stream):
            configured = kwargs.get("event_stream_handler") or getattr(
                agent, "event_stream_handler", None
            )
            kwargs["event_stream_handler"] = _publishing_handler(run, configured)
        return agent.run_stream(*args, **kwargs)

    return run_context_manager(open_context, supervision, supervisor)


def _accepts_event_handler(method):
    try:
        return "event_stream_handler" in inspect.signature(method).parameters
    except (TypeError, ValueError):
        return False


def _publishing_handler(run, configured):
    async def handler(context, events):
        async def published():
            async for event in events:
                run.publish_agent_event(event)
                yield event

        if configured is not None:
            await configured(context, published())
        else:
            async for _event in published():
                pass

    return handler
