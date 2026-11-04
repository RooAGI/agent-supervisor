"""Run PydanticAI agents within an Agent Supervisor scope."""

from ._lifecycle import invoke_scoped, scoped_context_manager


async def run(agent, *args, supervision=None, **kwargs):
    """Run one PydanticAI agent invocation with a deadline and cleanup."""
    return await invoke_scoped(lambda: agent.run(*args, **kwargs), supervision)


def run_stream(agent, *args, supervision=None, **kwargs):
    """Return a scoped async context manager for PydanticAI ``run_stream``."""
    return scoped_context_manager(agent.run_stream(*args, **kwargs), supervision)
