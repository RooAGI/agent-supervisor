"""Context shared by framework adapters and supervised local tools."""

from contextvars import ContextVar


active_run = ContextVar("agent_supervisor_run_context", default=None)


def current_run():
    """Return the agent run active in this async context, if any."""
    return active_run.get()
