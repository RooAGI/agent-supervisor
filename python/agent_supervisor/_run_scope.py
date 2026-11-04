"""Context shared by framework adapters and supervised local tools."""

from contextvars import ContextVar


active_scope = ContextVar("agent_supervisor_run_scope", default=None)


def current_scope():
    """Return the run scope active in this async context, if any."""
    return active_scope.get()
