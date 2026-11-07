"""Run local LangGraph workflows within an Agent Supervisor run."""

from ._lifecycle import invoke_run, run_iterator


async def run(graph, inputs, *args, supervision=None, supervisor=None, **kwargs):
    """Invoke a compiled graph and clean up its supervised tool processes."""
    return await invoke_run(
        lambda: graph.ainvoke(inputs, *args, **kwargs), supervision, supervisor
    )


def stream(graph, inputs, *args, supervision=None, supervisor=None, **kwargs):
    """Stream a compiled graph; consume or close the returned async iterator."""
    return run_iterator(
        graph.astream(inputs, *args, **kwargs), supervision,
        supervisor=supervisor
    )
