"""Run local LangGraph workflows within an Agent Supervisor scope."""

from ._lifecycle import invoke_scoped, scoped_iterator


async def run(graph, inputs, *args, supervision=None, **kwargs):
    """Invoke a compiled graph and clean up its supervised tool processes."""
    return await invoke_scoped(
        lambda: graph.ainvoke(inputs, *args, **kwargs), supervision
    )


def stream(graph, inputs, *args, supervision=None, **kwargs):
    """Stream a compiled graph; consume or close the returned async iterator."""
    return scoped_iterator(graph.astream(inputs, *args, **kwargs), supervision)
