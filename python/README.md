# agent-supervisor for Python

Python bindings for the `agent-supervisor` native process supervisor.

Version 0.2.0 source provides bounded one-shot execution and output-event
streaming through the native Linux, macOS, and Windows backends. The latest
published PyPI version is 0.1.2. Its wheels target Apple Silicon macOS,
x86-64 Windows, and x86-64 Linux with glibc 2.39 or newer. The Rust crate's
optional WASM runner is not exposed by the Python binding.

Install the published package with `python -m pip install agent-supervisor`.
To use the current repository source, build it from this checkout:

## Build from this checkout

Create and activate a virtual environment, then run:

```bash
python -m pip install maturin
cd python
python -m maturin develop
```

The API and supported platform behavior are documented in
[`../docs/python-api.md`](../docs/python-api.md). The repository source also includes
optional stdio adapters for the official MCP Python SDK and Prefect FastMCP; see
[`../docs/mcp.md`](../docs/mcp.md) for complete server examples and installation steps.

The 0.2.0 repository source includes optional run adapters for OpenAI Agents
SDK, Microsoft Agent Framework, LangGraph, PydanticAI, and Google ADK. The
published PyPI package is 0.1.2 and does not include these adapters yet. From a
checkout, install an adapter with its matching extra, for example:

```bash
cd python
python -m pip install -e '.[langgraph]'
```

Use `openai`, `microsoft`, `pydantic-ai`, or `google-adk` for the other
adapters. They share the Rust run scope,
apply the same default deadline and cleanup behavior, and clean up local tool
processes started through Agent Supervisor. Agent code remains in your app
process. See the
[agent SDK integration guide](../docs/api-boundaries.md).
