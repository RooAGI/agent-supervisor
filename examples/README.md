# Examples

Examples are grouped by implementation language. The manager-owned job
examples show how the host can track several processes, stop one by its stable
Supervisor ID, inspect its final output and status, and await cleanup.

## Rust

- [`supervisor-background/`](rust/supervisor-background/): runnable example
  for manager-owned background jobs.
- [`rmcp-server/`](rust/rmcp-server/): Rust MCP server using `rmcp`.

Run the background-job example from its project directory:

```bash
cd examples/rust/supervisor-background
cargo run
```

It starts this example executable in child mode, so it does not depend on a
system-specific shell or interpreter.

## Python

- [`supervisor_background_jobs.py`](python/supervisor_background_jobs.py):
  manager-owned background jobs.
- [`official_mcp_server.py`](python/official_mcp_server.py): official MCP SDK.
- [`prefect_fastmcp_server.py`](python/prefect_fastmcp_server.py): Prefect
  FastMCP.

Install the checkout's Python package, then run the background-job example
from the repository root:

```bash
python -m pip install -e ./python
python examples/python/supervisor_background_jobs.py
```

For MCP examples, install the matching server SDK from the repository root:

```bash
cd python
python -m pip install -e '.[mcp]'       # official MCP SDK
# or: python -m pip install -e '.[fastmcp]'  # Prefect FastMCP
```

Run Python MCP servers from the repository root with
`python examples/python/official_mcp_server.py` or
`python examples/python/prefect_fastmcp_server.py`. The Rust MCP server has
build and policy examples in
[`rust/rmcp-server/README.md`](rust/rmcp-server/README.md).
