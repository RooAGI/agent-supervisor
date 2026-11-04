# Examples

Examples are grouped by implementation language:

- [`rust/`](rust/): Rust MCP server using `rmcp`.
- [`python/`](python/): official MCP SDK and Prefect FastMCP servers.

Install the Python adapter and matching server SDK from the repository root:

```bash
cd python
pip install -e '.[mcp]'       # official MCP SDK
# or: pip install -e '.[fastmcp]'  # Prefect FastMCP
```

Run Python servers from the repository root with `python examples/python/official_mcp_server.py` or `python examples/python/prefect_fastmcp_server.py`. The Rust server has build and policy examples in `rust/rmcp-server/README.md`.
