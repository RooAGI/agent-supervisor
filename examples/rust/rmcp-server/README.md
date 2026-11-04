# Rust rmcp server

This example uses the official Rust `rmcp` SDK and Agent Supervisor's shared
MCP tool policy. From the repository root:

```bash
cargo build --manifest-path examples/rust/rmcp-server/Cargo.toml
python examples/rust/rmcp-server/check.py examples/rust/rmcp-server/target/debug/agent-supervisor-rmcp-example
```

Append `.exe` to the executable path on Windows. The check requires the
official Python MCP SDK (`pip install -e 'python[mcp]'` from the repository
root, or install `mcp` in the active environment).
