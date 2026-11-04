# Testing and coverage

Run the Rust and Python checks from the repository root. The Rust integration
tests use native process controls, so run them on the operating system you plan
to support.

## Rust

```bash
cargo fmt --all -- --check
cargo test --all-targets -- --test-threads=1
cargo test --features wasm --all-targets -- --test-threads=1
cargo clippy --all-targets -- -D warnings
```

The MCP stdio integration tests cover traffic forwarding,
argument forwarding, environment isolation and explicit allowlisting, child
failure reporting, cleanup after the client closes stdin, and shared tool-call
policy enforcement. The WASM tests
cover the guest ABI, rejected host imports, request authority, malformed
modules, fuel and memory limits, and input/output limits.

## Python API and MCP adapters

Use Python 3.10 or newer to install the optional MCP SDK test dependencies:

```bash
cd python
python -m pip install -e '.[mcp,fastmcp]'
python -m unittest discover -s tests -v
```

The full suite covers the Python command and stream APIs plus real stdio MCP
client sessions for the official MCP SDK v2 and Prefect FastMCP. Adapter tests
also cover denied tools, concurrency limits, call deadlines, result-size
limits, default environment inheritance, and explicit environment allowlists.
The suite skips an adapter if its optional SDK dependency is not installed.
The dedicated `mcp-integration` CI job installs both extras so neither adapter
can silently skip there. It also builds the Rust `rmcp` example and exercises it
through the official Python MCP client.

To check compatibility with the older official MCP SDK v1 API, use a fresh
environment and pin a v1 release:

```bash
cd python
python -m pip install -e '.[mcp]' 'mcp==1.30.0'
PYTHONPATH=. python -m unittest tests.test_mcp_adapters.OfficialMCPAdapterTests -v
```

The Python native stdio relay also has focused Rust tests:

```bash
cargo test --manifest-path python/Cargo.toml --lib mcp_stdio::tests
```

## Coverage limits

The example in `examples/rust/rmcp-server` runs a real Rust `rmcp` server. CI
builds and exercises this example on Linux. With
the official Python MCP SDK installed, exercise its discovery, denial, result
size, concurrency, and timeout controls from the repository root:

```bash
cargo build --manifest-path examples/rust/rmcp-server/Cargo.toml
python examples/rust/rmcp-server/check.py examples/rust/rmcp-server/target/debug/agent-supervisor-rmcp-example
```

On Windows, append `.exe` to the executable path. These examples cover stdio;
the adapters do not supervise HTTP transports.

The platform-specific filesystem suites run on Linux, macOS, and Windows. The
Windows suite also verifies that a network-disabled AppContainer cannot
connect to a listener bound to the host's non-loopback interface. The Rust CI
matrix runs the WASM feature tests on each supported OS. OpenShell adapter unit
tests run separately without a live gateway.
Optional MCP adapter dependencies must be installed for the Python adapter
tests to run rather than skip.
