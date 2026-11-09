# Examples

Examples are grouped by implementation language. The manager-owned job
examples show how the host can track several processes, stop one by its stable
Supervisor ID, inspect its final output and status, and await cleanup.

## Rust

- [`supervisor-background/`](rust/supervisor-background/): runnable example
  for manager-owned background jobs.
- [`supervisor-tui/`](rust/supervisor-tui/): launches the separate terminal
  process manager with framework-labelled jobs.
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
- [`supervisor_jsonl_worker.py`](python/supervisor_jsonl_worker.py):
  framework-neutral FlowRun worker protocol fixture. It uses only Python's
  standard library and can be smoke-tested without a Desktop package or SDK.
- [`supervisor_tui.py`](python/supervisor_tui.py): launch the separate TUI
  with framework-labelled jobs and stop one from the dashboard.
- [`official_mcp_server.py`](python/official_mcp_server.py): official MCP SDK.
- [`prefect_fastmcp_server.py`](python/prefect_fastmcp_server.py): Prefect
  FastMCP.

Install the checkout's Python package, then run the background-job example
from the repository root:

```bash
python -m pip install -e ./python
python examples/python/supervisor_background_jobs.py
```

### Framework-neutral FlowRun worker fixture

The service-owned workflow worker setting selects the executable and fixed
arguments used for each task attempt. For example, from the repository root:

```bash
cargo build --bin agent-supervisor-service
"$PWD/target/debug/agent-supervisor-service" \
  --socket /tmp/agent-supervisor.sock \
  --view-token-file /path/to/view.token \
  --control-token-file /path/to/control.token \
  --tui-token-file /path/to/tui.token \
  --state-dir /path/to/supervisor-state \
  --workflow-worker-command "$(command -v python3)" \
  --workflow-worker-arg -u \
  --workflow-worker-arg "$PWD/examples/python/supervisor_jsonl_worker.py"
```

The configured service worker receives one JSONL start envelope on stdin for
each reserved attempt. It writes correlated JSONL events to stdout with the
same `attemptId`, `generation`, and increasing `sourceCursor`; stdout is the
protocol stream, while diagnostics belong on stderr. The fixture emits
`running`, then either `succeeded` after its requested `durationMs` (default
one second) or `cancelled` after a cooperative `{"type":"cancel"}` command.
Each terminal event includes a small `output` evidence object. The current
service dispatcher sends a plain `cancel` command; the fixture also checks
attempt identity if a host includes it.

The fixture demonstrates the worker boundary and cancellation contract. It
does not run an agent framework or make model calls. The current service
worker adapter also validates the invocation object before launch; for the
Roo Runtime path that object includes `runtimeOptions`, `graph`, and `input`.
The example ignores those framework fields. Check its protocol locally with:

```bash
python3 -m unittest discover -s examples/python -p 'test_supervisor_jsonl_worker.py'
```

For the Python TUI example, also build the standalone Rust executable and set
its path:

```bash
cargo build --manifest-path tui/Cargo.toml --release
AGENT_SUPERVISOR_TUI="$PWD/tui/target/release/agent-supervisor-tui" \
  python examples/python/supervisor_tui.py
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
