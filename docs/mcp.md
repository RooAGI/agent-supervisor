# Supervise an MCP server

Keep your existing MCP server and tools. The Rust example wraps an `rmcp` stdio server in process supervision. The Python examples provide import adapters for the official MCP SDK (`mcp`) and Prefect FastMCP (`fastmcp`), which are separate implementations.

## How MCP supervision works

Your MCP client connects to your existing local stdio server and calls its tools as usual. Agent Supervisor runs the server in a supervised process, forwards MCP messages between the client and server, and cleans up the process when the session ends. Your MCP SDK continues to discover and execute the tools. HTTP transports are not supervised by these adapters.

The same Rust policy layer applies tool-call limits to both Rust and Python stdio servers. You can deny selected tools, bound concurrent calls and result sizes, and request cancellation when a call takes too long.

## Default controls

The following defaults apply to Rust and Python stdio sessions. Python reads these values from the same Rust policy defaults.

| Setting | Default | What it controls |
| --- | ---: | --- |
| `call_timeout` | 30 seconds | Time before cancellation is requested for a tool call |
| `max_concurrent_calls` | 4 | Maximum in-flight tool calls |
| `max_result_bytes` | 1 MiB | Maximum serialized result returned by one tool |
| `denied_tools` | empty | Tools hidden from discovery and rejected when called |
| `max_session_bytes` | 1 GiB per stream | Maximum bytes on each of stdin, stdout, and stderr during the session |
| `shutdown_grace` | 2 seconds | Grace period before forced process cleanup |

Process restrictions are configured separately. Both default to host networking, unrestricted filesystem access, and best-effort enforcement. Python inherits the launch environment by default for compatibility; Rust follows the supplied `SandboxPolicy`, whose default clears the child environment. Configure filesystem, network, and environment restrictions for your server.

A deadline sends an MCP cancellation request; the server may ignore it. A timed-out or client-cancelled call keeps its concurrency slot until the server replies or the session ends. This prevents unconfirmed work from exceeding the configured concurrency limit, but repeated cancellations can exhaust the available slots. Task-augmented tool calls and JSON-RPC batches are not supported by this stdio policy layer.

## Adopt with the official MCP SDK

Choose the tab for the language your server uses. Agent Supervisor manages the local server process and its descendants; the MCP SDK continues to handle protocol messages.

=== "Rust"

    Rust adoption currently requires wrapping your existing server startup; it is not a one-line import change. Add the wrapper to the entry point of your existing Rust MCP server. Keep its `ServerHandler`, tools, and `rmcp` stdio transport. Put the existing server startup and wait calls inside the closure:

    ```rust
    use agent_supervisor::{mcp::run_stdio_server, SandboxPolicy};
    use rmcp::{ServiceExt, transport::stdio};
    use std::error::Error;

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
        run_stdio_server(SandboxPolicy::default(), || async {
            let server = MyMcpServer::new().serve(stdio()).await?;
            server.waiting().await?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        })
        .await
    }
    ```

    Replace `MyMcpServer::new()` with the code your server already uses to construct its `ServerHandler`. When the MCP client starts this executable, `run_stdio_server` starts a supervised copy of the same executable with the same command-line arguments and working directory. That copy runs the closure and starts the MCP server. The original process forwards stdin, stdout, and stderr between the MCP client and the supervised copy, so the MCP client's launch command stays the same. Put server construction inside the closure because code before `run_stdio_server` runs in the original process too.

    Add any required environment variables to `policy.environment.variables` or allowlist names in `policy.environment.inherit`. The wrapper forwards UTF-8 command-line arguments. `timeout_ms` does not end a persistent MCP session. To change tool or session limits, pass an `McpCallPolicy` to `run_stdio_server_with_config`:

    ```rust
    use agent_supervisor::mcp::{McpCallPolicy, run_stdio_server_with_config};
    use std::time::Duration;

    let controls = McpCallPolicy {
        call_timeout: Duration::from_secs(20),
        max_concurrent_calls: 2,
        denied_tools: ["delete_record".to_owned()].into_iter().collect(),
        ..Default::default()
    };
    run_stdio_server_with_config(SandboxPolicy::default(), controls, || async {
        let server = MyMcpServer::new().serve(stdio()).await?;
        server.waiting().await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    }).await?;
    ```

=== "Python"

    This adapter supports the official MCP Python package named `mcp`. From a repository checkout, install it with the MCP extra:

    ```bash
    cd python
    pip install -e '.[mcp]'
    ```

    For the current official `mcp` v2 API, replace:

    ```diff
    - from mcp.server import MCPServer
    + from agent_supervisor.mcp import MCPServer
    ```

    Older official `mcp` v1 servers called the class `FastMCP`. For those, replace:

    ```diff
    - from mcp.server.fastmcp import FastMCP
    + from agent_supervisor.mcp import FastMCP
    ```

    Here is a complete server using the current official `mcp` v2 API. The only required code change is importing `MCPServer` from `agent_supervisor.mcp`; configuration is optional:

    ```python
    from agent_supervisor.mcp import MCPServer, MCPConfig

    mcp = MCPServer("Demo", supervision=MCPConfig(max_concurrent_calls=4))

    @mcp.tool()
    def add(a: int, b: int) -> int:
        return a + b

    if __name__ == "__main__":
        mcp.run()
    ```

    Keep your MCP client configuration pointed at the same `server.py` command. The adapter launches a supervised copy and forwards the stdio session.

## Prefect FastMCP (`fastmcp` package)

Use this section if your installed package is named `fastmcp` and your server imports with `from fastmcp import FastMCP`. This is Prefect's separate FastMCP project; it is not the older `FastMCP` API from the official `mcp` v1 package. Install it alongside the current source checkout:

```bash
cd python
pip install -e '.[fastmcp]'
```

Change only its import:

```diff
- from fastmcp import FastMCP
+ from agent_supervisor.fastmcp import FastMCP
```

Here is a complete Prefect FastMCP server:

```python
from agent_supervisor.fastmcp import FastMCP, MCPConfig

mcp = FastMCP(
    "Demo",
    supervision=MCPConfig(
        denied_tools=frozenset({"delete_record"}),
        call_timeout=20,
        max_concurrent_calls=4,
    ),
)

@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b

@mcp.tool()
def delete_record(record_id: str) -> str:
    """Delete a record (denied by this server's policy)."""
    return f"Deleted {record_id}"

if __name__ == "__main__":
    mcp.run()
```

The server definition, tool decorators, and `mcp.run()` call remain in place. The adapter supports stdio startup; the SDK's HTTP transports are not routed through the process supervisor.

## Customize Python controls

For example, set only the controls that match your server:

```python
from agent_supervisor import Policy
from agent_supervisor.mcp import MCPConfig, MCPServer

mcp = MCPServer(
    "Demo",
    supervision=MCPConfig(
        denied_tools=frozenset({"delete_record"}),
        call_timeout=20,
        max_concurrent_calls=4,
        max_result_bytes=256 * 1024,
        max_session_bytes=512 * 1024 * 1024,
        shutdown_grace=2,
        inherit_env=("PATH", "API_TOKEN"),
        process_policy=Policy(network="disabled"),
    ),
)
```

The example limits the environment to `PATH` and `API_TOKEN` and requests disabled IP networking. Network isolation varies by operating system; inspect `platform_capabilities()` and use `enforcement="required"` when the operation must fail if the requested isolation is unavailable. A network restriction does not restrict filesystem access. For filesystem grants, use the native filesystem policy and grant only the paths the server needs; platform requirements differ.

Denied tools disappear from discovery and are rejected if called directly. Oversized results return an error. The process supervisor owns the server child and its descendants. Cancellation is a request to the server and cannot forcibly stop an individual tool handler.

By default, the child inherits the launching environment to preserve existing server configuration. Set an explicit `inherit_env` tuple to allow only named variables. The native policy still blocks dynamic-loader override variables. `process_policy` applies the native process restrictions available on the current OS; the adapter does not itself provide a filesystem or network sandbox.

The server script is started again in a child process, so keep server startup under `if __name__ == "__main__":` and put `run()` last. Module runners and notebooks are not supported. The session byte limit applies separately to each stdio stream, while the result limit applies to an individual tool response.

For test commands and the current integration-test coverage boundaries, see [Testing and coverage](testing.md).
