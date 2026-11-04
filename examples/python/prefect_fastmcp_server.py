import asyncio
import os
import sys

from agent_supervisor.fastmcp import FastMCP, MCPConfig

allowed_env = ("PATH", "AGENT_SUPERVISOR_MCP_ALLOWED") if "--allowlist" in sys.argv else None
mcp = FastMCP(
    "test",
    supervision=MCPConfig(max_concurrent_calls=1 if "--policy" in sys.argv else 2,
                          inherit_env=allowed_env,
                          denied_tools={"hidden"} if "--policy" in sys.argv else set(),
                          call_timeout=0.15 if "--policy" in sys.argv else 30,
                          max_result_bytes=256 if "--policy" in sys.argv else 1024 * 1024),
)


@mcp.tool()
async def greet(name: str) -> str:
    return f"hello {name}"


@mcp.tool()
def environment_values() -> str:
    allowed = os.getenv("AGENT_SUPERVISOR_MCP_ALLOWED", "<unset>")
    secret = os.getenv("AGENT_SUPERVISOR_MCP_SECRET", "<unset>")
    return f"{allowed}|{secret}"


@mcp.tool()
async def hidden() -> str:
    return "hidden"


@mcp.tool()
async def slow() -> str:
    await asyncio.sleep(5)
    return "late"


@mcp.tool()
async def large() -> str:
    return "x" * 1024


if __name__ == "__main__":
    mcp.run()
