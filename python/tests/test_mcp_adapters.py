"""Tests for the optional official MCP and Prefect FastMCP adapters."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys
import unittest


try:
    from agent_supervisor.mcp import MCPConfig, MCPServer
    from agent_supervisor.mcp import ToolError as OfficialToolError
except ImportError as exc:  # The official SDK is an optional extra.
    MCPConfig = MCPServer = OfficialToolError = None
    OFFICIAL_IMPORT_ERROR = exc
else:
    OFFICIAL_IMPORT_ERROR = None

FIXTURES = Path(__file__).parent / "fixtures"


@asynccontextmanager
async def stdio_session(fixture, *args, env=None):
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=[str(FIXTURES / fixture), *args],
        env=env,
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


@unittest.skipIf(MCPServer is None, f"official MCP extra unavailable: {OFFICIAL_IMPORT_ERROR}")
class OfficialMCPAdapterTests(unittest.IsolatedAsyncioTestCase):
    def make_server(self, **config):
        server = MCPServer("test", supervision=MCPConfig(**config))

        @server.tool()
        async def visible() -> str:
            return "ok"

        @server.tool()
        async def hidden() -> str:
            return "hidden"

        return server

    def test_compatibility_defaults_and_validation(self):
        config = MCPConfig()
        self.assertEqual(config.call_timeout, 30.0)
        self.assertEqual(config.max_concurrent_calls, 4)
        self.assertEqual(config.max_result_bytes, 1024 * 1024)
        self.assertEqual(config.max_session_bytes, 1024 * 1024 * 1024)
        self.assertEqual(config.shutdown_grace, 2.0)
        with self.assertRaises(ValueError):
            MCPConfig(call_timeout=0)
        with self.assertRaises(ValueError):
            MCPConfig(max_concurrent_calls=0)
        with self.assertRaises(ValueError):
            MCPConfig(shutdown_grace=61)

    async def test_denied_tool_is_hidden_and_rejected(self):
        server = self.make_server(denied_tools={"hidden"})
        names = {tool.name for tool in await server.list_tools()}
        self.assertIn("visible", names)
        self.assertNotIn("hidden", names)
        with self.assertRaises(OfficialToolError):
            await server.call_tool("hidden", {})

    async def test_tool_result_size_is_limited(self):
        server = MCPServer("test", supervision=MCPConfig(max_result_bytes=32))

        @server.tool()
        async def large_result() -> str:
            return "x" * 256

        with self.assertRaises(OfficialToolError):
            await server.call_tool("large_result", {})

    async def test_concurrency_limit_rejects_an_extra_call(self):
        server = MCPServer("test", supervision=MCPConfig(max_concurrent_calls=1))

        @server.tool()
        async def slow() -> str:
            await asyncio.sleep(0.1)
            return "done"

        first = asyncio.create_task(server.call_tool("slow", {}))
        for _ in range(100):
            if server._supervised_calls:
                break
            await asyncio.sleep(0.001)
        with self.assertRaises(OfficialToolError):
            await server.call_tool("slow", {})
        await first

    async def test_call_deadline_requests_cancellation(self):
        server = MCPServer("test", supervision=MCPConfig(call_timeout=0.01))
        cancelled = asyncio.Event()

        @server.tool()
        async def slow() -> str:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return "late"

        with self.assertRaises(OfficialToolError):
            await server.call_tool("slow", {})
        await asyncio.wait_for(cancelled.wait(), timeout=1)

    async def test_stdio_session_runs_under_supervision(self):
        async with stdio_session("official_mcp_server.py") as session:
            tools = await session.list_tools()
            self.assertIn("greet", {tool.name for tool in tools.tools})
            result = await session.call_tool("greet", {"name": "Roo"})
            self.assertEqual(result.content[0].text, "hello Roo")

    async def test_stdio_shared_policy(self):
        await exercise_shared_stdio_policy(self, "official_mcp_server.py")

    async def test_stdio_environment_compatibility_and_allowlist(self):
        values = {
            "AGENT_SUPERVISOR_MCP_ALLOWED": "available",
            "AGENT_SUPERVISOR_MCP_SECRET": "secret",
        }
        async with stdio_session("official_mcp_server.py", env=values) as session:
            result = await session.call_tool("environment_values", {})
            self.assertEqual(result.content[0].text, "available|secret")
        async with stdio_session("official_mcp_server.py", "--allowlist", env=values) as session:
            result = await session.call_tool("environment_values", {})
            self.assertEqual(result.content[0].text, "available|<unset>")


async def exercise_shared_stdio_policy(testcase, fixture):
    async with stdio_session(fixture, "--policy") as session:
        tools = await session.list_tools()
        testcase.assertNotIn("hidden", {tool.name for tool in tools.tools})
        with testcase.assertRaisesRegex(Exception, "denied"):
            await session.call_tool("hidden", {})
        with testcase.assertRaisesRegex(Exception, "max_result_bytes"):
            await session.call_tool("large", {})
        first = asyncio.create_task(session.call_tool("slow", {}))
        await asyncio.sleep(0.03)
        with testcase.assertRaisesRegex(Exception, "concurrency"):
            await session.call_tool("slow", {})
        with testcase.assertRaisesRegex(Exception, "deadline"):
            await first


try:
    from agent_supervisor.fastmcp import FastMCP
    from fastmcp.exceptions import ToolError as PrefectToolError
except ImportError as exc:  # Prefect FastMCP is a separate optional extra.
    FastMCP = PrefectToolError = None
    PREFECT_IMPORT_ERROR = exc
else:
    PREFECT_IMPORT_ERROR = None


@unittest.skipIf(FastMCP is None, f"Prefect FastMCP extra unavailable: {PREFECT_IMPORT_ERROR}")
class PrefectFastMCPAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_denied_tool_is_hidden_and_rejected(self):
        server = FastMCP("test", supervision=MCPConfig(denied_tools={"hidden"}))

        @server.tool()
        async def visible() -> str:
            return "ok"

        @server.tool()
        async def hidden() -> str:
            return "hidden"

        names = {tool.name for tool in await server.list_tools()}
        self.assertIn("visible", names)
        self.assertNotIn("hidden", names)
        with self.assertRaises(PrefectToolError):
            await server.call_tool("hidden", {})

    async def test_tool_result_size_is_limited(self):
        server = FastMCP("test", supervision=MCPConfig(max_result_bytes=32))

        @server.tool()
        async def large_result() -> str:
            return "x" * 256

        with self.assertRaises(PrefectToolError):
            await server.call_tool("large_result", {})

    async def test_stdio_session_runs_under_supervision(self):
        async with stdio_session("prefect_mcp_server.py") as session:
            tools = await session.list_tools()
            self.assertIn("greet", {tool.name for tool in tools.tools})
            result = await session.call_tool("greet", {"name": "Roo"})
            self.assertEqual(result.content[0].text, "hello Roo")

    async def test_stdio_shared_policy(self):
        await exercise_shared_stdio_policy(self, "prefect_mcp_server.py")

    async def test_stdio_environment_compatibility_and_allowlist(self):
        values = {
            "AGENT_SUPERVISOR_MCP_ALLOWED": "available",
            "AGENT_SUPERVISOR_MCP_SECRET": "secret",
        }
        async with stdio_session("prefect_mcp_server.py", env=values) as session:
            result = await session.call_tool("environment_values", {})
            self.assertEqual(result.content[0].text, "available|secret")
        async with stdio_session("prefect_mcp_server.py", "--allowlist", env=values) as session:
            result = await session.call_tool("environment_values", {})
            self.assertEqual(result.content[0].text, "available|<unset>")

    async def test_concurrency_limit_rejects_an_extra_call(self):
        server = FastMCP("test", supervision=MCPConfig(max_concurrent_calls=1))

        @server.tool()
        async def slow() -> str:
            await asyncio.sleep(0.1)
            return "done"

        first = asyncio.create_task(server.call_tool("slow", {}))
        for _ in range(100):
            if server._supervised_calls:
                break
            await asyncio.sleep(0.001)
        with self.assertRaises(PrefectToolError):
            await server.call_tool("slow", {})
        await first

    async def test_call_deadline_requests_cancellation(self):
        server = FastMCP("test", supervision=MCPConfig(call_timeout=0.01))
        cancelled = asyncio.Event()

        @server.tool()
        async def slow() -> str:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return "late"

        with self.assertRaises(PrefectToolError):
            await server.call_tool("slow", {})
        await asyncio.wait_for(cancelled.wait(), timeout=1)


if __name__ == "__main__":
    unittest.main()
