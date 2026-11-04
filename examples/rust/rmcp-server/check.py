"""Exercise the example with an official MCP Python client."""
import asyncio
import sys

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def main(binary: str):
    async with stdio_client(StdioServerParameters(command=binary)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            names = {tool.name for tool in (await session.list_tools()).tools}
            assert "greet" in names and "hidden" not in names
            assert (await session.call_tool("greet", {})).content[0].text == "hello"
            for name, expected in (("hidden", "denied"), ("large", "max_result_bytes")):
                try:
                    await session.call_tool(name, {})
                except Exception as exc:
                    assert expected in str(exc), repr(exc)
                else:
                    raise AssertionError(f"{name} was not rejected")
            first = asyncio.create_task(session.call_tool("slow", {}))
            await asyncio.sleep(0.03)
            try:
                await session.call_tool("slow", {})
            except Exception as exc:
                assert "concurrency" in str(exc), repr(exc)
            else:
                raise AssertionError("concurrency limit did not fire")
            try:
                await first
            except Exception as exc:
                assert "deadline" in str(exc), repr(exc)
            else:
                raise AssertionError("deadline did not fire")
    print("MCP tool controls passed")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
