"""Supervised adapter for PrefectHQ's standalone ``fastmcp`` package.

Replace ``from fastmcp import FastMCP`` with ``from agent_supervisor.fastmcp import FastMCP``.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

from . import CancellationToken, Command
from ._native import relay_stdio
from .mcp import MCPConfig

try:
    from fastmcp import FastMCP as _FastMCP
    from fastmcp.exceptions import ToolError
except ModuleNotFoundError as exc:
    raise ImportError('Install agent-supervisor[fastmcp] to use the Prefect FastMCP adapter') from exc

_LOG = logging.getLogger("agent_supervisor.fastmcp")
_CHILD = "_AGENT_SUPERVISOR_MCP_PARENT"


class FastMCP(_FastMCP):
    """Prefect FastMCP server with supervised stdio and tool-call controls."""

    def __init__(self, *args, supervision: MCPConfig | None = None, **kwargs):
        self.supervision = supervision or MCPConfig()
        self._mcp_proxy_active = False
        self._supervised_calls: set[asyncio.Task] = set()
        super().__init__(*args, **kwargs)

    async def list_tools(self, *args, **kwargs):
        tools = await super().list_tools(*args, **kwargs)
        if self._mcp_proxy_active:
            return tools
        return [tool for tool in tools if tool.name not in self.supervision.denied_tools]

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None, **kwargs):
        if self._mcp_proxy_active:
            return await super().call_tool(name, arguments, **kwargs)
        # FastMCP re-enters this public method from its middleware chain with
        # run_middleware=False. The outer call already owns supervision; let
        # that inner dispatch reach FastMCP's tool implementation directly.
        if kwargs.get("run_middleware") is False:
            return await super().call_tool(name, arguments, **kwargs)

        config = self.supervision
        if name in config.denied_tools:
            raise ToolError(f"Tool {name!r} is denied by supervisor policy")
        if len(self._supervised_calls) >= config.max_concurrent_calls:
            raise ToolError("Supervisor concurrency limit reached; retry after active work finishes")
        started = time.monotonic()

        async def invoke():
            return await super(FastMCP, self).call_tool(name, arguments, **kwargs)

        task = asyncio.create_task(invoke())
        self._supervised_calls.add(task)

        def completed(done):
            self._supervised_calls.discard(done)
            outcome = "cancelled" if done.cancelled() else ("error" if done.exception() else "completed")
            _LOG.info("tool=%s outcome=%s duration=%.3fs", name, outcome, time.monotonic() - started)

        task.add_done_callback(completed)
        try:
            done, _ = await asyncio.wait({task}, timeout=config.call_timeout)
            if not done:
                task.cancel()
                raise ToolError("Tool deadline exceeded; cancellation requested, execution may still be running")
            result = task.result()
            from pydantic import TypeAdapter
            if len(TypeAdapter(Any).dump_json(result)) > config.max_result_bytes:
                raise ToolError("Tool result exceeds supervisor max_result_bytes")
            return result
        except asyncio.CancelledError:
            task.cancel()
            raise

    def run(self, transport=None, show_banner=None, **kwargs):
        if transport not in (None, "stdio") or kwargs:
            raise ValueError("Supervised startup currently supports stdio only")
        asyncio.run(self.run_async("stdio", show_banner=show_banner))

    async def run_async(self, transport=None, show_banner=None, **kwargs):
        if transport not in (None, "stdio") or kwargs:
            raise ValueError("Supervised startup currently supports stdio only")
        if os.environ.pop(_CHILD, None) == str(os.getppid()):
            self._mcp_proxy_active = True
            await super().run_async("stdio", show_banner=show_banner)
            return
        main = sys.modules.get("__main__")
        script = getattr(main, "__file__", None)
        if not script or not Path(script).is_file() or getattr(main, "__spec__", None) is not None:
            raise RuntimeError("Use a Python script with an if __name__ == '__main__' guard and server.run(); module runners and notebooks are not supported")
        config = self.supervision
        env = {"PYTHONPATH": os.pathsep.join(os.path.abspath(p or '.') for p in sys.path), _CHILD: str(os.getpid())}
        names = list(os.environ) if config.inherit_env is None else list(config.inherit_env)
        names = [name for name in names if name != _CHILD]
        command = Command(
            argv=[sys.executable, str(Path(script).resolve()), *sys.argv[1:]], cwd=os.getcwd(), env=env,
            inherit_env=names, max_stdin_bytes=config.max_session_bytes,
            max_stdout_bytes=config.max_session_bytes, max_stderr_bytes=config.max_session_bytes,
        )
        token = CancellationToken()
        operation = asyncio.ensure_future(relay_stdio(command, policy=config.process_policy,
            cancellation=token, shutdown_grace=config.shutdown_grace, call_timeout=config.call_timeout,
            max_concurrent_calls=config.max_concurrent_calls, max_result_bytes=config.max_result_bytes,
            denied_tools=list(config.denied_tools)))
        previous = {}
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, lambda *_: token.cancel())
            try:
                result = await asyncio.shield(operation)
            except asyncio.CancelledError:
                token.cancel()
                await asyncio.shield(operation)
                raise
            if result.termination == "exited" and result.exit_code:
                raise SystemExit(result.exit_code)
        finally:
            token.cancel()
            try:
                await asyncio.shield(operation)
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)


__all__ = ["FastMCP", "MCPConfig"]
