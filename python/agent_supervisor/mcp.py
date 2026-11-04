"""Supervised stdio servers for the official MCP Python SDK.

Replace the SDK server-class import with this module. See docs/mcp.md for
supported entry points and the distinction between call and process limits.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from typing import Any

from . import CancellationToken, Command, Policy
from ._native import mcp_defaults, relay_stdio

try:
    _major = int(version("mcp").split(".")[0])
    if _major == 1:
        from mcp.server.fastmcp import FastMCP as _Server, Context
        from mcp.server.fastmcp.exceptions import ToolError
    elif _major == 2:
        from mcp.server.mcpserver import MCPServer as _Server, Context
        from mcp.server.mcpserver.exceptions import ToolError
    else:
        raise ImportError("agent_supervisor.mcp supports official mcp SDK versions 1 and 2")
except (PackageNotFoundError, ModuleNotFoundError) as exc:
    raise ImportError('Install agent-supervisor[mcp] to use the MCP adapter') from exc

from pydantic import TypeAdapter

_LOG = logging.getLogger("agent_supervisor.mcp")
_CHILD = "_AGENT_SUPERVISOR_MCP_PARENT"
_SERIALIZER = TypeAdapter(Any)
_CALL_TIMEOUT, _MAX_CALLS, _MAX_RESULT, _MAX_SESSION, _SHUTDOWN_GRACE = mcp_defaults()


@dataclass(frozen=True)
class MCPConfig:
    """Call controls and the policy shared by all tools in the server process.

    inherit_env=None preserves the launch environment for compatibility.
    Set an explicit tuple to restrict it. Core forbidden loader variables
    remain rejected. Stdio budgets apply to the entire server session.
    """
    call_timeout: float = _CALL_TIMEOUT
    max_concurrent_calls: int = _MAX_CALLS
    max_result_bytes: int = _MAX_RESULT
    denied_tools: frozenset[str] = field(default_factory=frozenset)
    process_policy: Policy = field(default_factory=Policy)
    inherit_env: tuple[str, ...] | None = None
    max_session_bytes: int = _MAX_SESSION
    shutdown_grace: float = _SHUTDOWN_GRACE

    def __post_init__(self):
        if not math.isfinite(self.call_timeout) or self.call_timeout <= 0:
            raise ValueError("call_timeout must be finite and positive")
        for name in ("max_concurrent_calls", "max_result_bytes", "max_session_bytes"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not math.isfinite(self.shutdown_grace) or not 0 <= self.shutdown_grace <= 60:
            raise ValueError("shutdown_grace must be between 0 and 60 seconds")
        object.__setattr__(self, "denied_tools", frozenset(self.denied_tools))


class MCPServer(_Server):
    """SDK-compatible server with supervised stdio startup and tool dispatch."""

    def __init__(self, *args, supervision: MCPConfig | None = None, **kwargs):
        self.supervision = supervision or MCPConfig()
        self._mcp_proxy_active = False
        self._supervised_calls: set[asyncio.Task] = set()
        super().__init__(*args, **kwargs)

    async def list_tools(self):
        if self._mcp_proxy_active:
            return await super().list_tools()
        return [t for t in await super().list_tools() if t.name not in self.supervision.denied_tools]

    async def call_tool(self, name, arguments, *args, **kwargs):
        if self._mcp_proxy_active:
            return await super().call_tool(name, arguments, *args, **kwargs)
        config = self.supervision
        if name in config.denied_tools:
            raise ToolError(f"Tool {name!r} is denied by supervisor policy")
        if len(self._supervised_calls) >= config.max_concurrent_calls:
            raise ToolError("Supervisor concurrency limit reached; retry after active work finishes")
        started = time.monotonic()

        async def invoke():
            result = await super(MCPServer, self).call_tool(name, arguments, *args, **kwargs)
            if len(_SERIALIZER.dump_json(result)) > config.max_result_bytes:
                raise ToolError("Tool result exceeds supervisor max_result_bytes")
            return result

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
                # It retains its slot until it actually exits, even if it suppresses cancellation.
                raise ToolError("Tool deadline exceeded; cancellation requested, execution may still be running")
            return task.result()
        except asyncio.CancelledError:
            task.cancel()
            raise

    def run(self, transport="stdio", **kwargs):
        if transport != "stdio" or kwargs:
            raise ValueError("Supervised startup currently supports run() with stdio only")
        asyncio.run(self.run_stdio_async())

    async def run_stdio_async(self):
        # The marker is launcher coordination, not an authentication boundary.
        if os.environ.pop(_CHILD, None) == str(os.getppid()):
            self._mcp_proxy_active = True
            await super().run_stdio_async()
            return
        main = sys.modules.get("__main__")
        script = getattr(main, "__file__", None)
        if not script or not Path(script).is_file() or getattr(main, "__spec__", None) is not None:
            raise RuntimeError("Use a Python script with an if __name__ == '__main__' guard and server.run(); module runners and notebooks are not supported")
        config = self.supervision
        # Preserve Python's import paths as the native backend clears ambient env.
        env = {"PYTHONPATH": os.pathsep.join(os.path.abspath(p or '.') for p in sys.path),
               _CHILD: str(os.getpid())}
        names = list(os.environ) if config.inherit_env is None else list(config.inherit_env)
        names = [name for name in names if name != _CHILD]
        command = Command(
            argv=[sys.executable, str(Path(script).resolve()), *sys.argv[1:]],
            cwd=os.getcwd(), env=env, inherit_env=names,
            max_stdin_bytes=config.max_session_bytes,
            max_stdout_bytes=config.max_session_bytes,
            max_stderr_bytes=config.max_session_bytes,
        )
        token = CancellationToken()
        operation = asyncio.ensure_future(relay_stdio(command, policy=config.process_policy,
            cancellation=token, shutdown_grace=config.shutdown_grace, call_timeout=config.call_timeout,
            max_concurrent_calls=config.max_concurrent_calls, max_result_bytes=config.max_result_bytes,
            denied_tools=list(config.denied_tools)))
        previous = {}
        # signal.signal also works with Windows' default event loop. Preserve host handlers.
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

    async def run_sse_async(self, *args, **kwargs):
        raise ValueError("Supervised startup currently supports stdio only")

    async def run_streamable_http_async(self, *args, **kwargs):
        raise ValueError("Supervised startup currently supports stdio only")


# Selects the installed official SDK; this alias does not migrate SDK v1 APIs to v2.
FastMCP = MCPServer
__all__ = ["FastMCP", "MCPServer", "Context", "MCPConfig"]
