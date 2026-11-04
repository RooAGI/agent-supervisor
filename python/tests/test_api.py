"""Integration tests for the public Python API."""

import asyncio
import os
import sys
import tempfile
import unittest

from agent_supervisor import (
    AgentRunPolicy,
    AgentRunScope,
    CancellationToken,
    Command,
    Policy,
    Supervisor,
    SupervisorError,
    platform_capabilities,
    run,
    stream,
)
from agent_supervisor._run_scope import current_scope


def py_command(code, **kwargs):
    if os.name == "nt":
        kwargs.setdefault("inherit_env", ["SystemRoot", "WINDIR"])
    return Command(argv=[sys.executable, "-c", code], **kwargs)


class ConfigurationTests(unittest.TestCase):
    def test_agent_run_policy_defaults_and_validation(self):
        self.assertEqual(AgentRunPolicy(), AgentRunPolicy(300, 5))
        self.assertEqual(AgentRunPolicy(180, 3).timeout, 180.0)
        for timeout in (0, -1, float("nan"), float("inf"), "10"):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                AgentRunPolicy(timeout=timeout)
        for grace in (-1, 61, float("nan"), "1"):
            with self.subTest(shutdown_grace=grace), self.assertRaises(ValueError):
                AgentRunPolicy(shutdown_grace=grace)
        with self.assertRaises(ValueError):
            AgentRunPolicy(timeout=float("inf"))
        with self.assertRaises(ValueError):
            AgentRunPolicy(shutdown_grace=61)

    def test_policy_rejects_unknown_network_and_enforcement_values(self):
        with self.assertRaises(ValueError):
            Policy(network="private")
        with self.assertRaises(ValueError):
            Policy(enforcement="strict")

    def test_policy_rejects_unknown_filesystem_access(self):
        with self.assertRaises(ValueError):
            Policy(filesystem=[("/tmp", ["execute"])])

    def test_platform_capabilities_reports_documented_fields(self):
        capabilities = platform_capabilities()
        expected = {
            "containment",
            "host_network",
            "network_isolation",
            "network_isolation_requires_filesystem",
            "graceful_shutdown",
            "process_group_cleanup",
            "parent_death_cleanup",
            "filesystem_isolation",
            "memory_limits",
            "process_limits",
            "cpu_limits",
        }
        self.assertEqual(set(capabilities), expected)
        self.assertIn(
            capabilities["containment"],
            {"enforced", "degraded", "unavailable", "trusted"},
        )
        self.assertTrue(
            all(
                isinstance(value, bool)
                for key, value in capabilities.items()
                if key != "containment"
            )
        )


class RunTests(unittest.IsolatedAsyncioTestCase):
    async def test_agent_run_scope_context_manager_cleans_tools_after_success(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))
        async with scope:
            await scope.start(py_command("import time; time.sleep(30)", timeout=30))
            self.assertEqual(scope.active_processes(), 1)
        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_context_manager_cleans_tools_after_error(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))
        with self.assertRaisesRegex(RuntimeError, "agent failed"):
            async with scope:
                await scope.start(py_command("import time; time.sleep(30)", timeout=30))
                raise RuntimeError("agent failed")
        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_passes_through_operation_error_and_restores_context(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))

        async def operation():
            self.assertIs(current_scope(), scope)
            raise RuntimeError("framework failed")

        with self.assertRaisesRegex(RuntimeError, "framework failed"):
            await scope.run(operation())
        self.assertIsNone(current_scope())

    async def test_agent_run_scope_wraps_agent_future_and_cleans_tools(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))
        observed = {}

        async def operation():
            observed["scope"] = current_scope()
            result = await run(py_command("print('tool')"))
            observed["process"] = await scope.start(
                py_command("import time; time.sleep(30)", timeout=30)
            )
            return result

        result = await scope.run(operation())
        self.assertEqual(result.stdout, b"tool" + os.linesep.encode())
        self.assertIs(observed["scope"], scope)
        self.assertEqual(scope.active_processes(), 1)

        await scope.cancel()

        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_deadline_cancels_future_and_tools(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=0.05, shutdown_grace=0))

        async def operation():
            await scope.start(py_command("import time; time.sleep(30)", timeout=30))
            await asyncio.sleep(30)

        with self.assertRaises(asyncio.TimeoutError):
            await scope.run(operation())
        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_token_cancels_future_and_cleans_tools(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))
        token = scope.cancellation_token()

        async def operation():
            await scope.start(py_command("import time; time.sleep(30)", timeout=30))
            await asyncio.sleep(30)

        task = asyncio.create_task(scope.run(operation()))
        await asyncio.sleep(0.1)
        token.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_request_cancel_stops_operation_and_tools(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))

        async def operation():
            await scope.start(py_command("import time; time.sleep(30)", timeout=30))
            await asyncio.sleep(30)

        task = asyncio.create_task(scope.run(operation()))
        await asyncio.sleep(0.1)
        scope.request_cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_cancel_now_stops_process_group(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=5, shutdown_grace=0))
        await scope.start(py_command("import time; time.sleep(30)", timeout=30))
        self.assertEqual(scope.active_processes(), 1)

        scope.cancel_now()
        await scope.cancel()

        self.assertEqual(scope.active_processes(), 0)

    async def test_agent_run_scope_run_command_obeys_scope_deadline(self):
        scope = AgentRunScope(AgentRunPolicy(timeout=0.05, shutdown_grace=0))
        with self.assertRaisesRegex(SupervisorError, "agent run exceeded its deadline"):
            await scope.run_command(
                py_command("import time; time.sleep(30)", timeout=30)
            )

    async def test_captures_stdout_and_stderr_as_bytes(self):
        result = await run(
            py_command("import sys; print('out'); print('err', file=sys.stderr)"),
            policy=Policy(),
        )

        newline = os.linesep.encode()
        self.assertEqual(result.stdout, b"out" + newline)
        self.assertEqual(result.stderr, b"err" + newline)
        self.assertTrue(result.success)
        self.assertEqual(result.termination, "exited")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.stdout_text("utf-8", "strict"), "out" + os.linesep)

    async def test_nonzero_exit_is_a_result(self):
        result = await run(py_command("raise SystemExit(7)"))

        self.assertFalse(result.success)
        self.assertEqual(result.exit_code, 7)
        self.assertEqual(result.termination, "exited")

    async def test_deadline_is_reported(self):
        result = await run(py_command("import time; time.sleep(10)", timeout=0.1))

        self.assertEqual(result.termination, "timed_out")

    async def test_explicit_cancellation_returns_termination_result(self):
        token = CancellationToken()
        token.cancel()

        result = await run(
            py_command("import time; time.sleep(10)"), cancellation=token
        )

        self.assertTrue(token.cancelled())
        self.assertEqual(result.termination, "cancelled")

    async def test_output_limit_returns_bounded_result(self):
        result = await run(py_command("print('x' * 4096)", max_stdout_bytes=128))

        self.assertEqual(result.termination, "stdout_limit_exceeded")
        self.assertLessEqual(len(result.stdout), 128)

    async def test_stderr_is_bounded_independently(self):
        result = await run(
            py_command(
                "import sys; print('e' * 4096, file=sys.stderr)",
                max_stderr_bytes=128,
            )
        )

        self.assertEqual(result.termination, "stderr_limit_exceeded")
        self.assertLessEqual(len(result.stderr), 128)

    async def test_python_task_cancellation_waits_for_cleanup(self):
        task = asyncio.create_task(
            run(py_command("import time; time.sleep(10)", timeout=15))
        )
        await asyncio.sleep(0.1)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_streams_chunks_and_captures_bounded_result(self):
        seen = {"stdout": bytearray(), "stderr": bytearray()}
        result = None

        async with stream(
            py_command(
                "import sys; print('one', flush=True); "
                "print('two', file=sys.stderr, flush=True)"
            ),
            capture=True,
        ) as events:
            async for event in events:
                if event.kind in seen:
                    seen[event.kind].extend(event.data)
                elif event.kind == "exited":
                    result = event.result

        newline = os.linesep.encode()
        self.assertEqual(bytes(seen["stdout"]), b"one" + newline)
        self.assertEqual(bytes(seen["stderr"]), b"two" + newline)
        self.assertIsNotNone(result)
        self.assertEqual(result.stdout, bytes(seen["stdout"]))
        self.assertEqual(result.stderr, bytes(seen["stderr"]))
        self.assertEqual(result.termination, "exited")

    async def test_stream_without_capture_has_no_collected_output(self):
        result = None

        async with stream(py_command("print('live', flush=True)")) as events:
            async for event in events:
                if event.kind == "exited":
                    result = event.result

        self.assertIsNotNone(result)
        self.assertEqual(result.stdout, b"")


class PersistentProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_write_events_wait_and_result_capture(self):
        supervisor = Supervisor()
        process = await supervisor.start(
            py_command(
                "import sys; line=sys.stdin.buffer.readline(); "
                "sys.stdout.buffer.write(b'received:' + line); sys.stdout.flush(); "
                "sys.stderr.write('diagnostic'); sys.stderr.flush()"
            ),
            policy=Policy(),
        )

        await process.write(b"hello\n")
        await process.close_stdin()
        seen = []
        result = None
        async for event in process.events():
            seen.append(event.kind)
            if event.kind == "exited":
                result = event.result

        self.assertEqual(seen[0], "started")
        self.assertIn("stdout", seen)
        self.assertIn("stderr", seen)
        self.assertEqual(result.stdout, b"received:hello\n")
        self.assertEqual(result.stderr, b"diagnostic")
        self.assertEqual(result.termination, "exited")
        waited = await process.wait()
        self.assertEqual(waited.stdout, result.stdout)
        self.assertEqual(waited.termination, result.termination)

    async def test_wait_drains_events_and_returns_exit_status(self):
        process = await Supervisor().start(py_command("print('waited')"))

        result = await process.wait()

        self.assertEqual(result.stdout, b"waited" + os.linesep.encode())
        self.assertEqual(result.exit_code, 0)

    async def test_shutdown_stops_a_long_running_child(self):
        process = await Supervisor().start(
            py_command("import time; time.sleep(30)", timeout=30)
        )

        result = await process.shutdown(grace=0)

        self.assertIn(result.termination, {"timed_out", "exited"})
        if result.termination == "exited":
            self.assertIsNotNone(result.signal)

    async def test_stream_output_limit_raises_after_cleanup(self):
        with self.assertRaises(SupervisorError):
            async with stream(
                py_command("print('x' * 4096)", max_stdout_bytes=128)
            ) as events:
                async for _event in events:
                    pass

    async def test_stream_deadline_is_reported_in_terminal_result(self):
        result = None

        async with stream(
            py_command("import time; time.sleep(10)", timeout=0.1)
        ) as events:
            async for event in events:
                if event.kind == "exited":
                    result = event.result

        self.assertIsNotNone(result)
        self.assertEqual(result.termination, "timed_out")

    async def test_leaving_stream_context_cleans_up_child(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = os.path.join(directory, "survived")
            code = (
                "import pathlib, time; time.sleep(0.8); "
                f"pathlib.Path({marker!r}).write_text('alive')"
            )
            async with stream(py_command(code, timeout=5)) as events:
                first_event = await events.__anext__()
            self.assertEqual(first_event.kind, "started")
            await asyncio.sleep(1.0)
            self.assertFalse(os.path.exists(marker))


if __name__ == "__main__":
    unittest.main()
