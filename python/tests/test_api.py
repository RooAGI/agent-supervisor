"""Integration tests for the public Python API."""

import asyncio
import os
import sys
import tempfile
import unittest

from agent_supervisor import (
    CancellationToken,
    Command,
    SupervisorError,
    run,
    stream,
)


def py_command(code, **kwargs):
    if os.name == "nt":
        kwargs.setdefault("inherit_env", ["SystemRoot", "WINDIR"])
    return Command(argv=[sys.executable, "-c", code], **kwargs)


class RunTests(unittest.IsolatedAsyncioTestCase):
    async def test_captures_stdout_and_stderr_as_bytes(self):
        result = await run(
            py_command("import sys; print('out'); print('err', file=sys.stderr)")
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
