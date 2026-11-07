"""Tests for the optional OpenAI Agents SDK Runner adapter."""

import asyncio
import os
import sys
import unittest
from unittest.mock import patch

try:
    from agent_supervisor.openai import Runner, current_run
    from agents import Runner as OpenAIRunner
except ImportError:
    Runner = None
    OpenAIRunner = None

from agent_supervisor import Command, Supervisor, run


def py_command(code, **kwargs):
    return Command(argv=[sys.executable, "-c", code], **kwargs)


@unittest.skipIf(Runner is None, "install agent-supervisor[openai]")
class OpenAIRunnerAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_routes_tool_commands_through_native_run(self):
        async def fake_run(*args, **kwargs):
            self.assertIsNotNone(current_run())
            result = await run(py_command("print('managed')"))
            self.assertEqual(result.stdout, b"managed" + os.linesep.encode())
            return "complete"

        with patch.object(OpenAIRunner, "run", side_effect=fake_run):
            result = await Runner.run(object(), "start", supervision={"shutdown_grace": 0})

        self.assertEqual(result, "complete")
        self.assertIsNone(current_run())

    async def test_timeout_cancels_rust_run_and_registered_processes(self):
        observed = {}

        async def fake_run(*args, **kwargs):
            observed["run"] = current_run()
            observed["process"] = await Supervisor().start(
                py_command("import time; time.sleep(30)", timeout=30)
            )
            await run(py_command("import time; time.sleep(30)", timeout=30))

        with patch.object(OpenAIRunner, "run", side_effect=fake_run):
            with self.assertRaises(asyncio.TimeoutError):
                await Runner.run(
                    object(),
                    "wait",
                    supervision={"timeout": 0.15, "shutdown_grace": 0},
                )

        self.assertIsNotNone(observed["run"])
        self.assertEqual(observed["run"].active_processes(), 0)

    async def test_stream_context_uses_run_and_closes_early_process(self):
        observed = {}

        async def fake_run(*args, **kwargs):
            observed["run"] = current_run()
            process = await Supervisor().start(
                py_command("import time; time.sleep(30)", timeout=30)
            )
            first = await process.__anext__()
            self.assertEqual(first.kind, "started")

        with patch.object(OpenAIRunner, "run", side_effect=fake_run):
            await Runner.run(object(), "stream", supervision={"shutdown_grace": 0})

        self.assertEqual(observed["run"].active_processes(), 0)

    async def test_watcher_sees_streamed_events_without_a_reader(self):
        class FakeStreamingResult:
            cancelled = False

            def cancel(self):
                self.cancelled = True

            async def stream_events(self):
                yield "first"
                yield "second"

        fake = FakeStreamingResult()
        supervisor = Supervisor()

        async def fake_streamed(*args, **kwargs):
            return fake

        with patch.object(OpenAIRunner, "run_streamed", side_effect=fake_streamed):
            result = await Runner.run_streamed(object(), "go", supervisor=supervisor)

        async def watch():
            return [e async for e in supervisor.stream(run=result.agent_run)]

        events = await asyncio.wait_for(watch(), 10)
        self.assertEqual(
            [e.payload for e in events if e.kind == "agent"], ["first", "second"]
        )
        self.assertEqual([e async for e in result.stream_events()], ["first", "second"])
        self.assertFalse(fake.cancelled)

    async def test_streamed_run_keeps_run_until_events_finish(self):
        class FakeStreamingResult:
            is_complete = False

            def cancel(self):
                self.is_complete = True

            async def stream_events(self):
                self.assert_run()
                yield "event"
                self.is_complete = True

            def assert_run(self):
                if current_run() is None:
                    raise AssertionError("stream events lost their agent run")

        streaming = FakeStreamingResult()

        async def fake_streamed(*args, **kwargs):
            self.assertIsNotNone(current_run())
            return streaming

        with patch.object(OpenAIRunner, "run_streamed", side_effect=fake_streamed):
            result = await Runner.run_streamed(
                object(), "stream", supervision={"shutdown_grace": 0}
            )
            events = [event async for event in result.stream_events()]

        self.assertEqual(events, ["event"])
        self.assertTrue(streaming.is_complete)
        self.assertIsNone(current_run())


if __name__ == "__main__":
    unittest.main()
