"""Tests for Microsoft Agent Framework run lifecycle integration."""

import asyncio
import importlib.util
import sys
import unittest

from agent_supervisor import Command, Supervisor, run
from agent_supervisor._run_context import current_run
from agent_supervisor.microsoft import run as run_agent


def command(code):
    return Command(
        argv=[sys.executable, "-c", code],
        inherit_env=["SystemRoot", "WINDIR"],
        timeout=30,
    )


class FakeAgent:
    def __init__(self, operation):
        self.operation = operation

    async def run(self, *args, **kwargs):
        return await self.operation(*args, **kwargs)


class FakeResponseStream:
    def __init__(self):
        self.closed = False
        self.seen_run = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed:
            raise StopAsyncIteration
        self.seen_run = current_run() is not None
        self.closed = True
        return "update"

    async def close(self, exc=None):
        self.closed = True


class MicrosoftAgentFrameworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_response_waits_for_the_background_reader(self):
        class Stream:
            def __init__(self):
                self.remaining = ["a", "b"]
                self.read = []

            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.sleep(0.01)
                if not self.remaining:
                    raise StopAsyncIteration
                self.read.append(self.remaining.pop(0))
                return self.read[-1]

            async def get_final_response(self):
                return list(self.read)

        class Agent:
            def __init__(self):
                self.stream = Stream()

            def run(self, prompt, *, stream=False):
                return self.stream

        supervisor = Supervisor()
        updates = await run_agent(Agent(), "go", stream=True, supervisor=supervisor)
        self.assertEqual(await updates.get_final_response(), ["a", "b"])

        async def watch():
            return [e async for e in supervisor.stream(run=updates.agent_run)]

        events = await asyncio.wait_for(watch(), 10)
        self.assertEqual([e.payload for e in events if e.kind == "agent"], ["a", "b"])

    @unittest.skipUnless(
        importlib.util.find_spec("agent_framework"),
        "install agent-supervisor[microsoft] to run the Microsoft SDK integration test",
    )
    async def test_official_agent_run_uses_the_shared_run(self):
        from agent_framework import Agent, ChatResponse, Message

        class StubChatClient:
            additional_properties = {}

            def get_response(self, messages, **kwargs):
                async def response():
                    return ChatResponse(
                        messages=[Message("assistant", ["framework response"])],
                        finish_reason="stop",
                    )

                return response()

        agent = Agent(client=StubChatClient(), name="Lifecycle test agent")
        result = await run_agent(
            agent, "Respond", supervision={"shutdown_grace": 0}
        )

        self.assertEqual(result.text, "framework response")
        self.assertIsNone(current_run())

    async def test_run_tracks_tools_and_cleans_them_on_success(self):
        observed = {}

        async def operation(prompt):
            observed["run"] = current_run()
            observed["process"] = await Supervisor().start(
                command("import time; time.sleep(30)")
            )
            observed["tool_result"] = await run(command("print('tool output')"))
            return prompt

        result = await run_agent(
            FakeAgent(operation), "hello", supervision={"shutdown_grace": 0}
        )

        self.assertEqual(result, "hello")
        self.assertTrue(observed["tool_result"].success)
        self.assertEqual(observed["tool_result"].exit_code, 0)
        self.assertIsNotNone(observed["run"])
        self.assertEqual(observed["run"].active_processes(), 0)
        self.assertIsNone(current_run())

    async def test_timeout_cancels_run_and_stops_started_processes(self):
        observed = {}

        async def operation():
            observed["run"] = current_run()
            await Supervisor().start(command("import time; time.sleep(30)"))
            await asyncio.sleep(30)

        with self.assertRaises(asyncio.TimeoutError):
            await run_agent(
                FakeAgent(operation),
                supervision={"timeout": 0.1, "shutdown_grace": 0},
            )

        self.assertEqual(observed["run"].active_processes(), 0)
        self.assertIsNone(current_run())

    async def test_stream_keeps_run_until_consumed_and_closes(self):
        class Agent:
            def run(self, prompt, *, stream=False):
                self.prompt = prompt
                return self.response_stream

        agent = Agent()
        agent.response_stream = FakeResponseStream()
        managed = await run_agent(
            agent,
            "hello",
            stream=True,
            supervision={"shutdown_grace": 0},
        )
        self.assertIsNone(current_run())
        async with managed as updates:
            self.assertEqual([item async for item in updates], ["update"])
        self.assertTrue(agent.response_stream.seen_run)
        self.assertTrue(agent.response_stream.closed)
        self.assertIsNone(current_run())


if __name__ == "__main__":
    unittest.main()
