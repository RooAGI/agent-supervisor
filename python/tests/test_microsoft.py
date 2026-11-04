"""Tests for Microsoft Agent Framework run lifecycle integration."""

import asyncio
import importlib.util
import sys
import unittest

from agent_supervisor import Command, Supervisor, run
from agent_supervisor._run_scope import current_scope
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
        self.seen_scope = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed:
            raise StopAsyncIteration
        self.seen_scope = current_scope() is not None
        self.closed = True
        return "update"

    async def close(self, exc=None):
        self.closed = True


class MicrosoftAgentFrameworkTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(
        importlib.util.find_spec("agent_framework"),
        "install agent-supervisor[microsoft] to run the Microsoft SDK integration test",
    )
    async def test_official_agent_run_uses_the_shared_scope(self):
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
        self.assertIsNone(current_scope())

    async def test_run_tracks_tools_and_cleans_them_on_success(self):
        observed = {}

        async def operation(prompt):
            observed["scope"] = current_scope()
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
        self.assertIsNotNone(observed["scope"])
        self.assertEqual(observed["scope"].active_processes(), 0)
        self.assertIsNone(current_scope())

    async def test_timeout_cancels_run_and_stops_started_processes(self):
        observed = {}

        async def operation():
            observed["scope"] = current_scope()
            await Supervisor().start(command("import time; time.sleep(30)"))
            await asyncio.sleep(30)

        with self.assertRaises(asyncio.TimeoutError):
            await run_agent(
                FakeAgent(operation),
                supervision={"timeout": 0.1, "shutdown_grace": 0},
            )

        self.assertEqual(observed["scope"].active_processes(), 0)
        self.assertIsNone(current_scope())

    async def test_stream_keeps_scope_until_consumed_and_closes(self):
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
        self.assertIsNone(current_scope())
        async with managed as updates:
            self.assertEqual([item async for item in updates], ["update"])
        self.assertTrue(agent.response_stream.seen_scope)
        self.assertTrue(agent.response_stream.closed)
        self.assertIsNone(current_scope())


if __name__ == "__main__":
    unittest.main()
