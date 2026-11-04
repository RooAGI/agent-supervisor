"""Lifecycle tests against local runs from the three supported frameworks."""

import asyncio
import importlib.util
import sys
import unittest

from agent_supervisor import Command, Supervisor
from agent_supervisor._run_scope import current_scope
from agent_supervisor._lifecycle import scoped_context_manager


def command(code="import time; time.sleep(30)"):
    return Command(argv=[sys.executable, "-c", code], timeout=30)


class ScopedContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_deadline_exits_native_stream_context_and_cleans_tools(self):
        observed = {}

        class ContextManager:
            async def __aenter__(self):
                observed["scope"] = current_scope()
                await Supervisor().start(command())
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                observed["exit_type"] = exc_type

        with self.assertRaises(TimeoutError):
            async with scoped_context_manager(
                ContextManager(), {"timeout": 0.05, "shutdown_grace": 0}
            ):
                await asyncio.sleep(30)

        self.assertIs(observed["exit_type"], asyncio.CancelledError)
        self.assertEqual(observed["scope"].active_processes(), 0)
        self.assertIsNone(current_scope())


class LangGraphAdapterTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(importlib.util.find_spec("langgraph"), "install the langgraph extra")
    async def test_compiled_graph_run_tracks_and_cleans_tools(self):
        from typing import TypedDict

        from langgraph.graph import END, START, StateGraph

        from agent_supervisor.langgraph import run

        observed = {}

        class State(TypedDict):
            value: str

        async def node(state):
            observed["scope"] = current_scope()
            await Supervisor().start(command())
            return {"value": state["value"] + " done"}

        builder = StateGraph(State)
        builder.add_node("work", node)
        builder.add_edge(START, "work")
        builder.add_edge("work", END)
        result = await run(
            builder.compile(), {"value": "graph"}, supervision={"shutdown_grace": 0}
        )

        self.assertEqual(result["value"], "graph done")
        self.assertEqual(observed["scope"].active_processes(), 0)
        self.assertIsNone(current_scope())

    @unittest.skipUnless(importlib.util.find_spec("langgraph"), "install the langgraph extra")
    async def test_stream_keeps_scope_during_node_execution(self):
        from typing import TypedDict

        from langgraph.graph import END, START, StateGraph

        from agent_supervisor.langgraph import stream

        observed = []

        class State(TypedDict):
            value: str

        async def node(state):
            observed.append(current_scope() is not None)
            return {"value": state["value"] + " streamed"}

        builder = StateGraph(State)
        builder.add_node("work", node)
        builder.add_edge(START, "work")
        builder.add_edge("work", END)
        chunks = [chunk async for chunk in stream(
            builder.compile(), {"value": "graph"}, supervision={"shutdown_grace": 0}
        )]

        self.assertTrue(observed)
        self.assertTrue(all(observed))
        self.assertTrue(chunks)
        self.assertIsNone(current_scope())


class PydanticAIAdapterTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(importlib.util.find_spec("pydantic_ai"), "install the pydantic-ai extra")
    async def test_agent_tool_process_is_cleaned_after_run(self):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from agent_supervisor.pydantic_ai import run

        observed = {}
        agent = Agent(TestModel(call_tools=["run_project_tests"]))

        @agent.tool_plain
        async def run_project_tests() -> str:
            observed["scope"] = current_scope()
            await Supervisor().start(command())
            return "tests completed"

        result = await run(agent, "Run tests", supervision={"shutdown_grace": 0})

        self.assertIsNotNone(result.output)
        self.assertIn("scope", observed)
        self.assertEqual(observed["scope"].active_processes(), 0)
        self.assertIsNone(current_scope())

    @unittest.skipUnless(importlib.util.find_spec("pydantic_ai"), "install the pydantic-ai extra")
    async def test_agent_stream_context_cleans_up(self):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from agent_supervisor.pydantic_ai import run_stream

        agent = Agent(TestModel())
        async with run_stream(agent, "Say hello", supervision={"shutdown_grace": 0}) as result:
            self.assertIsNotNone(result)
            async for _text in result.stream_text():
                self.assertIsNotNone(current_scope())
        self.assertIsNone(current_scope())


class GoogleADKAdapterTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(importlib.util.find_spec("google.adk"), "install the google-adk extra")
    async def test_runner_events_track_and_clean_tool_processes(self):
        from google.adk.agents import BaseAgent
        from google.adk.events import Event
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types

        from agent_supervisor.google_adk import run

        observed = []

        class ProbeAgent(BaseAgent):
            async def _run_async_impl(self, context):
                observed.append(current_scope())
                await Supervisor().start(command())
                yield Event(author=self.name)

        sessions = InMemorySessionService()
        await sessions.create_session(app_name="lifecycle-test", user_id="user", session_id="session")
        runner = Runner(
            agent=ProbeAgent(name="probe"),
            app_name="lifecycle-test",
            session_service=sessions,
        )
        abort_signal = asyncio.Event()
        events = [
            event
            async for event in run(
                runner,
                user_id="user",
                session_id="session",
                new_message=types.Content(role="user", parts=[types.Part(text="go")]),
                abort_signal=abort_signal,
                supervision={"shutdown_grace": 0},
            )
        ]

        self.assertTrue(events)
        self.assertIsNotNone(observed[0])
        self.assertEqual(observed[0].active_processes(), 0)
        self.assertFalse(abort_signal.is_set())
        self.assertIsNone(current_scope())

    @unittest.skipUnless(importlib.util.find_spec("google.adk"), "install the google-adk extra")
    async def test_early_close_signals_adk_abort(self):
        from google.adk.agents import BaseAgent
        from google.adk.events import Event
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types

        from agent_supervisor.google_adk import run

        class ProbeAgent(BaseAgent):
            async def _run_async_impl(self, context):
                yield Event(author=self.name)
                await asyncio.sleep(30)

        sessions = InMemorySessionService()
        await sessions.create_session(app_name="early-close", user_id="user", session_id="session")
        runner = Runner(
            agent=ProbeAgent(name="probe"),
            app_name="early-close",
            session_service=sessions,
        )
        abort_signal = asyncio.Event()
        updates = run(
            runner,
            user_id="user",
            session_id="session",
            new_message=types.Content(role="user", parts=[types.Part(text="go")]),
            abort_signal=abort_signal,
            supervision={"shutdown_grace": 0},
        )
        await updates.__anext__()
        await updates.aclose()

        self.assertTrue(abort_signal.is_set())
        self.assertIsNone(current_scope())
