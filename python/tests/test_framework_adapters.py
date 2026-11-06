"""Lifecycle tests against local runs from the three supported frameworks."""

import asyncio
import importlib.util
import sys
import unittest

from agent_supervisor import Command, Supervisor, run as run_tool
from agent_supervisor._run_context import current_run
from agent_supervisor._lifecycle import run_context_manager, run_iterator


def command(code="import time; time.sleep(30)"):
    return Command(argv=[sys.executable, "-c", code], timeout=30)


class AgentRunContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_deadline_exits_native_stream_context_and_cleans_tools(self):
        observed = {}

        class ContextManager:
            async def __aenter__(self):
                observed["run"] = current_run()
                await Supervisor().start(command())
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                observed["exit_type"] = exc_type

        try:
            async with run_context_manager(
                lambda _run: ContextManager(), {"timeout": 0.05, "shutdown_grace": 0}
            ):
                await asyncio.sleep(30)
        except asyncio.TimeoutError:
            pass
        else:
            self.fail("run deadline should raise asyncio.TimeoutError")

        self.assertIs(observed["exit_type"], asyncio.CancelledError)
        self.assertEqual(observed["run"].active_processes(), 0)
        self.assertIsNone(current_run())


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
            observed["run"] = current_run()
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
        self.assertEqual(observed["run"].active_processes(), 0)
        self.assertIsNone(current_run())

    @unittest.skipUnless(importlib.util.find_spec("langgraph"), "install the langgraph extra")
    async def test_stream_keeps_run_during_node_execution(self):
        from typing import TypedDict

        from langgraph.graph import END, START, StateGraph

        from agent_supervisor.langgraph import stream

        observed = []

        class State(TypedDict):
            value: str

        async def node(state):
            observed.append(current_run() is not None)
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
        self.assertIsNone(current_run())


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
            observed["run"] = current_run()
            await Supervisor().start(command())
            return "tests completed"

        result = await run(agent, "Run tests", supervision={"shutdown_grace": 0})

        self.assertIsNotNone(result.output)
        self.assertIn("run", observed)
        self.assertEqual(observed["run"].active_processes(), 0)
        self.assertIsNone(current_run())

    @unittest.skipUnless(importlib.util.find_spec("pydantic_ai"), "install the pydantic-ai extra")
    async def test_agent_stream_context_cleans_up(self):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from agent_supervisor.pydantic_ai import run_stream

        agent = Agent(TestModel())
        async with run_stream(agent, "Say hello", supervision={"shutdown_grace": 0}) as result:
            self.assertIsNotNone(result)
            async for _text in result.stream_text():
                self.assertIsNotNone(current_run())
        self.assertIsNone(current_run())


class GoogleADKAdapterTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(
        importlib.util.find_spec("google")
        and importlib.util.find_spec("google.adk"),
        "install the google-adk extra",
    )
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
                observed.append(current_run())
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
        self.assertIsNone(current_run())

    @unittest.skipUnless(
        importlib.util.find_spec("google")
        and importlib.util.find_spec("google.adk"),
        "install the google-adk extra",
    )
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
        self.assertIsNone(current_run())


class WatchAgentStreamTests(unittest.IsolatedAsyncioTestCase):
    async def watch(self, supervisor, agent_run):
        async def read():
            return [event async for event in supervisor.stream(run=agent_run)]

        return await asyncio.wait_for(read(), 20)

    async def test_watcher_sees_agent_events_when_nobody_reads_the_stream(self):
        async def framework_stream():
            for number in range(3):
                yield number

        supervisor = Supervisor()
        stream = run_iterator(framework_stream(), supervisor=supervisor)

        events = await self.watch(supervisor, stream.agent_run)

        payloads = [event.payload for event in events if event.kind == "agent"]
        self.assertEqual(payloads, [0, 1, 2])
        self.assertEqual(events[-1].kind, "run_ended")
        self.assertEqual(supervisor.active_agent_runs(), 0)

    async def test_caller_still_receives_every_event_after_the_watcher(self):
        async def framework_stream():
            for number in range(3):
                yield number

        supervisor = Supervisor()
        stream = run_iterator(framework_stream(), supervisor=supervisor)
        await self.watch(supervisor, stream.agent_run)
        self.assertEqual([event async for event in stream], [0, 1, 2])

    async def test_closing_early_stops_the_framework_stream_and_ends_the_run(self):
        closed = asyncio.Event()

        async def framework_stream():
            try:
                yield "first"
                await asyncio.sleep(30)
                yield "never"
            finally:
                closed.set()

        supervisor = Supervisor()
        stream = run_iterator(framework_stream(), supervisor=supervisor)
        self.assertEqual(await stream.__anext__(), "first")
        await stream.aclose()
        self.assertTrue(closed.is_set())
        self.assertEqual(supervisor.active_agent_runs(), 0)

    async def test_run_deadline_reaches_the_caller_and_ends_the_run(self):
        async def framework_stream():
            yield "first"
            await asyncio.sleep(30)

        supervisor = Supervisor()
        stream = run_iterator(
            framework_stream(),
            {"timeout": 0.2, "shutdown_grace": 0},
            supervisor=supervisor,
        )
        self.assertEqual(await stream.__anext__(), "first")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(stream.__anext__(), 10)
        self.assertEqual(supervisor.active_agent_runs(), 0)

    async def test_tool_output_and_agent_events_share_one_stream(self):
        supervisor = Supervisor()

        async def framework_stream():
            yield "calling tool"
            # Started by a tool, inside the run this stream belongs to.
            await run_tool(command("print('tool output')"))
            yield "tool finished"

        stream = run_iterator(framework_stream(), supervisor=supervisor)
        events = await self.watch(supervisor, stream.agent_run)
        kinds = [event.kind for event in events]
        self.assertLess(kinds.index("agent"), kinds.index("stdout"))
        self.assertEqual(
            b"".join(e.data for e in events if e.kind == "stdout").strip(),
            b"tool output",
        )
        self.assertEqual(
            [e.payload for e in events if e.kind == "agent"],
            ["calling tool", "tool finished"],
        )

    @unittest.skipUnless(importlib.util.find_spec("langgraph"), "install the langgraph extra")
    async def test_real_langgraph_stream_reaches_the_watcher(self):
        from typing import TypedDict

        from langgraph.graph import END, START, StateGraph

        from agent_supervisor.langgraph import stream as stream_graph

        class State(TypedDict):
            value: int

        builder = StateGraph(State)
        builder.add_node("step", lambda state: {"value": state["value"] + 1})
        builder.add_edge(START, "step")
        builder.add_edge("step", END)

        supervisor = Supervisor()
        stream = stream_graph(builder.compile(), {"value": 1}, supervisor=supervisor)
        events = await self.watch(supervisor, stream.agent_run)
        self.assertIn(
            {"step": {"value": 2}},
            [event.payload for event in events if event.kind == "agent"],
        )

    @unittest.skipUnless(importlib.util.find_spec("pydantic_ai"), "install the pydantic-ai extra")
    async def test_real_pydantic_ai_events_reach_the_watcher(self):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from agent_supervisor.pydantic_ai import run_stream

        def lookup() -> str:
            """Return a value."""
            return "value"

        agent = Agent(TestModel(), tools=[lookup])
        supervisor = Supervisor()
        context = run_stream(agent, "Say hello", supervisor=supervisor)
        async with context as result:
            text = [chunk async for chunk in result.stream_text()]
        self.assertTrue(text)
        events = await self.watch(supervisor, context.agent_run)
        self.assertTrue([event for event in events if event.kind == "agent"])
