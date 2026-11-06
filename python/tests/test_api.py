"""Integration tests for the public Python API."""

import asyncio
import gc
import json
import os
import sys
import tempfile
import unittest

from agent_supervisor import (
    AgentRunPolicy,
    AgentRun,
    CancellationToken,
    Command,
    Policy,
    Supervisor,
    SupervisorProcessId,
    SupervisorError,
    platform_capabilities,
    run,
)
from agent_supervisor._run_context import current_run


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
    async def test_agent_run_context_manager_cleans_tools_after_success(self):
        supervisor = Supervisor()
        run = supervisor.start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))
        self.assertIsInstance(run, AgentRun)
        self.assertEqual(supervisor.active_agent_runs(), 1)
        async with run:
            await run.start(py_command("import time; time.sleep(30)", timeout=30))
            self.assertEqual(run.active_processes(), 1)
            self.assertEqual(supervisor.active_processes(), 1)
        self.assertEqual(run.active_processes(), 0)
        self.assertEqual(supervisor.active_agent_runs(), 0)

    async def test_agent_run_context_manager_cleans_tools_after_error(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))
        with self.assertRaisesRegex(RuntimeError, "agent failed"):
            async with run:
                await run.start(py_command("import time; time.sleep(30)", timeout=30))
                raise RuntimeError("agent failed")
        self.assertEqual(run.active_processes(), 0)

    async def test_agent_run_passes_through_operation_error_and_restores_context(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))

        async def operation():
            self.assertIs(current_run(), run)
            raise RuntimeError("framework failed")

        with self.assertRaisesRegex(RuntimeError, "framework failed"):
            await run.run(operation())
        self.assertIsNone(current_run())

    async def test_agent_run_wraps_agent_future_and_cleans_tools(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))
        observed = {}

        async def operation():
            observed["run"] = current_run()
            result = await run.run_command(py_command("print('tool')"))
            observed["process"] = await run.start(
                py_command("import time; time.sleep(30)", timeout=30)
            )
            return result

        result = await run.run(operation())
        self.assertEqual(result.stdout, b"tool" + os.linesep.encode())
        self.assertIs(observed["run"], run)
        self.assertEqual(run.active_processes(), 1)

        await run.cancel()

        self.assertEqual(run.active_processes(), 0)

    async def test_agent_run_deadline_cancels_future_and_tools(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=0.05, shutdown_grace=0))

        async def operation():
            await run.start(py_command("import time; time.sleep(30)", timeout=30))
            await asyncio.sleep(30)

        with self.assertRaises(asyncio.TimeoutError):
            await run.run(operation())
        self.assertEqual(run.active_processes(), 0)

    async def test_agent_run_token_cancels_future_and_cleans_tools(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))
        token = run.cancellation_token()

        async def operation():
            await run.start(py_command("import time; time.sleep(30)", timeout=30))
            await asyncio.sleep(30)

        task = asyncio.create_task(run.run(operation()))
        await asyncio.sleep(0.1)
        token.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(run.active_processes(), 0)

    async def test_agent_run_request_cancel_stops_operation_and_tools(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))

        async def operation():
            await run.start(py_command("import time; time.sleep(30)", timeout=30))
            await asyncio.sleep(30)

        task = asyncio.create_task(run.run(operation()))
        await asyncio.sleep(0.1)
        run.request_cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(run.active_processes(), 0)

    async def test_agent_run_cancel_now_stops_process_group(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=5, shutdown_grace=0))
        await run.start(py_command("import time; time.sleep(30)", timeout=30))
        self.assertEqual(run.active_processes(), 1)

        run.cancel_now()
        await run.cancel()

        self.assertEqual(run.active_processes(), 0)

    async def test_agent_run_run_command_obeys_run_deadline(self):
        run = Supervisor().start_agent_run(AgentRunPolicy(timeout=0.05, shutdown_grace=0))
        with self.assertRaisesRegex(SupervisorError, "agent run exceeded its deadline"):
            await run.run_command(
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


class PersistentProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_id_token_round_trips_through_json(self):
        supervisor = Supervisor()
        process_id = await supervisor.start_background(
            py_command("import time; time.sleep(0.05)")
        )

        token = process_id.token
        decoded = SupervisorProcessId.from_token(json.loads(json.dumps(token)))
        self.assertEqual(decoded, process_id)
        self.assertEqual(hash(decoded), hash(process_id))
        self.assertEqual(decoded.value, process_id.value)
        self.assertEqual(decoded.token, token)
        await supervisor.wait_process(process_id)

    async def test_foreign_process_id_cannot_address_new_supervisors_job(self):
        old_supervisor = Supervisor()
        old_id = await old_supervisor.start_background(
            py_command("print('old')")
        )
        await old_supervisor.wait_process(old_id)

        supervisor = Supervisor()
        new_id = await supervisor.start_background(
            py_command("import time; time.sleep(30)")
        )
        self.assertEqual(old_id.value, new_id.value)
        self.assertNotEqual(old_id, new_id)
        self.assertIsNone(supervisor.process_status(old_id))
        self.assertIsNone(supervisor.process_result(old_id))
        self.assertIsNone(await supervisor.wait_process(old_id))
        self.assertEqual(
            await supervisor.shutdown_process(old_id, grace=0.1),
            "not_found",
        )
        self.assertEqual(supervisor.process_status(new_id), "running")
        await supervisor.shutdown_process(new_id, grace=0.1)
        await supervisor.wait_process(new_id)

    async def test_background_job_captures_result_and_result_lifecycle(self):
        supervisor = Supervisor()
        process_id = await supervisor.start_background(
            py_command(
                "import sys, time; time.sleep(0.05); print('out'); "
                "print('err', file=sys.stderr); "
                "raise SystemExit(7)"
            )
        )

        self.assertEqual(int(process_id), process_id.value)
        self.assertEqual(supervisor.process_status(process_id), "running")
        result = await supervisor.wait_process(process_id)

        self.assertEqual(supervisor.process_status(process_id), "completed")
        self.assertEqual(result.stdout, b"out" + os.linesep.encode())
        self.assertEqual(result.stderr, b"err" + os.linesep.encode())
        self.assertEqual(result.exit_code, 7)
        self.assertEqual(result.termination, "exited")
        self.assertIsNone(result.error)
        self.assertEqual(supervisor.process_result(process_id).stdout, result.stdout)
        self.assertTrue(supervisor.forget_process(process_id))
        self.assertIsNone(supervisor.process_status(process_id))
        self.assertIsNone(await supervisor.wait_process(process_id))

    async def test_background_job_stdin_and_output_limits_are_managed(self):
        supervisor = Supervisor()
        process_id = await supervisor.start_background(
            py_command(
                "import sys; data=sys.stdin.buffer.read(); "
                "sys.stdout.buffer.write(b'received:' + data)"
            )
        )
        await supervisor.write_process_stdin(process_id, b"hello")
        await supervisor.close_process_stdin(process_id)
        result = await supervisor.wait_process(process_id)
        self.assertEqual(result.stdout, b"received:hello")
        self.assertEqual(result.termination, "exited")

        limited_id = await supervisor.start_background(
            py_command("print('x' * 4096 + 'end')", max_stdout_bytes=128)
        )
        limited = await supervisor.wait_process(limited_id)
        self.assertEqual(limited.termination, "exited")
        self.assertTrue(limited.success)
        self.assertTrue(limited.output_truncated)
        self.assertEqual(len(limited.stdout), 128)
        self.assertIn(b"end", limited.stdout)

    async def test_stopping_one_background_job_keeps_sibling_running(self):
        supervisor = Supervisor()
        cancelled_id = await supervisor.start_background(
            py_command("import time; time.sleep(30)", timeout=30)
        )
        sibling_id = await supervisor.start_background(
            py_command("import time; time.sleep(0.2); print('sibling')", timeout=5)
        )

        stop = await supervisor.shutdown_process(cancelled_id, grace=0.2)
        self.assertIn(
            stop,
            {"graceful_stop_requested", "force_stop_requested", "already_exited"},
        )
        sibling = await supervisor.wait_process(sibling_id)

        self.assertEqual(sibling.stdout, b"sibling" + os.linesep.encode())
        self.assertEqual(sibling.termination, "exited")
        cancelled = await supervisor.wait_process(cancelled_id)
        self.assertIn(cancelled.termination, {"cancelled", "exited"})

    async def test_shutdown_all_returns_and_consumes_background_results(self):
        supervisor = Supervisor()
        first = await supervisor.start_background(
            py_command("import time; time.sleep(30)", timeout=30)
        )
        second = await supervisor.start_background(
            py_command("import time; time.sleep(30)", timeout=30)
        )

        results = await supervisor.shutdown_all(grace=0.2)

        self.assertEqual({int(process_id) for process_id, _ in results}, {int(first), int(second)})
        results_by_id = dict(results)
        self.assertIn(first, results_by_id)
        self.assertIn(second, results_by_id)
        self.assertTrue(all(result.termination in {"cancelled", "exited"} for _, result in results))
        self.assertIsNone(supervisor.process_status(first))
        self.assertIsNone(supervisor.process_status(second))

    async def test_dropping_supervisor_cleans_up_background_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            ready = os.path.join(directory, "descendant-ready")
            marker = os.path.join(directory, "descendant-survived")
            child_code = (
                "import pathlib, time; time.sleep(0.4); "
                f"pathlib.Path({marker!r}).write_text('alive')"
            )
            code = (
                "import pathlib, subprocess, sys, time; "
                f"child_code = {child_code!r}; "
                "subprocess.Popen([sys.executable, '-c', child_code]); "
                f"pathlib.Path({ready!r}).write_text('ready'); "
                "time.sleep(30)"
            )
            supervisor = Supervisor()
            await supervisor.start_background(py_command(code, timeout=5))
            deadline = asyncio.get_running_loop().time() + 2.0
            while not os.path.exists(ready):
                if asyncio.get_running_loop().time() >= deadline:
                    self.fail("descendant did not start before owner drop")
                await asyncio.sleep(0.01)
            del supervisor
            gc.collect()
            await asyncio.sleep(0.7)
            self.assertFalse(os.path.exists(marker))

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


class WatcherTests(unittest.IsolatedAsyncioTestCase):
    async def collect(self, stream):
        async def read():
            return [event async for event in stream]

        return await asyncio.wait_for(read(), 10)

    def output(self, events, kind="stdout"):
        return b"".join(event.data for event in events if event.kind == kind)

    async def test_background_job_streams_live_and_ends_on_exit(self):
        supervisor = Supervisor()
        job = await supervisor.start_background(
            py_command(
                "import sys, time; print('first', flush=True); "
                "print('oops', file=sys.stderr, flush=True); time.sleep(0.3); "
                "print('second', flush=True)"
            )
        )
        stream = supervisor.stream(process=job)
        early = []
        while not early or early[-1].kind != "stdout":
            early.append(await asyncio.wait_for(stream.__anext__(), 10))
        self.assertEqual(early[-1].data.strip(), b"first")
        self.assertEqual(early[-1].process_id, job)
        self.assertEqual(supervisor.process_status(job), "running")

        rest = await self.collect(stream)
        self.assertEqual(self.output(rest).strip(), b"second")
        # stdout and stderr are read separately, so they are not ordered
        # against each other.
        self.assertEqual(self.output(early + rest, "stderr").strip(), b"oops")
        self.assertEqual(rest[-1].kind, "exited")
        self.assertEqual(rest[-1].result.termination, "exited")

    async def test_late_watcher_replays_retained_events(self):
        supervisor = Supervisor()
        job = await supervisor.start_background(py_command("print('done')"))
        await supervisor.wait_process(job)
        events = await self.collect(supervisor.stream(process=job))
        self.assertEqual(events[0].kind, "started")
        self.assertEqual(self.output(events).strip(), b"done")
        self.assertEqual(events[-1].kind, "exited")

    async def test_stopping_a_watcher_does_not_stop_the_job(self):
        supervisor = Supervisor()
        job = await supervisor.start_background(
            py_command("import time; time.sleep(0.3); print('survived')")
        )
        stream = supervisor.stream(process=job)
        await asyncio.wait_for(stream.__anext__(), 10)
        del stream
        result = await supervisor.wait_process(job)
        self.assertEqual(result.stdout.strip(), b"survived")

    async def test_run_scope_shows_agent_events_and_tools_in_one_order(self):
        supervisor = Supervisor()
        run_ = supervisor.start_agent_run(AgentRunPolicy())
        sibling = supervisor.start_agent_run(AgentRunPolicy())
        stream = supervisor.stream(run=run_)
        marker = {"framework": "event"}

        async def operation():
            run_.publish_agent_event(marker)
            await run(py_command("print('one-shot')", timeout=10))
            process = await supervisor.start(py_command("print('interactive')"))
            await process.wait()

        async with run_:
            await run_.run(operation())
        await sibling.cancel()

        events = await self.collect(stream)
        self.assertEqual(events[0].kind, "run_started")
        self.assertEqual(events[1].kind, "agent")
        self.assertIs(events[1].payload, marker)
        self.assertEqual(
            self.output(events).split(), [b"one-shot", b"interactive"]
        )
        self.assertEqual(events[-1].kind, "run_ended")
        self.assertEqual({event.run_id for event in events}, {run_.id})
        sequence = [event.seq for event in events]
        self.assertEqual(sequence, sorted(sequence))

    async def test_scope_arguments_are_exclusive(self):
        supervisor = Supervisor()
        run_ = supervisor.start_agent_run(AgentRunPolicy())
        job = await supervisor.start_background(py_command("pass"))
        with self.assertRaises(ValueError):
            supervisor.stream(run=run_, process=job)
        await run_.cancel()

    async def test_watcher_is_empty_for_another_supervisors_work(self):
        supervisor = Supervisor()
        other = Supervisor()
        foreign_run = other.start_agent_run(AgentRunPolicy())
        self.assertEqual(await self.collect(supervisor.stream(run=foreign_run)), [])
        await foreign_run.cancel()

    async def test_watch_everything_sees_each_process(self):
        supervisor = Supervisor()
        first = await supervisor.start_background(py_command("print('a')"))
        second = await supervisor.start_background(py_command("print('b')"))
        await supervisor.wait_process(first)
        await supervisor.wait_process(second)
        stream = supervisor.stream()
        seen = set()
        while len(seen) < 2:
            event = await asyncio.wait_for(stream.__anext__(), 10)
            if event.kind == "exited":
                seen.add(event.process_id)
        self.assertEqual(seen, {first, second})


if __name__ == "__main__":
    unittest.main()
