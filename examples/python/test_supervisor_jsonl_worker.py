"""Protocol smoke tests for the framework-neutral FlowRun worker fixture."""

import json
import queue
import subprocess
import sys
import threading
import unittest
from pathlib import Path

WORKER = Path(__file__).with_name("supervisor_jsonl_worker.py")


class JsonlWorkerTests(unittest.TestCase):
    def start_worker(self, duration_ms: int):
        process = subprocess.Popen(
            [sys.executable, "-u", str(WORKER)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        events: queue.Queue[dict] = queue.Queue()

        def read_events() -> None:
            for line in process.stdout:
                events.put(json.loads(line))

        threading.Thread(target=read_events, daemon=True).start()
        process.stdin.write(json.dumps({
            "type": "roo.runtime.worker.start",
            "protocolVersion": 1,
            "attemptId": "attempt-fixture-7",
            "generation": 3,
            "invocation": {"durationMs": duration_ms},
        }) + "\n")
        process.stdin.flush()
        return process, events

    def test_worker_emits_correlated_running_then_success_with_increasing_cursor(self) -> None:
        process, events = self.start_worker(0)
        try:
            started = events.get(timeout=3)
            completed = events.get(timeout=3)
            self.assertEqual(started["status"], "running")
            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(started["attemptId"], completed["attemptId"])
            self.assertEqual(started["generation"], completed["generation"])
            self.assertLess(started["sourceCursor"], completed["sourceCursor"])
            self.assertIn("summary", completed["output"])
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()

    def test_worker_honors_matching_cancel_and_emits_terminal_evidence(self) -> None:
        process, events = self.start_worker(30_000)
        try:
            started = events.get(timeout=3)
            self.assertEqual(started["status"], "running")
            assert process.stdin is not None
            process.stdin.write(json.dumps({"type": "cancel"}) + "\n")
            process.stdin.flush()
            cancelled = events.get(timeout=3)
            self.assertEqual(cancelled["status"], "cancelled")
            self.assertEqual(cancelled["attemptId"], started["attemptId"])
            self.assertEqual(cancelled["generation"], started["generation"])
            self.assertGreater(cancelled["sourceCursor"], started["sourceCursor"])
            self.assertEqual(cancelled["reasonCode"], "worker_cancelled")
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()

    def test_worker_ignores_cancel_for_a_different_attempt_generation(self) -> None:
        process, events = self.start_worker(100)
        try:
            started = events.get(timeout=3)
            assert process.stdin is not None
            process.stdin.write(json.dumps({
                "type": "cancel",
                "attemptId": "stale-attempt",
                "generation": started["generation"],
            }) + "\n")
            process.stdin.flush()
            completed = events.get(timeout=3)
            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(completed["attemptId"], started["attemptId"])
            self.assertGreater(completed["sourceCursor"], started["sourceCursor"])
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            process.stdin.close()
            process.stdout.close()
            process.stderr.close()


if __name__ == "__main__":
    unittest.main()
