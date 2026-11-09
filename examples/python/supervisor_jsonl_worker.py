#!/usr/bin/env python3
"""Framework-neutral JSONL worker fixture for Agent Supervisor FlowRuns.

This example intentionally uses only the Python standard library. The service
starts one copy per task attempt, writes a start envelope to stdin, and later
writes a cooperative cancel command to that same stdin stream.
"""

from __future__ import annotations

import json
import queue
import sys
import threading
import time
from typing import Any, Dict, Optional

MAX_LINE_BYTES = 1_048_576


def emit(event: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, separators=(",", ":"), ensure_ascii=True) + "\n")
    sys.stdout.flush()


def read_commands(commands: queue.Queue[Optional[Dict[str, Any]]]) -> None:
    while True:
        line = sys.stdin.readline(MAX_LINE_BYTES + 1)
        if not line:
            commands.put(None)
            return
        if len(line.encode("utf-8")) > MAX_LINE_BYTES:
            commands.put({"type": "invalid"})
            return
        try:
            command = json.loads(line)
        except json.JSONDecodeError:
            commands.put({"type": "invalid"})
            return
        if isinstance(command, dict):
            commands.put(command)
        else:
            commands.put({"type": "invalid"})


def main() -> int:
    line = sys.stdin.readline(MAX_LINE_BYTES + 1)
    if not line or len(line.encode("utf-8")) > MAX_LINE_BYTES:
        print("expected one bounded roo.runtime.worker.start JSONL envelope", file=sys.stderr)
        return 2
    try:
        envelope = json.loads(line)
    except json.JSONDecodeError as error:
        print(f"invalid start envelope: {error}", file=sys.stderr)
        return 2
    if not isinstance(envelope, dict) or envelope.get("type") != "roo.runtime.worker.start":
        print("unsupported worker envelope type", file=sys.stderr)
        return 2
    if envelope.get("protocolVersion") != 1:
        print("unsupported worker protocol version", file=sys.stderr)
        return 2
    attempt_id = envelope.get("attemptId")
    generation = envelope.get("generation")
    if (
        not isinstance(attempt_id, str)
        or not attempt_id
        or not isinstance(generation, int)
        or generation < 1
    ):
        print("start envelope requires attemptId and positive generation", file=sys.stderr)
        return 2

    invocation = envelope.get("invocation")
    if not isinstance(invocation, dict):
        invocation = {}
    duration_ms = invocation.get("durationMs", 1_000)
    if not isinstance(duration_ms, int) or isinstance(duration_ms, bool):
        duration_ms = 1_000
    duration_ms = min(max(duration_ms, 0), 60_000)
    cancel_delay_ms = invocation.get("cancelDelayMs", 0)
    if not isinstance(cancel_delay_ms, int) or isinstance(cancel_delay_ms, bool):
        cancel_delay_ms = 0
    cancel_delay_ms = min(max(cancel_delay_ms, 0), 10_000)

    emit({
        "type": "roo.runtime.worker.started",
        "attemptId": attempt_id,
        "generation": generation,
        "sourceCursor": 1,
        "status": "running",
        "reasonCode": "fixture_started",
        "output": {"message": "framework-neutral worker started"},
    })

    commands: queue.Queue[Optional[Dict[str, Any]]] = queue.Queue()
    reader = threading.Thread(target=read_commands, args=(commands,), daemon=True)
    reader.start()
    deadline = time.monotonic() + duration_ms / 1_000
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            emit({
                "type": "roo.runtime.worker.completed",
                "attemptId": attempt_id,
                "generation": generation,
                "sourceCursor": 2,
                "status": "succeeded",
                "reasonCode": "fixture_completed",
                "output": {"summary": "fixture work completed", "durationMs": duration_ms},
            })
            return 0
        try:
            command = commands.get(timeout=remaining)
        except queue.Empty:
            continue
        if command is None:
            print("worker control stream closed before completion", file=sys.stderr)
            return 2
        if command.get("type") == "cancel":
            # The current host sends {type: cancel}; newer senders may also
            # attach attempt identity. If present, reject stale cancellation.
            if command.get("attemptId", attempt_id) != attempt_id:
                continue
            if command.get("generation", generation) != generation:
                continue
            if cancel_delay_ms:
                time.sleep(cancel_delay_ms / 1_000)
            emit({
                "type": "roo.runtime.worker.completed",
                "attemptId": attempt_id,
                "generation": generation,
                "sourceCursor": 2,
                "status": "cancelled",
                "reasonCode": "worker_cancelled",
                "output": {"summary": "cooperative cancellation acknowledged"},
            })
            return 0
        if command.get("type") == "invalid":
            print("worker control input must be a JSON object per line", file=sys.stderr)
            return 2


if __name__ == "__main__":
    raise SystemExit(main())
