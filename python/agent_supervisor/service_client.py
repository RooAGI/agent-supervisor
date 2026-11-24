"""Async client for the authenticated Agent Supervisor Unix service.

This module covers process lifecycle and process-event streaming, run/activity/
attempt reporting, process-scoped output access, and Supervisor-owned FlowRun
registration and reconciliation.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Literal


MAX_FRAME_BYTES = 16 * 1024 * 1024
ServiceRole = Literal["view", "control", "tui", "reporter", "canceller", "output"]
ExecutionStatus = Literal["queued", "running", "succeeded", "failed", "cancelled", "blocked", "unknown"]
ActivityStatus = Literal["queued", "running", "waitingForInput", "waitingForApproval", "succeeded", "failed", "cancelled", "timedOut", "blocked", "unknown"]
Health = Literal["unknown", "healthy", "stale"]
CancellationState = Literal["none", "requested", "delivered", "completed", "failed"]


class SupervisorServiceError(RuntimeError):
    """Base error raised by the remote Supervisor service client."""


class SupervisorServiceDisconnected(SupervisorServiceError):
    """The service connection closed before a response or process event."""


class SupervisorServiceRejected(SupervisorServiceError):
    """The service rejected a request, with its protocol error code."""

    def __init__(self, code: str, message: str):
        super().__init__(f"Supervisor service returned {code}: {message}")
        self.code = code


@dataclass(frozen=True)
class SupervisorServiceSnapshot:
    manager_id: str
    epoch: str
    cursor: int
    counts: dict[str, int]
    processes: list["ServiceProcessRecord"]
    runs: list["ServiceRunRecord"]
    activities: list["ServiceActivityRecord"]
    attempts: list["ServiceAttemptRecord"]
    flow_runs: list[dict[str, Any]]


@dataclass(frozen=True)
class RegisteredAttempt:
    attempt_id: str
    generation: int
    reporter_token: str
    cancel_token: str

    def __repr__(self) -> str:
        return (f"RegisteredAttempt(attempt_id={self.attempt_id!r}, generation={self.generation!r}, "
                "reporter_token='[REDACTED]', cancel_token='[REDACTED]')")


@dataclass(frozen=True)
class CoordinatorEvent:
    sequence: int
    cursor: int
    epoch: str
    kind: str
    run_id: str | None
    activity_id: str | None
    attempt_id: str | None
    process_id: str | None
    revision: int
    occurred_at_ms: int
    detail: Any


@dataclass(frozen=True)
class SubscriptionStart:
    epoch: str
    cursor: int
    resync_required: bool
    events: list[CoordinatorEvent]
    snapshot: SupervisorServiceSnapshot | None


@dataclass(frozen=True)
class CoordinatorResyncRequired:
    epoch: str
    cursor: int
    snapshot: SupervisorServiceSnapshot | None


@dataclass(frozen=True)
class SupervisorProcessEvent:
    type: str
    process_id: str
    payload: dict[str, Any]
    data: bytes | None = None


@dataclass(frozen=True)
class ServiceRunRecord:
    run_id: str
    manager_id: str
    status: ExecutionStatus
    context: dict[str, Any]
    revision: int
    updated_at_ms: int


@dataclass(frozen=True)
class ServiceActivityRecord:
    activity_id: str
    run_id: str
    kind: str
    status: ActivityStatus
    health: Health
    last_contact_at_ms: int | None
    waiting_reason: str | None
    revision: int
    updated_at_ms: int


@dataclass(frozen=True)
class ServiceAttemptRecord:
    attempt_id: str
    activity_id: str
    run_id: str
    generation: int
    status: ExecutionStatus
    cancellation: CancellationState
    process_ids: list[str]
    reports: list[dict[str, Any]]
    revision: int
    updated_at_ms: int


@dataclass(frozen=True)
class ServiceProcessRecord:
    process_id: str
    run_id: str | None
    attempt_id: str | None
    alive: bool
    exit_status: str | None
    revision: int
    updated_at_ms: int


@dataclass(frozen=True)
class ServiceSubscriptionMessage:
    event: CoordinatorEvent | None = None
    resync_required: CoordinatorResyncRequired | None = None


@dataclass
class SupervisorServiceProcess:
    """An attached live process stream; reconnect with `attach_process` after disconnect."""

    _client: "SupervisorServiceClient"
    process_id: str
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter
    output_token: str | None = field(default=None, repr=False)

    async def next_event(self) -> SupervisorProcessEvent:
        value = await _read_json_line(self._reader)
        if value is None:
            raise SupervisorServiceDisconnected("process event stream disconnected")
        if value.get("type") == "error":
            raise _rejected(value)
        process_id = value.get("processId")
        if process_id != self.process_id:
            raise SupervisorServiceError("process event belongs to another process")
        event_type = str(value.get("type", "unknown"))
        data = None
        if event_type in ("stdout", "stderr"):
            try:
                data = base64.b64decode(value.get("data", ""), validate=True)
            except (ValueError, TypeError) as error:
                raise SupervisorServiceError("invalid base64 in process event") from error
        return SupervisorProcessEvent(event_type, process_id, value, data)

    async def events(self) -> AsyncIterator[SupervisorProcessEvent]:
        while True:
            event = await self.next_event()
            yield event
            if event.type == "exited":
                return

    async def stop(self, grace_ms: int = 1500) -> dict[str, Any]:
        return await self._client.stop_process(self.process_id, grace_ms)

    async def write_stdin(self, data: bytes) -> None:
        await self._client.write_stdin(self.process_id, data)

    async def close_stdin(self) -> None:
        await self._client.close_stdin(self.process_id)

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()


@dataclass
class SupervisorServiceSubscription:
    start: SubscriptionStart
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter

    async def next_message(self) -> ServiceSubscriptionMessage:
        value = await _read_json_line(self._reader)
        if value is None:
            raise SupervisorServiceDisconnected("coordinator event stream disconnected")
        kind = value.get("type")
        if kind == "coordinatorEvent":
            event = _coordinator_event(value.get("event", value))
            return ServiceSubscriptionMessage(event=event)
        if kind == "resyncRequired":
            snapshot = value.get("snapshot")
            return ServiceSubscriptionMessage(resync_required=CoordinatorResyncRequired(
                str(value.get("epoch", "")), int(value.get("cursor", 0)),
                _snapshot(snapshot) if isinstance(snapshot, dict) else None
            ))
        if kind == "error":
            raise _rejected(value)
        raise SupervisorServiceError(f"unexpected coordinator subscription frame {kind!r}")

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()


class SupervisorServiceClient:
    """Reusable client for process and coordinator APIs on a local service.

    Each one-shot operation uses a fresh authenticated Unix connection. A
    process start/attach keeps its connection open for events. If that stream
    disconnects, call `attach_process(process_id)` to receive retained process
    history. `view` reads metadata only; a start result's output token can be
    used with the `output` role to attach to that one process. TUI has metadata,
    output, and targeted-stop access. Coordinator subscriptions carry an epoch and monotonic cursor;
    callers must install the subscription before applying its replay, then
    snapshot again when the service signals a cursor gap or epoch change.
    Coordinator state is service-lifetime state until durable recovery is added.
    """

    def __init__(self, socket_path: str | os.PathLike[str], token: str, role: ServiceRole):
        self.socket_path = os.fspath(socket_path)
        self.token = token
        self.role = role

    @classmethod
    async def connect(
        cls,
        socket_path: str | os.PathLike[str],
        *,
        token: str | None = None,
        token_path: str | os.PathLike[str] | None = None,
        role: ServiceRole = "control",
    ) -> "SupervisorServiceClient":
        if role not in ("view", "control", "tui", "reporter", "canceller", "output"):
            raise ValueError("role must be view, control, tui, reporter, canceller, or output")
        if (token is None) == (token_path is None):
            raise ValueError("provide exactly one of token or token_path")
        if token_path is not None:
            token = _read_token_file(Path(token_path))
        assert token is not None
        if len(token.encode("utf-8")) < 32:
            raise ValueError("role token must contain at least 32 bytes")
        client = cls(socket_path, token, role)
        reader, writer = await client._open()
        writer.close()
        await writer.wait_closed()
        return client

    async def snapshot(self) -> SupervisorServiceSnapshot:
        if self.role not in ("view", "control", "tui"):
            self._raise_role("read metadata snapshots")
        value = await self._request({"type": "snapshot"})
        return _snapshot(value)

    async def register_run(self, context: dict[str, Any] | None = None) -> str:
        self._require("control", "register runs")
        response = await self._request({"type": "registerRun", "context": context or {}})
        return _required_string_or_record(response, "runId", "run")

    async def register_activity(self, run_id: str, kind: str = "activity") -> str:
        self._require("control", "register activities")
        response = await self._request({"type": "registerActivity", "runId": run_id, "kind": kind})
        return _required_string_or_record(response, "activityId", "activity")

    async def register_attempt(self, activity_id: str) -> RegisteredAttempt:
        self._require("control", "register attempts")
        response = await self._request({"type": "registerAttempt", "activityId": activity_id})
        record = response.get("attempt") if isinstance(response.get("attempt"), dict) else response
        return RegisteredAttempt(
            _required_string_or_record(response, "attemptId", "attempt"),
            _required_int(record, "generation"),
            _required_string(response, "reporterToken"),
            _required_string(response, "cancelToken"),
        )

    async def set_activity_status(
        self, activity_id: str, status: ActivityStatus, reason: str | None = None
    ) -> ServiceActivityRecord:
        self._require("control", "report activity status")
        response = await self._request({
            "type": "setActivityStatus", "activityId": activity_id, "status": status,
            **({"reason": reason} if reason is not None else {}),
        })
        activity = response.get("activity")
        if not isinstance(activity, dict):
            raise SupervisorServiceError("activity status response omitted activity")
        return _activity_record(activity)

    async def set_waiting(
        self, activity_id: str, reason: str | None = None,
        status: Literal["waitingForInput", "waitingForApproval"] = "waitingForInput",
    ) -> ServiceActivityRecord:
        if status not in ("waitingForInput", "waitingForApproval"):
            raise ValueError("status must be waitingForInput or waitingForApproval")
        return await self.set_activity_status(activity_id, status, reason)

    async def set_run_status(self, run_id: str, status: ExecutionStatus) -> ServiceRunRecord:
        self._require("control", "report run status")
        response = await self._request({"type": "setRunStatus", "runId": run_id, "status": status})
        run = response.get("run")
        if not isinstance(run, dict):
            raise SupervisorServiceError("run status response omitted run")
        return _run_record(run)

    async def set_activity_timeout(self, activity_id: str, timeout_ms: int) -> dict[str, Any]:
        self._require("control", "set activity timeout")
        if not isinstance(timeout_ms, int) or not 1 <= timeout_ms <= 86_400_000:
            raise ValueError("timeout_ms must be between 1 and 86400000")
        return await self._request({
            "type": "setActivityTimeout", "activityId": activity_id, "timeoutMs": timeout_ms,
        })

    async def register_flow_run(
        self, projection: dict[str, Any], coordinator_id: str = "supervisor",
        invocations: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._require("control", "register FlowRuns")
        if not isinstance(projection, dict) or not isinstance(projection.get("flowId"), str):
            raise ValueError("a valid FlowRun projection is required")
        response = await self._request({
            "type": "registerFlowRun", "projection": projection,
            "coordinatorId": coordinator_id,
            **({"invocations": invocations} if invocations is not None else {}),
        })
        return _flow_run(response)

    async def claim_flow_run(
        self, flow_id: str, expected_fence: int, coordinator_id: str = "supervisor"
    ) -> dict[str, Any]:
        self._require("control", "claim FlowRuns")
        if not isinstance(expected_fence, int) or expected_fence < 0:
            raise ValueError("expected_fence must be a non-negative integer")
        response = await self._request({
            "type": "claimFlowRun", "flowId": flow_id,
            "coordinatorId": coordinator_id, "expectedFence": expected_fence,
        })
        return _flow_run(response)

    async def advance_flow_stage(
        self, *, flow_id: str, expected_stage: str, next_stage: str,
        expected_flow_revision: int, fence: int,
    ) -> dict[str, Any]:
        self._require("control", "advance FlowRun stages")
        if not flow_id or not expected_stage or not next_stage:
            raise ValueError("flow_id, expected_stage, and next_stage are required")
        if any(not isinstance(value, int) or value < 0 for value in (expected_flow_revision, fence)):
            raise ValueError("expected_flow_revision and fence must be non-negative integers")
        response = await self._request({
            "type": "advanceFlowStage", "flowId": flow_id,
            "expectedStage": expected_stage, "nextStage": next_stage,
            "expectedFlowRevision": expected_flow_revision, "fence": fence,
        })
        return _flow_run(response)

    async def complete_flow_run(
        self, *, flow_id: str, expected_stage: str, expected_flow_revision: int,
        evidence: Any, fence: int,
    ) -> dict[str, Any]:
        self._require("control", "complete FlowRuns")
        if not flow_id or not expected_stage:
            raise ValueError("flow_id and expected_stage are required")
        if any(not isinstance(value, int) or value < 0 for value in (expected_flow_revision, fence)):
            raise ValueError("expected_flow_revision and fence must be non-negative integers")
        response = await self._request({
            "type": "completeFlowRun", "flowId": flow_id,
            "expectedStage": expected_stage, "expectedFlowRevision": expected_flow_revision,
            "evidence": evidence, "fence": fence,
        })
        return _flow_run(response)

    async def update_flow_invocations(
        self, flow_id: str, invocations: dict[str, Any], fence: int
    ) -> int:
        self._require("control", "update FlowRun invocations")
        if not isinstance(invocations, dict) or not isinstance(fence, int) or fence < 0:
            raise ValueError("invocations and a non-negative fence are required")
        response = await self._request({
            "type": "updateFlowInvocations", "flowId": flow_id,
            "invocations": invocations, "fence": fence,
        })
        count = response.get("count")
        if not isinstance(count, int) or count < 0:
            raise SupervisorServiceError("service response omitted invocation count")
        return count

    async def bind_flow_attempt(
        self, *, flow_id: str, task_id: str, attempt_id: str, generation: int,
        source_cursor: int, fence: int,
    ) -> dict[str, Any]:
        self._require("control", "bind FlowRun attempts")
        values = (generation, source_cursor, fence)
        if any(not isinstance(value, int) or value < 0 for value in values):
            raise ValueError("generation, source_cursor, and fence must be non-negative integers")
        response = await self._request({
            "type": "bindFlowAttempt", "flowId": flow_id, "taskId": task_id,
            "attemptId": attempt_id, "generation": generation,
            "sourceCursor": source_cursor, "fence": fence,
        })
        return _flow_run(response)

    async def reconcile_flow_attempt(
        self, *, flow_id: str, task_id: str, outcome: dict[str, Any], fence: int
    ) -> dict[str, Any]:
        self._require("control", "reconcile FlowRun attempts")
        if not isinstance(outcome, dict) or not isinstance(fence, int) or fence < 0:
            raise ValueError("an attempt outcome and non-negative fence are required")
        response = await self._request({
            "type": "reconcileFlowAttempt", "flowId": flow_id,
            "taskId": task_id, "outcome": outcome, "fence": fence,
        })
        return _flow_run(response)

    async def apply_flow_review(
        self, *, flow_id: str, task_id: str, flow_revision: int,
        evidence: Any, fence: int,
    ) -> dict[str, Any]:
        self._require("control", "review FlowRun tasks")
        if any(not isinstance(value, int) or value < 0 for value in (flow_revision, fence)):
            raise ValueError("flow_revision and fence must be non-negative integers")
        response = await self._request({
            "type": "applyFlowReview", "flowId": flow_id, "taskId": task_id,
            "flowRevision": flow_revision, "evidence": evidence, "fence": fence,
        })
        return _flow_run(response)

    async def cancel_flow_task(
        self, *, flow_id: str, task_id: str, attempt_id: str,
        generation: int, fence: int,
    ) -> dict[str, Any]:
        self._require("control", "cancel FlowRun tasks")
        if any(not isinstance(value, int) or value < 0 for value in (generation, fence)):
            raise ValueError("generation and fence must be non-negative integers")
        response = await self._request({
            "type": "cancelFlowTask", "flowId": flow_id, "taskId": task_id,
            "attemptId": attempt_id, "generation": generation, "fence": fence,
        })
        return _flow_run(response)

    async def report_attempt(self, attempt: RegisteredAttempt, status: ExecutionStatus, detail: Any = None) -> int:
        reporter = await type(self).connect(self.socket_path, token=attempt.reporter_token, role="reporter")
        response = await reporter._request({
            "type": "report", "attemptId": attempt.attempt_id,
            "generation": attempt.generation, "status": status, "detail": detail,
        })
        attempt = response.get("attempt") if isinstance(response.get("attempt"), dict) else {}
        return _required_int(response if "revision" in response else attempt, "revision")

    async def poll_attempt(self, attempt: RegisteredAttempt) -> ServiceAttemptRecord:
        reporter = await type(self).connect(self.socket_path, token=attempt.reporter_token, role="reporter")
        response = await reporter._request({
            "type": "pollAttempt", "attemptId": attempt.attempt_id, "generation": attempt.generation,
        })
        record = response.get("attempt")
        if not isinstance(record, dict):
            raise SupervisorServiceError("poll response omitted attempt")
        return _attempt_record(record)

    async def cancel_attempt(self, attempt: RegisteredAttempt) -> CancellationState:
        canceller = await type(self).connect(self.socket_path, token=attempt.cancel_token, role="canceller")
        response = await canceller._request({
            "type": "cancelAttempt", "attemptId": attempt.attempt_id, "generation": attempt.generation,
        })
        attempt = response.get("attempt") if isinstance(response.get("attempt"), dict) else {}
        state = response.get("cancellation", response.get("cancelState", attempt.get("cancellation", attempt.get("cancelState"))))
        if state not in ("none", "requested", "delivered", "completed", "failed"):
            raise SupervisorServiceError("cancel response omitted a valid cancellation state")
        return state

    async def subscribe(
        self,
        *,
        epoch: str | None = None,
        cursor: int | None = None,
        run_id: str | None = None,
        activity_id: str | None = None,
        attempt_id: str | None = None,
    ) -> SupervisorServiceSubscription:
        if self.role not in ("view", "control", "tui"):
            self._raise_role("subscribe to coordinator events")
        reader, writer = await self._open()
        request_id = _request_id()
        request: dict[str, Any] = {"type": "subscribe", "requestId": request_id}
        for key, value in (("epoch", epoch), ("cursor", cursor), ("runId", run_id),
                           ("activityId", activity_id), ("attemptId", attempt_id)):
            if value is not None:
                request[key] = value
        await _write_json(writer, request)
        response = await _read_response(reader, request_id)
        response_type = response.get("type")
        if response_type not in ("subscriptionReady", "subscribed", "resyncRequired"):
            writer.close()
            raise SupervisorServiceError(f"unexpected subscription response {response_type!r}")
        events = response.get("events")
        return SupervisorServiceSubscription(
            SubscriptionStart(
                _required_string(response, "epoch"),
                int(response.get("cursor", 0)),
                response_type == "resyncRequired" or response.get("resyncRequired") is True,
                [_coordinator_event(event) for event in events if isinstance(event, dict)] if isinstance(events, list) else [],
                _snapshot(response["snapshot"]) if isinstance(response.get("snapshot"), dict) else None,
            ),
            reader,
            writer,
        )

    async def start_process(
        self,
        executable: str,
        *,
        args: list[str] | tuple[str, ...] = (),
        cwd: str | os.PathLike[str] | None = None,
        metadata: dict[str, Any] | None = None,
        policy: dict[str, Any],
        attempt_id: str | None = None,
        generation: int | None = None,
    ) -> SupervisorServiceProcess:
        self._require("control", "start processes")
        if (attempt_id is None) != (generation is None):
            raise ValueError("attempt_id and generation must be provided together")
        reader, writer = await self._open()
        request_id = _request_id()
        command: dict[str, Any] = {"executable": executable, "args": list(args)}
        if cwd is not None:
            command["cwd"] = os.fspath(cwd)
        request: dict[str, Any] = {
            "type": "start",
            "requestId": request_id,
            "command": command,
            "metadata": metadata or {},
            "policy": policy,
        }
        if attempt_id is not None:
            request["attemptId"] = attempt_id
            request["generation"] = generation
        await _write_json(writer, request)
        try:
            response = await _read_response(reader, request_id)
            process_id = _required_string(response, "processId")
            output_token = _required_string(response, "outputToken")
            return SupervisorServiceProcess(self, process_id, reader, writer, output_token)
        except Exception:
            writer.close()
            await writer.wait_closed()
            raise

    async def attach_process(self, process_id: str) -> SupervisorServiceProcess:
        if self.role not in ("control", "tui", "output"):
            self._raise_role("attach to process output")
        reader, writer = await self._open()
        request_id = _request_id()
        await _write_json(writer, {"type": "attach", "requestId": request_id, "processId": process_id})
        try:
            response = await _read_response(reader, request_id)
            attached_id = _required_string(response, "processId")
            if attached_id != process_id:
                raise SupervisorServiceError("attached process ID does not match the requested ID")
            return SupervisorServiceProcess(self, attached_id, reader, writer)
        except Exception:
            writer.close()
            await writer.wait_closed()
            raise

    async def wait_process(self, process_id: str) -> dict[str, Any]:
        process = await self.attach_process(process_id)
        try:
            async for event in process.events():
                if event.type == "exited":
                    return event.payload.get("receipt", {})
        finally:
            await process.close()
        raise SupervisorServiceDisconnected("process stream ended without an exit receipt")

    async def stop_process(self, process_id: str, grace_ms: int = 1500) -> dict[str, Any]:
        if self.role not in ("control", "tui"):
            self._raise_role("stop processes")
        return await self._request({"type": "stop", "processId": process_id, "graceMs": grace_ms})

    async def write_stdin(self, process_id: str, data: bytes) -> None:
        self._require("control", "write process stdin")
        response = await self._request({
            "type": "stdin", "processId": process_id, "data": base64.b64encode(data).decode("ascii")
        })
        if response.get("ok") is not True:
            raise SupervisorServiceError("Supervisor service rejected process stdin")

    async def close_stdin(self, process_id: str) -> None:
        self._require("control", "close process stdin")
        response = await self._request({"type": "closeStdin", "processId": process_id})
        if response.get("ok") is not True:
            raise SupervisorServiceError("Supervisor service rejected stdin close")

    async def _request(self, request: dict[str, Any]) -> dict[str, Any]:
        reader, writer = await self._open()
        request_id = _request_id()
        request = {**request, "requestId": request_id}
        try:
            await _write_json(writer, request)
            return await _read_response(reader, request_id)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _open(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if not hasattr(asyncio, "open_unix_connection"):
            raise NotImplementedError("Supervisor service clients currently require Unix sockets")
        reader, writer = await asyncio.open_unix_connection(self.socket_path, limit=MAX_FRAME_BYTES)
        await _write_json(writer, {"type": "auth", "role": self.role, "token": self.token})
        ready = await _read_json_line(reader)
        if ready is None:
            writer.close()
            raise SupervisorServiceDisconnected("service closed during authentication")
        if ready.get("type") == "error":
            writer.close()
            raise _rejected(ready)
        if ready.get("type") != "ready" or ready.get("role") != self.role:
            writer.close()
            raise SupervisorServiceError("service rejected authentication or negotiated an unexpected role")
        return reader, writer

    def _require(self, role: str, action: str) -> None:
        if self.role != role:
            self._raise_role(action)

    def _raise_role(self, action: str) -> None:
        raise PermissionError(f"the {self.role} role cannot {action}; server authorization remains authoritative")


def _read_token_file(path: Path) -> str:
    if os.name != "posix":
        raise NotImplementedError("private Supervisor token-file validation currently requires Unix")
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & 0o077
        or metadata.st_size > 4096
    ):
        raise PermissionError("role token file must be a regular file owned by the current user with mode 0600")
    token = path.read_text(encoding="utf-8").strip()
    if len(token.encode("utf-8")) < 32:
        raise ValueError("role token must contain at least 32 bytes")
    return token


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _snapshot(value: dict[str, Any]) -> SupervisorServiceSnapshot:
    counts = value.get("counts") if isinstance(value.get("counts"), dict) else {}
    return SupervisorServiceSnapshot(
        manager_id=_required_string(value, "managerId"),
        epoch=_required_string(value, "epoch"),
        cursor=_required_int(value, "cursor"),
        counts={str(key): int(item) for key, item in counts.items() if isinstance(item, int)},
        processes=[_process_record(item) for item in _dict_list(value.get("processes"))],
        runs=[_run_record(item) for item in _dict_list(value.get("runs"))],
        activities=[_activity_record(item) for item in _dict_list(value.get("activities"))],
        attempts=[_attempt_record(item) for item in _dict_list(value.get("attempts"))],
        flow_runs=_dict_list(value.get("flowRuns")),
    )


def _flow_run(value: dict[str, Any]) -> dict[str, Any]:
    flow = value.get("flowRun")
    if not isinstance(flow, dict):
        raise SupervisorServiceError("service response omitted FlowRun")
    return flow


def _run_record(value: dict[str, Any]) -> ServiceRunRecord:
    return ServiceRunRecord(
        _required_string(value, "runId"), _required_string(value, "managerId"),
        value.get("status", "unknown"), value.get("context", {}) if isinstance(value.get("context"), dict) else {},
        int(value.get("revision", 0)), int(value.get("updatedAtMs", 0)),
    )


def _activity_record(value: dict[str, Any]) -> ServiceActivityRecord:
    return ServiceActivityRecord(
        _required_string(value, "activityId"), _required_string(value, "runId"),
        str(value.get("kind", "activity")), value.get("status", "unknown"),
        value.get("health", "unknown"), value.get("lastContactAtMs"), value.get("waitingReason"),
        int(value.get("revision", 0)), int(value.get("updatedAtMs", 0)),
    )


def _attempt_record(value: dict[str, Any]) -> ServiceAttemptRecord:
    return ServiceAttemptRecord(
        _required_string(value, "attemptId"), _required_string(value, "activityId"),
        _required_string(value, "runId"), _required_int(value, "generation"),
        value.get("status", "unknown"), value.get("cancellation", "none"),
        [str(item) for item in value.get("processIds", []) if isinstance(item, str)],
        _dict_list(value.get("reports")), int(value.get("revision", 0)), int(value.get("updatedAtMs", 0)),
    )


def _process_record(value: dict[str, Any]) -> ServiceProcessRecord:
    return ServiceProcessRecord(
        _required_string(value, "processId"), value.get("runId"), value.get("attemptId"),
        bool(value.get("alive", False)), value.get("exitStatus"),
        int(value.get("revision", 0)), int(value.get("updatedAtMs", 0)),
    )


def _coordinator_event(value: dict[str, Any]) -> CoordinatorEvent:
    return CoordinatorEvent(
        _required_int(value, "sequence"), _required_int(value, "cursor"),
        _required_string(value, "epoch"), str(value.get("kind", "unknown")),
        value.get("runId"), value.get("activityId"), value.get("attemptId"), value.get("processId"),
        int(value.get("revision", 0)), int(value.get("occurredAtMs", 0)), value.get("detail"),
    )


def _required_int(value: dict[str, Any], key: str) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int):
        raise SupervisorServiceError(f"service response omitted integer {key}")
    return result


def _request_id() -> str:
    return f"python-client-{id(object()):x}-{os.urandom(8).hex()}"


async def _read_json_line(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    try:
        frame = await reader.readuntil(b"\n")
    except asyncio.IncompleteReadError as error:
        if not error.partial:
            return None
        raise SupervisorServiceDisconnected("service closed mid-frame") from error
    except asyncio.LimitOverrunError as error:
        raise SupervisorServiceError("service response exceeds the 16 MiB frame limit") from error
    if len(frame) > MAX_FRAME_BYTES:
        raise SupervisorServiceError("service response exceeds the 16 MiB frame limit")
    value = json.loads(frame)
    if not isinstance(value, dict):
        raise SupervisorServiceError("service response must be a JSON object")
    return value


async def _write_json(writer: asyncio.StreamWriter, value: dict[str, Any]) -> None:
    frame = json.dumps(value, separators=(",", ":")).encode("utf-8") + b"\n"
    if len(frame) > MAX_FRAME_BYTES:
        raise SupervisorServiceError("request exceeds the 16 MiB frame limit")
    writer.write(frame)
    await writer.drain()


async def _read_response(reader: asyncio.StreamReader, request_id: str) -> dict[str, Any]:
    while True:
        value = await _read_json_line(reader)
        if value is None:
            raise SupervisorServiceDisconnected("service closed before responding")
        if value.get("requestId") != request_id:
            continue
        if value.get("type") == "error":
            raise _rejected(value)
        return value


def _required_string(value: dict[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str):
        raise SupervisorServiceError(f"service response omitted {key}")
    return result


def _required_string_or_record(value: dict[str, Any], key: str, record_key: str) -> str:
    nested = value.get(record_key)
    if key not in value and isinstance(nested, dict):
        return _required_string(nested, key)
    return _required_string(value, key)


def _rejected(value: dict[str, Any]) -> SupervisorServiceRejected:
    return SupervisorServiceRejected(str(value.get("code", "unknown")), str(value.get("message", "request failed")))
