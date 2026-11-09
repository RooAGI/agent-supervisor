"""Cross-SDK observer used by the Node service end-to-end check."""

from __future__ import annotations

import asyncio
import sys

from agent_supervisor.service_client import (
    RegisteredAttempt,
    SupervisorServiceClient,
    SupervisorServiceRejected,
)


async def main() -> None:
    (
        socket_path,
        view_token_path,
        control_token_path,
        reporter_token,
        output_token,
        process_id,
        foreign_process_id,
        run_id,
        activity_id,
        attempt_id,
        generation_text,
    ) = sys.argv[1:]
    generation = int(generation_text)

    control = await SupervisorServiceClient.connect(
        socket_path, token_path=control_token_path, role="control"
    )
    flow = await control.register_flow_run({
        "flowId": "flow-python-e2e",
        "boardId": "board-python-e2e",
        "flowRevision": 0,
        "tasks": [{
            "taskId": "task-python-e2e", "postId": "post-python-e2e",
            "title": "Python FlowRun task", "dependencies": [], "assignee": None,
            "state": "ready", "waitingReason": None, "attemptId": None,
            "generation": None, "lastSourceCursor": 0,
        }],
    }, coordinator_id="python-e2e")
    assert flow["flowId"] == "flow-python-e2e"
    claimed_flow = await control.claim_flow_run(
        flow["flowId"], expected_fence=0, coordinator_id="python-e2e"
    )
    assert claimed_flow["coordinatorFence"] == 1
    invocation = {
        "runtimeOptions": {"checkpointEndpoint": "http://127.0.0.1/checkpoints"},
        "graph": {"id": "python-e2e"}, "input": {"messages": []},
    }
    assert await control.update_flow_invocations(
        flow["flowId"], {"task-python-e2e": invocation},
        claimed_flow["coordinatorFence"],
    ) == 1
    await control.bind_flow_attempt(
        flow_id=flow["flowId"], task_id="task-python-e2e",
        attempt_id="attempt-python-e2e", generation=1, source_cursor=1, fence=1,
    )
    await control.reconcile_flow_attempt(
        flow_id=flow["flowId"], task_id="task-python-e2e", fence=1,
        outcome={
            "attemptId": "attempt-python-e2e", "generation": 1,
            "sourceCursor": 2, "status": "succeeded",
            "workflowCancelRequested": False,
        },
    )
    reviewed_flow = await control.apply_flow_review(
        flow_id=flow["flowId"], task_id="task-python-e2e", flow_revision=1,
        evidence={"verdict": "approved", "summary": "Python client contract test"},
        fence=1,
    )
    assert reviewed_flow["tasks"]["task-python-e2e"]["state"] == "completed"
    cancel_flow = await control.register_flow_run({
        "flowId": "flow-python-cancel-e2e",
        "boardId": "board-python-cancel-e2e",
        "flowRevision": 0,
        "tasks": [{
            "taskId": "task-python-cancel-e2e", "postId": "post-python-cancel-e2e",
            "title": "Python cancel contract", "dependencies": [], "assignee": None,
            "state": "ready", "waitingReason": None, "attemptId": None,
            "generation": None, "lastSourceCursor": 0,
        }],
    }, coordinator_id="python-cancel-e2e")
    cancel_flow = await control.claim_flow_run(
        cancel_flow["flowId"], expected_fence=0, coordinator_id="python-cancel-e2e"
    )
    await control.bind_flow_attempt(
        flow_id="flow-python-cancel-e2e", task_id="task-python-cancel-e2e",
        attempt_id="attempt-python-cancel-e2e", generation=1, source_cursor=1, fence=1,
    )
    cancel_flow = await control.cancel_flow_task(
        flow_id="flow-python-cancel-e2e", task_id="task-python-cancel-e2e",
        attempt_id="attempt-python-cancel-e2e", generation=1, fence=1,
    )
    assert cancel_flow["tasks"]["task-python-cancel-e2e"]["cancelRequested"] is True
    cancel_flow = await control.reconcile_flow_attempt(
        flow_id="flow-python-cancel-e2e", task_id="task-python-cancel-e2e", fence=1,
        outcome={
            "attemptId": "attempt-python-cancel-e2e", "generation": 1,
            "sourceCursor": 2, "status": "cancelled", "workflowCancelRequested": True,
        },
    )
    assert cancel_flow["tasks"]["task-python-cancel-e2e"]["state"] == "cancelled"

    view = await SupervisorServiceClient.connect(socket_path, token_path=view_token_path, role="view")
    snapshot = await view.snapshot()
    assert {flow["flowId"] for flow in snapshot.flow_runs} == {
        "flow-node-e2e", "flow-python-e2e", "flow-node-cancel-e2e", "flow-python-cancel-e2e"
    }
    python_flow = next(flow for flow in snapshot.flow_runs if flow["flowId"] == "flow-python-e2e")
    assert python_flow["tasks"]["task-python-e2e"]["state"] == "completed"
    assert any(run.run_id == run_id for run in snapshot.runs)
    assert any(activity.activity_id == activity_id for activity in snapshot.activities)
    attempt = next(item for item in snapshot.attempts if item.attempt_id == attempt_id)
    assert attempt.generation == generation
    process = next(item for item in snapshot.processes if item.process_id == process_id)
    assert process.attempt_id == attempt_id and process.run_id == run_id
    subscription = await view.subscribe(
        epoch=snapshot.epoch, cursor=snapshot.cursor, attempt_id=attempt_id
    )
    try:
        await view.attach_process(process_id)
    except PermissionError:
        pass
    else:
        raise AssertionError("metadata-only view unexpectedly attached to process output")

    output = await SupervisorServiceClient.connect(socket_path, token=output_token, role="output")
    try:
        await output.attach_process(foreign_process_id)
    except SupervisorServiceRejected as error:
        assert error.code == "forbidden"
    else:
        raise AssertionError("process output capability attached to a different process")

    stream = await output.attach_process(process_id)
    saw_output = False
    try:
        async for event in stream.events():
            if event.type == "stdout" and event.data and b"interop-output" in event.data:
                saw_output = True
                break
    finally:
        await stream.close()
    assert saw_output, "output capability did not replay the process output"

    reporter = await SupervisorServiceClient.connect(socket_path, token=reporter_token, role="reporter")
    attempt_capability = RegisteredAttempt(attempt_id, generation, reporter_token, "unused")
    observed_attempt = await reporter.poll_attempt(attempt_capability)
    assert observed_attempt.cancellation == "delivered"
    replay_message = await asyncio.wait_for(subscription.next_message(), timeout=3)
    assert replay_message.event is not None
    assert replay_message.event.kind == "attemptCancelDelivered"
    assert replay_message.event.cursor > snapshot.cursor
    await reporter.report_attempt(
        attempt_capability,
        "cancelled",
        {"reasonCode": "cross_client_e2e"},
    )
    await subscription.close()
    print("cross-client snapshot, FlowRun registration/claim, task cancellation reconciliation, output replay, and scoped cancellation passed")


if __name__ == "__main__":
    asyncio.run(main())
