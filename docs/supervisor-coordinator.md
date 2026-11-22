# Supervisor-owned run coordinator

The local service keeps the Supervisor alive as a coordinator while framework
clients connect and disconnect. It owns the process registry, typed lifecycle
records, event ordering, and cancellation bookkeeping for the lifetime of that
service process. A client connection is a control channel; it is not the owner
of the work it starts. `managerId` is the service epoch; it changes when a new
service instance starts. A monotonically increasing `cursor` orders coordinator
events within that epoch.

With `--state-dir`, the service persists coordinator snapshots, typed events,
request receipts, launch reservations, and bounded recovery diagnostics in a
private versioned journal. It restores terminal outcomes and descriptive
references under a fresh service epoch. Active run/activity/attempt states are
reported as unknown or stale; old process IDs are not adopted as live process
ownership. Framework session/checkpoint verification remains framework owned,
so the service does not resume agent work from its journal alone.

## Ownership and records

The service has one internal `managerId` for its lifetime. Its authenticated
control role is the manager principal. A future multi-manager service must
replace that singleton with separately authenticated manager identities and
must scope every operation to one manager. A process ID, Flowboard ID, runtime
run ID, or caller-provided label never grants authority.

| Record | Supervisor identity | Meaning and lifetime |
| --- | --- | --- |
| Run | Opaque `runId`, owning `managerId` | One requested framework execution. `status` is its execution outcome; `context` holds external references; `revision` and `updatedAtMs` order updates. |
| Activity | Opaque `activityId`, parent `runId` | One unit of work in a run, including work without a process. `status`, `health`, `waitingReason`, `lastContactAtMs`, `revision`, and `updatedAtMs` remain independent. |
| Attempt | Opaque `attemptId`, parent activity/run, monotonic `generation` | One concrete worker attempt, including retries. Status reports and cancellation controls require both attempt ID and generation. `cancellation`, `revision`, and `updatedAtMs` remain distinct from execution status. |
| Process | Existing opaque `processId` | One OS process tree started by the Supervisor. `alive`, `exitStatus`, `revision`, and `updatedAtMs` describe observed process lifecycle. It may outlive or serve several activities/attempts. |

`projectId`, `flowRunId`, `taskId`, `postId`, `workerId`, `runtimeRunId`,
`framework`, and `runLabel` are descriptive context only. The coordinator
preserves them for display and reporting, but never uses them to look up an
owner or authorize a command. Framework session/checkpoint references remain
owned by the framework runtime; the Supervisor may retain opaque references
and lifecycle summaries but does not interpret or persist their contents.

An activity or attempt may be reported without a corresponding OS process.
Likewise, a persistent process may be shared across activities or remain
running after an attempt ends. Attempt cancellation and process stop are
separate operations: cancellation targets adapter work and records its
requested, delivered, and completed/failed state; process stop targets exactly
one Supervisor `processId` and waits for its exit/reap receipt.

## Reporting and lifecycle

The adapter reports run/activity/attempt changes through the service. The
service assigns internal IDs, validates parent relationships, sequences
events, and records when cancellation is requested, delivered, completed, or
failed. External IDs and reported workflow status are evidence supplied by a
framework adapter, not proof that the Supervisor observed an OS process exit.
The process receipt remains authoritative for process completion. Provenance is
server-derived; records do not accept a caller-controlled `source` string:

| Change | Provenance source | Evidence retained |
| --- | --- | --- |
| Run/activity/attempt registration | Authenticated manager/control request | Coordinator event, record IDs, revisions, and timestamps |
| Attempt status or health report | Attempt-scoped reporter credential | Bounded report history with status, sanitized detail, revision, and `reportedAtMs` |
| Attempt cancellation | Authenticated manager or attempt-scoped canceller credential | Cancellation state transitions and coordinator events |
| Process start/exit | Supervisor process launcher and reaper | Process record and process stream receipt; framework status cannot synthesize this evidence |

External status strings never establish a process exit. All record timestamps
are Unix epoch milliseconds; record revisions increase on updates. A
coordinator event includes its global cursor/sequence, event revision,
`occurredAtMs`, typed record IDs, and structured detail. Terminal execution
outcomes are `succeeded`, `failed`, `cancelled`, `blocked`, or `unknown`;
activities also use `timedOut`. Process termination remains the process
receipt's exit/signal outcome. Cancellation moves through `none`, `requested`,
`delivered`, then `completed` or `failed`.

### Current JSONL coordinator requests

Requests are one JSON object per newline, with a `requestId` on requests that
expect a reply. IDs are generated by the service. Current registration calls
are:

```json
{"type":"registerRun","requestId":"1","context":{"flowRunId":"flow-123","projectId":"project-7"}}
{"type":"registerActivity","requestId":"2","runId":"<runId>","kind":"worker"}
{"type":"registerAttempt","requestId":"3","activityId":"<activityId>"}
```

The replies carry typed `run`, `activity`, or `attempt` records. Attempt
registration also returns a `generation` in the attempt record and independent
random `reporterToken` and `cancelToken` values. A
client authenticates with `{"type":"auth","role":"reporter",
"token":"<reporterToken>"}` or the corresponding `canceller` role. These
capabilities are in-memory and scoped to the issued `attemptId`.

The reporter can send `{"type":"report","attemptId":"<attemptId>",
"generation":1,"status":"running","detail":{}}` and
`{"type":"pollAttempt","attemptId":"<attemptId>","generation":1}`. The
manager or scoped canceller sends `{"type":"cancelAttempt",
"attemptId":"<attemptId>","generation":1}`;
the `canceller` capability may send that request for its own attempt only.
Cancellation progresses from `none` to `requested`, then to `delivered` when
the reporter polls. The adapter invokes its framework cancellation method and
reports the terminal result. Accepted report states are `queued`, `running`,
`succeeded`, `failed`, `cancelled`, `blocked`, and `unknown`. A report revision
increases on each update. `detail` is restricted to a short `reasonCode` and a
boolean `retryable`; prompts, commands, environment values, credentials, and
arbitrary worker output are rejected. Terminal reports retire both scoped
capabilities.

The control role can update generic run and activity state with
`setRunStatus`, `setActivityStatus`, and `setActivityTimeout`. Activity status
accepts `waitingForInput` and `waitingForApproval`; its optional `reason` is
bounded to 2 KiB and is exposed as `waitingReason`. Timeout requests take a
`timeoutMs` from 1 through 86,400,000 and return immediately; expiration marks
the activity `timedOut` with stale health and publishes an `activityTimedOut`
event. A timeout captures the current attempt generation and is discarded if a
new generation starts first. Timeouts are actor-owned and epoch-local, so
callers should re-arm them after service restart. All three mutations require a request ID and use the
coordinator's idempotency receipts.

The manager can associate a launched process with an attempt using
`start(..., attemptId, generation)`. Both identifiers are validated by the
coordinator. The `processId` appears in the attempt snapshot. This
does not make the process ID an attempt credential: a process stop still uses
the process-scoped stop action and waits for the process receipt.

For `start`, the service first durably reserves the scoped request ID with a
SHA-256 fingerprint of the launch request, then dispatches once, durably
associates the resulting process, and records the process ID as the completion
receipt before replying. A retry with the same ID and fingerprint returns the
recorded process result while the service still owns that process. A changed
fingerprint is rejected. If the service restarts after reserving but before
recording completion, the reservation is exposed as unresolved/unknown and a
retry is refused; the service never redispatches work whose side effect may
have happened. This provides at-most-once dispatch with an explicit uncertain
window, not exactly-once execution. New intentional launches need new request
IDs.

For a Supervisor-owned FlowRun task, the manager may send
`{"type":"cancelFlowTask","flowId":"flow-123","taskId":"task-7","attemptId":"<attemptId>","generation":2,"fence":1}`.
The current attempt identity and coordinator fence are both checked. The
service records cancellation intent and asks its worker adapter to send a
cooperative cancel message. The task remains active until the worker reports a
terminal result; an acknowledged cancellation becomes `cancelled`, while an
unrequested process stop remains blocked pending workflow reconciliation.
Repeated requests for the same active attempt are idempotent.

`shutdown({graceMs})` is a control-only request. It stops accepting connections,
requests cleanup through `shutdown_all()`, awaits the results, closes active
service sessions, and removes the socket path.

The `stop` action always targets one opaque Supervisor `processId`; it accepts
`graceMs` (default 1500, maximum 30000). On Unix the Supervisor requests a
graceful stop and force-terminates the process tree if it remains alive after
that interval. On Windows the current process backend force-terminates
immediately because it has no graceful signal. The response reports the stop
request result; clients use the matching `exited` receipt to confirm reaping.
Unknown action names are denied by the role matrix, including for `control`.

`snapshot` returns `managerId`, `epoch`, `cursor`, the four typed record sets,
and record counts. `subscribe` subscribes before replay. A matching epoch and
available cursor returns bounded typed replay, then later `coordinatorEvent`
frames. An epoch mismatch, pruned cursor, future cursor, or broadcast lag
returns `resyncRequired` with a current snapshot. Clients must take that
snapshot before acting on state. Output process streams remain separately
attached by `processId`; they do not replace coordinator events. Reports retain
their newest 64 entries per attempt, and event/output/client queues are bounded.

`status` returns the service epoch, event cursor, PID, uptime, and active/total
record counts. The binary publishes `--ready-file` only after its private Unix
socket is bound. `--state-dir` takes an exclusive service-instance lock so only
one service owns that configured directory. The lock does not itself claim
restart recovery; see the store/restart contract when enabled.

### Supervisor-owned Flowboard outbox

When FlowRun coordination is configured, the service owns the MCP stdio
connection and the retry worker. Configure it on service startup with
`--flowboard-mcp-command PATH` and one or more `--flowboard-mcp-arg ARG`
options. Add `--flowboard-mcp-env NAME` to pass a selected variable from the
service environment to the MCP child without placing its value in process
arguments. The MCP process also inherits the service environment, so configure
Flowboard credentials through the user service manager. Outbox delivery requires `--state-dir`; event append
uses stable event and request IDs so a lost reply can be retried safely. The
worker persists acknowledgement only after Flowboard confirms the new revision.
On a revision conflict it reads the current Flow and rebases only after checking
whether that stable event ID already exists. The service stops and reaps its
MCP child when shutting down. Desktop clients do not own this connection and
may disconnect while the outbox continues retrying.

The service can own task dispatch through its Roo Runtime JSONL worker host.
Configure its absolute executable and arguments with
`--workflow-worker-command PATH` and repeated `--workflow-worker-arg ARG`
options; `--workflow-worker-cwd DIR` sets an absolute working directory, and
`--workflow-worker-env NAME` releases only named values from the service
environment. The adapter starts each worker as a Supervisor-managed process,
passes its invocation over stdin, and reconciles only validated attempt ID,
generation, cursor, and status fields from worker stdout.

Worker `succeeded` results move tasks to `readyForReview`; they never complete
tasks by themselves. `applyFlowReview` requires the task to be in that state
and a newer Flowboard revision, then records the supplied review evidence
before completing the task and releasing dependent work. Review requests for
active, blocked, cancelled, or already completed tasks are rejected.

Control clients provide each task's transient `runtimeOptions`, `graph`, and
`input` in the `invocations` map to `registerFlowRun`, or refresh them with
`updateFlowInvocations` after reconnecting. These values stay in service memory
and are sent only through the private process pipe; they are excluded from the
coordinator journal, snapshots, and events. The configured Roo Runtime worker
must use a durable checkpoint endpoint and its approved provider configuration.
Without a configured worker or a task invocation, ready work stays queued and
`status`/`snapshot` explain why dispatch is unavailable.

Example Unix lifecycle:

```sh
agent-supervisor-service --socket "$XDG_RUNTIME_DIR/agent-supervisor.sock" \
  --state-dir "$XDG_STATE_HOME/agent-supervisor" \
  --ready-file "$XDG_RUNTIME_DIR/agent-supervisor.ready" \
  --flowboard-mcp-command /usr/local/bin/flowboard-mcp \
  --flowboard-mcp-arg stdio \
  --view-token-file "$XDG_CONFIG_HOME/agent-supervisor/view.token" \
  --control-token-file "$XDG_CONFIG_HOME/agent-supervisor/control.token" \
  --tui-token-file "$XDG_CONFIG_HOME/agent-supervisor/tui.token"

agent-supervisor-service --status --socket "$XDG_RUNTIME_DIR/agent-supervisor.sock" \
  --view-token-file "$XDG_CONFIG_HOME/agent-supervisor/view.token"

agent-supervisor-service --shutdown --socket "$XDG_RUNTIME_DIR/agent-supervisor.sock" \
  --control-token-file "$XDG_CONFIG_HOME/agent-supervisor/control.token" --grace-ms 5000
```

`--state-dir` defaults to the socket's parent when omitted. Closing a desktop,
TUI, or observer connection does not stop work. Explicit owner shutdown stops
accepting new clients, awaits the Supervisor's process cleanup, closes active
sessions, removes the socket, and removes the readiness file created by that
instance. The service binary currently supports Unix sockets on Unix systems.
It can be run under a user service manager such as systemd or launchd with
restart-on-failure enabled; that restarts the service process, but only the
separate versioned store/reconciliation path may restore safe coordinator
state. No reboot survival is implied by process restart alone.

## Authorization scopes

The service authenticates a role on each connection and enforces permissions
on the server. Clients hiding a command in their UI is not authorization.

| Scope | Intended authority |
| --- | --- |
| Metadata view | Read run/activity/attempt/process metadata and lifecycle snapshots. |
| Output read | Read output for processes covered by the issued capability. |
| Launch | Create runs/activities/attempts and start processes for the manager. |
| Report | Append lifecycle updates to one issued attempt. |
| Attempt cancel | Request cancellation for one issued attempt; cannot stop arbitrary processes. |
| Process stop | Stop one allowed `processId`; cannot cancel unrelated attempts or shut down the service. |
| Service shutdown | Ask the manager to stop accepting work, then stop and reap owned processes. |

The existing role files remain distinct: `view` reads snapshots, status, and
coordinator subscriptions. Each process start returns an `outputToken`; its
`output` role can attach only to that one process. Output tokens expire one
hour after issue; at most 4096 are retained, with the oldest evicted when the
cap is reached. Buffered process history remains available through a valid
output token during that lifetime. `control` is the manager
credential, and `tui` reads snapshots/status/subscriptions and attached streams
and can stop one named process. A manager can create an
attempt and receives separate random `reporterToken` and `cancelToken`
capabilities. The reporter token authenticates only `report` and `pollAttempt`
for that attempt; the cancel token authenticates only `cancelAttempt` for that
attempt. A reporter can observe a pending cancellation through `pollAttempt`.
The manager can start processes, cancel its attempts, stop a named process,
and request service shutdown. TUI stop attribution comes from the authenticated
TUI role, not a caller-supplied actor string. Only the manager control scope can
revoke view/TUI roles or request service shutdown. Reporter credentials are
retired on terminal report. Credentials are never included in events,
snapshots, or logs.

## Boundaries

The Supervisor coordinator does not execute model calls, own framework
checkpoints, or replace a framework's scheduling and retry policy. A framework
adapter still runs that code and reports its lifecycle. The service can keep a
separate adapter process and its process tree supervised after the initiating
client disconnects, but the service cannot keep in-process framework code alive
after that code's host exits. Workflow decisions such as retry, task
dependency, and business success remain with Flowboard or the framework; the
Supervisor provides bounded execution, authoritative process lifecycle, and
the coordinator protocol. Restart recovery is a separate store/reconciliation
feature and must report unrecoverable or uncertain work as such rather than
claiming it is still running.

## How this extends tasks 17–23

Tasks 17–23 establish reusable process metadata and the Unix service/client
bridge. They remain useful for labels, provider process launch, stdio, replay,
TUI view/stop roles, and cancellation evidence after a matching process is
reaped. They do not define the framework-neutral work model: `projectId`,
`flowRunId`, `taskId`, `postId`, `workerId`, `runtimeRunId`, and `processId`
remain separate identities. Context from task 17 and trusted host mappings from
task 19 are descriptive only. The service ownership and reconnect path from
task 18 keeps process lifetime independent of desktop observers. Task 20's
provider verification and task 21's Codex/Claude launch adapters supply or
consume process metadata; they do not own the cross-framework coordinator.
Task 22's view/control/TUI permissions are the base roles, now extended with
per-attempt reporting and cancellation capabilities. Task 23's confirmed
TUI-stop callback remains process-stop evidence; a worker-attempt cancellation
is a separate generation-fenced transition.
