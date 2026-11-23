# Supervisor TUI control protocol

This document defines the control boundary for a separate terminal UI launched
and owned by one `Supervisor`. It is a design contract for the protocol and
transport implementation; it does not add the TUI or IPC server itself.

## Ownership and trust boundary

The application owns a `Supervisor`. That `Supervisor` starts one TUI child
and creates the only control channel for that child. The channel is a pair of
inherited anonymous pipes: the supervisor writes protocol messages to the
TUI's stdin and reads protocol messages from its stdout. The TUI uses the
controlling terminal for keyboard and screen I/O, keeping its terminal UI
separate from the protocol streams. It must not print terminal escape
sequences, logs, or diagnostics to the protocol stdout.

Anonymous pipes avoid a listening TCP port, a discoverable socket path, and a
shared bearer token. The capability to send supervisor requests is the pipe
handle passed to this particular child. Do not put a control secret in command
arguments or environment variables, or fall back to an unauthenticated local
socket. On Unix, the child receives only its standard pipe descriptors; on
Windows, the launcher must restrict inherited handles to the child standard
handles. The TUI must not pass the protocol handles to subprocesses it starts.

The TUI is a trusted operator client, not a security boundary against code
already running as the same OS user. A process with that user's authority may
be able to inspect or interfere with the host application or its child. The
protocol does ensure that a different process cannot discover an endpoint and
send commands merely because it runs on the same machine. The supervisor
remains authoritative: it checks every action and process ID against its own
registry and never accepts an OS PID as a control target.

## Framing and versioning

Each direction is a stream of frames. A frame is a four-byte unsigned
big-endian payload length followed by one UTF-8 JSON object. The length counts
payload bytes, excludes the prefix, and must be between 1 and 1,048,576 bytes.
Reject an oversized, truncated, invalid UTF-8, or invalid JSON frame and close
the channel. Do not attempt to resynchronize after malformed framing.

Every message has `version: 1`. The first TUI-to-supervisor message must be a
`hello` with `min_version` and `max_version`; the supervisor replies with
`hello` containing the selected version and its supervisor instance ID. If
version 1 is not in the requested range, reply with `unsupported_version` and
close. Unknown message types and fields are rejected with `invalid_request`;
unknown fields must not silently widen a command's authority.

Messages after the handshake carry an increasing `request_id` for
request/response correlation. Responses echo that ID. Supervisor events use
their own `event_id` and may arrive between responses. The first operation
after handshake is `subscribe`; it returns an initial snapshot before the
supervisor emits subsequent `process_changed` events. If the event buffer
overflows, emit `resync_required`; the TUI requests a fresh snapshot rather
than assuming it saw every transition.

## Version 1 messages

TUI requests:

```json
{"version":1,"type":"hello","min_version":1,"max_version":1}
{"version":1,"type":"subscribe","request_id":1}
{"version":1,"type":"list_processes","request_id":2}
{"version":1,"type":"stop_process","request_id":3,"process_id":"v1:<uuid>:<sequence>","grace_ms":5000}
{"version":1,"type":"shutdown_all","request_id":4,"grace_ms":5000}
```

Supervisor responses:

```json
{"version":1,"type":"hello","selected_version":1,"supervisor_id":"<uuid>"}
{"version":1,"type":"snapshot","request_id":1,"processes":[{"process_id":"v1:<uuid>:<sequence>","pid":1234,"alive":true,"parent_process_id":null,"start_time_ticks":null,"executable":"/path/to/program","framework":null,"run_label":null}]}
{"version":1,"type":"processes","request_id":2,"processes":[]}
{"version":1,"type":"stop_result","request_id":3,"process_id":"v1:<uuid>:<sequence>","outcome":"force_stop_requested"}
{"version":1,"type":"shutdown_result","request_id":4,"outcomes":[]}
{"version":1,"type":"error","request_id":3,"code":"unknown_process","message":"The process is no longer registered."}
```

The process fields `framework` and `run_label` are optional informational
values. `parent_process_id` and `start_time_ticks` carry OS ancestry observations
when the platform exposes them; the TUI links a child only when the parent PID
is present and the parent's known start time precedes the child's. Missing or
ambiguous identity data leaves the row at the root. These fields are never
control identities. `pid` is display-only; all stop operations use the full
supervisor-scoped `SupervisorProcessId` token. Stop results describe the
supervisor API outcome and must not claim that a process has exited until the
manager observes that state.

The service snapshot includes `coordinatorProcess` with the actual service PID
and executable path observed by the service. The coordinator is the host OS
process root/context, not a managed child row. If the executable path cannot be
read, `identityLimitation` states that `std::env::current_exe()` was unavailable;
the PID remains the service's `std::process::id()`.

FlowRun, task, post, attempt and worker identifiers are context attached to an
observed process or shown in the separate logical-work panel. They are not
process rows or ancestry nodes. Validated worker tool requests are exposed in
`toolRequests` snapshots and `tool_activity_changed` events with request ID,
tool name, attempt/generation, FlowRun/task context and lifecycle state. A tool
request is an activity; it gains an OS child row only when a
`supervisorProcessId` explicitly links it to a process in the observed process
registry. No process is inferred from a request or task label.

Events:

```json
{"version":1,"type":"process_changed","event_id":7,"process":{...}}
{"version":1,"type":"process_removed","event_id":8,"process_id":"v1:<uuid>:<sequence>"}
{"version":1,"type":"tool_activity_changed","event_id":9,"activity":{"requestId":"tool-1","attemptId":"attempt-1","generation":2,"flowId":"flow-1","taskId":"task-3","toolName":"exec","status":"claimed","supervisorProcessId":null,"updatedAtMs":123}}
{"version":1,"type":"resync_required","event_id":10}
```

Error codes are `unsupported_version`, `invalid_request`, `unknown_process`,
`operation_failed`, and `resync_required`. Error messages are for display and
must not be parsed for control flow. A malformed message closes the channel;
a well-formed but unavailable action returns `operation_failed`.

## Authorization rules

The protocol is intentionally command-only; it does not provide a generic
shell, signal-by-PID, or arbitrary filesystem/network operation. The
supervisor accepts only `subscribe`, `list_processes`, `stop_process`, and
`shutdown_all`. For `stop_process`, parse the full ID and look it up in the
owning supervisor before calling the existing per-process shutdown method. A
missing, malformed, foreign-supervisor, or stale ID is rejected and must not
fall back to its numeric sequence or OS PID. `shutdown_all` applies only to
this exact `Supervisor` instance. The UI requires a visible confirmation
before sending either destructive request; the supervisor still validates
and bounds each grace period independently.

Clamp `grace_ms` to a documented maximum of 60,000 ms. Limit concurrent
requests and serialize destructive operations using the existing supervisor
semantics. The supervisor should rate-limit repeated UI requests and cap
retained event/output data so a client cannot grow unbounded memory. Once the
TUI exits or either pipe closes, stop serving requests and reap the UI child;
when the supervisor shuts down, close the pipes and terminate/reap its TUI
child without changing the managed-job shutdown policy.

## Implementation checklist

- Keep the TUI process outside the managed-agent process registry so
  `shutdown_all` cannot accidentally shut down its own control client.
- Spawn with stdin/stdout reserved exclusively for framed protocol messages;
  stderr may be captured for diagnostics but must not be parsed as protocol.
- On Windows, verify that no unrelated inheritable handles are passed to the
  TUI. On Unix, verify that only the intended pipe descriptors survive exec.
- Test protocol version mismatch, invalid and oversized frames, foreign and
  stale process IDs, targeted stop preserving a sibling, shutdown-all scope,
  TUI disconnect, and supervisor shutdown reaping the TUI child.
- Test that terminal output remains usable while protocol stdin/stdout are
  redirected to pipes on each supported platform.
