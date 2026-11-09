# Node client for Agent Supervisor

Build the service with `cargo build --release --bin agent-supervisor-service`.
Ask the Supervisor to issue the three role credentials into a new private
directory. It prints only the resulting file paths, never token values:

```sh
agent-supervisor-service --init-credentials-dir "$XDG_CONFIG_HOME/agent-supervisor"
```

The initialization command fails if the directory already exists. It creates
the directory with mode `0700` and distinct raw 256-bit tokens in `view.token`,
`control.token`, and `tui.token`, each with mode `0600`. Pass all three file
paths to the service; a client receives only its role's file path.

```sh
agent-supervisor-service --socket "$XDG_RUNTIME_DIR/agent-supervisor.sock" \
  --view-token-file "$XDG_CONFIG_HOME/agent-supervisor/view.token" \
  --control-token-file "$XDG_CONFIG_HOME/agent-supervisor/control.token" \
  --tui-token-file "$XDG_CONFIG_HOME/agent-supervisor/tui.token"
```

Each token file must be a regular file owned by the current user with mode
`0600`. The service refuses a live socket and only removes an owned stale Unix
socket. It sets the socket mode to `0600`.

The published Node package name is `@rooagi/agent-supervisor-node`; the source
entrypoint is `clients/node/index.mjs`. RooAGI Desktop can consume the local
package with a `file:` dependency while this shared client is developed.

Connect with an explicit role. A `view` client can request metadata snapshots
and coordinator subscriptions, but cannot attach to process output. A successful
start also returns an `outputToken` scoped to that process. Connect with
`role: 'output'` and that in-memory token to attach or wait for that process;
this role cannot snapshot, launch, write stdin, stop, cancel, or shut down. A
`tui` client can inspect metadata, attach to output, and stop targeted processes,
but cannot launch processes or write stdin. The TUI token authenticates the stop
actor; the request's `actor` field is ignored. A `control` client can start,
provide stdin, close stdin, and stop a process. Process controls use the full
Supervisor process ID, never an OS PID. Output capabilities remain valid for
that process while the service is running and can replay its retained history;
the service clears them on restart. JSONL frames are limited to 1 MiB.

## Embedded and service modes

Use the Rust or Python embedded `Supervisor` API when the host owns and runs
managed children in its own process. Use `SupervisorServiceClient` when the
service process must own them independently of a desktop, runtime, or framework
client. The service client connects only to the configured Unix socket; it does
not fall back to native spawning if the socket is unavailable. A route that
requires supervision must surface that connection error to its caller.

The current JSONL auth handshake has no protocol-version negotiation. Clients
and service must use compatible releases; incompatible replies and unavailable
services surface as client errors. Do not treat reconnect as a request to start
a replacement job: attach with the stable process ID and its output capability.

```js
import { SupervisorClient } from './index.mjs'

const client = await SupervisorClient.connect({ socketPath, tokenPath, role: 'control' })
const process = await client.start({ executable: '/usr/bin/tool', args: ['--version'], policy })
process.stdout.on('data', (chunk) => frameworkProtocolReader.write(chunk))
const receipt = await process.wait()
```

`createManagedProcessLauncher()` adapts the remote launch protocol to the
desktop provider launcher shape. It returns an immediate process-like handle
with stdin/stdout/stderr, then binds the Supervisor ID after the server accepts
the launch. Its policy callback must explicitly release provider environment
values through `policy.environment.variables`; the adapter checks that the
Codex `CODEX_HOME`/`ACCESS_TOKEN` and Claude authentication variables that are
present in `SpawnOptions.env` match the policy. It never transmits
`SpawnOptions.env` as an unrestricted environment.

The manager client can register run, activity, and attempt records. Registering
an attempt returns an opaque attempt ID and separate one-attempt reporting and
cancellation capabilities. Give the reporter capability only to the adapter that reports this attempt; connect it
in memory with
`SupervisorClient.connect({ socketPath, role: 'reporter', token })`. It can call
`report({ attemptId, generation, status, detail })` and
`pollAttempt({ attemptId, generation })` only. The manager's
`cancelAttempt({ attemptId, generation })` sets cancellation to `requested`;
the reporter's next poll changes it to `delivered`. A cancel-scoped client can instead connect
with `SupervisorClient.connect({ socketPath, role: 'canceller', token: cancelToken })`
and call `cancelAttempt({ attemptId, generation })`; this credential cannot
report, launch, or stop processes. The adapter must then invoke its framework
cancellation API and report the outcome. Terminal reports retire the
capability. `start({ ..., attemptId, generation })` associates a supervised
process with the attempt and returns an `outputToken`. To reconnect with the
process-scoped capability, use `SupervisorClient.connect({ socketPath, role:
'output', token: handle.outputToken })`, then `attach(processId)`.

FlowRun control uses the same authenticated control role. `registerFlowRun`
accepts the Flowboard projection and coordinator identity; `claimFlowRun`
checks the expected fencing token; `bindFlowAttempt` and
`reconcileFlowAttempt` report a worker attempt using its generation and source
cursor; `applyFlowReview` records approved evidence against the current
Flowboard revision; `cancelFlowTask` records a generation-fenced cancellation
request and returns the task projection with `cancelRequested` set. The worker
adapter is asked to stop cooperatively; cancellation is terminal only after
the worker reports its result. Each method returns the canonical Supervisor FlowRun
projection. `snapshot()` includes `flowRuns` so clients can recover the current
projection after reconnecting.

`snapshot()` returns run, activity, attempt, process, and FlowRun records with a
service-lifetime `epoch` and monotonic `cursor`. `subscribe({ epoch, cursor,
runId, activityId, attemptId })` returns an initial replay or resync snapshot,
then emits `event` messages. If the service emits `resyncRequired`, apply its
snapshot and subscribe again from that epoch and cursor. A new epoch means the
service restarted; event replay is in-memory and does not survive that restart.

`client.shutdown(graceMs)` is a control-only service shutdown request. The
service stops accepting connections after it requests cleanup and awaits the
registered process results. Run and attempt records live for the service
lifetime; they do not relocate framework code out of Roo Runtime/Desktop or
restore framework sessions after a service restart. The framework remains
responsible for its session/checkpoint and native cancellation API.

The `stop_requested` event reports intent. A TUI cancellation callback is
emitted as `supervisorStop` only after the same process's `exited` event
arrives, after the supervisor has waited for and reaped it. A dropped client
does not stop the process; another authenticated client can attach by its
stable process ID and receives the bounded recent event history.

Authenticated sockets expire after eight hours by default (the service caps
configured lifetimes at 24 hours). Expiry closes the socket; clients must
authenticate again. A control client can call `client.revokeRole('view')` or
`client.revokeRole('tui')`. Revocation closes existing sessions for that role
and rejects future authentication for the rest of the service lifetime. The
control role cannot revoke itself or another control credential. Restarting
the service clears in-memory revocations; issue replacement role files when
rotating credentials. Token values are never logged or returned by the
revocation API.
