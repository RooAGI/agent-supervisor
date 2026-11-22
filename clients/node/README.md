# RooAGI Agent Supervisor

## Keep the tools behind your agents under control

Multi-agent applications often run many workers, and those workers launch
commands, MCP servers, and other subprocesses. RooAGI Agent Supervisor gives
that work a shared lifecycle: it tracks the processes, bounds their output,
supports cancellation, and cleans up child processes.

Think of it as the process supervisor around your agent system. Your framework
still decides which agents to run, how to assign work, and whether a result is
acceptable. Agent Supervisor manages the local processes those agents use and
records their run and attempt activity. The independent service can keep those
processes alive while a desktop or other client disconnects.

The npm package `@rooagi/agent-supervisor` provides two Node.js integration
modes: an embedded API backed by the Rust core, and an authenticated client for
the independent Supervisor service.

## Service mode: start the independent service

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

## What this package provides

The package entrypoint is `index.mjs`. `Supervisor` embeds the process manager
in the Node application. `SupervisorClient` connects over the authenticated
JSONL protocol and exposes process control, run/activity/attempt reporting,
FlowRun coordination, approvals, and event subscriptions. In service mode, the
service owns the managed processes.

The Rust and Python packages also provide an embedded `Supervisor` API for
applications that manage local process trees in-process. They additionally
provide service clients. The bindings share process and coordination concepts;
their high-level APIs differ by language and by whether the service is remote.

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

## Embedded mode

The embedded Node API owns children in the Node process, using the same Rust
supervision and policy implementation as the Rust and Python APIs. Build the
native addon from a checkout with Rust installed:

```sh
npm run build:native
```

Packages created with `npm pack` or `npm publish` build and include the native
binary through `prepack`. That build targets the machine running the command;
the npm release workflow still needs per-platform native packages before this
API is published for cross-platform installation.

```js
import {
  AgentRunPolicy,
  Command,
  Policy,
  Supervisor
} from '@rooagi/agent-supervisor'

const supervisor = new Supervisor()
const run = supervisor.startAgentRun(new AgentRunPolicy({
  timeoutMs: 180_000,
  shutdownGraceMs: 3_000
}))

try {
  const result = await run.run(async (scope, { signal }) => {
    signal.throwIfAborted()
    return scope.runCommand(
      new Command(['/usr/bin/tool', '--version']),
      { policy: new Policy({ network: 'disabled' }) }
    )
  })
  console.log(result.stdout.toString())
} finally {
  await supervisor.shutdownAll()
}
```

`AgentRun.run()` applies its deadline and optional `AbortSignal`, then cleans up
run-owned processes before it settles. Use `run.start()` for an interactive
child or `Supervisor.startBackground()` for a manager-owned job that should
continue between agent runs. Read output and lifecycle events with
`Supervisor.stream()` or `AgentRun.stream()`. The operation receives an
`AbortSignal` and should pass it into its framework call; JavaScript cannot
forcibly interrupt arbitrary in-process work. One-shot `runCommand()` also
accepts an `AbortSignal` or a `CancellationToken`.

Use `SupervisorClient` when the service should own processes independently of a
desktop, runtime, or framework client. It connects only to the configured Unix
socket and does not fall back to native spawning if the socket is unavailable.
A route that requires service supervision must surface that connection error to
its caller.

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

## Framework adapters

The optional framework entry points connect an agent invocation to the same
Supervisor run, activity, and attempt records. The framework still owns agent
planning, tool results, conversation state, and checkpoints. Importing the
base package does not load any agent SDK; install only the optional SDKs your
application uses:

```sh
npm install @rooagi/agent-supervisor
# Install the SDK peer dependencies used by your application, for example:
npm install @openai/agents langchain @langchain/langgraph ai @google/adk
```

| Python integration | Node adapter | Node entry point |
| --- | --- | --- |
| OpenAI Agents SDK | Supported | `@rooagi/agent-supervisor/frameworks/openai` |
| LangGraph | Supported | `@rooagi/agent-supervisor/frameworks/langgraph` |
| Google ADK | Supported | `@rooagi/agent-supervisor/frameworks/google-adk` |
| LangChain Runnable agents | Supported | `@rooagi/agent-supervisor/frameworks/langchain` |
| PydanticAI | No supported Node SDK adapter | — |
| Microsoft Agent Framework | No supported Node SDK adapter | — |
| FastMCP and MCP stdio helpers | Python-only integrations here; no matching framework adapter in this Node package | — |

Adapters use cooperative cancellation. The caller's `AbortSignal` is passed to
the SDK, and a cancellation requested through the Supervisor service is
forwarded through an invocation signal where that SDK supports it. The
Supervisor records cancellation after the invocation or stream ends; it cannot
forcibly stop arbitrary JavaScript executing in-process. LangGraph thread IDs
and checkpoints, Google ADK sessions, and other framework-owned state remain
with their respective frameworks. Child processes are supervised only when a
tool explicitly starts them with the per-invocation `execute` option's
`startProcess` helper. The helper requires an explicit policy and binds the
child to the current attempt and host-supplied context. Failed or cancelled
invocations stop those children; after success, their handles remain under
caller/Supervisor ownership. Adapters do not intercept global process APIs or
unrelated subprocesses.

### OpenAI Agents SDK

```js
import { createOpenAIAgentsAdapter } from '@rooagi/agent-supervisor/frameworks/openai'

const managed = createOpenAIAgentsAdapter({ client }).wrap(agent)
const result = await managed.run(input, { context, session })
const streamed = await managed.runStream(input, { context, session })
for await (const event of streamed) consume(event)
```

`@openai/agents` is loaded on first use. The adapter waits for streamed events
and the SDK's completion result before reporting the run as finished.

### LangChain.js and LangGraph.js

```js
import { createLangChainAdapter } from '@rooagi/agent-supervisor/frameworks/langchain'
import { createLangGraphAdapter } from '@rooagi/agent-supervisor/frameworks/langgraph'

const managedAgent = createLangChainAdapter({ client }).wrap(agent)
const answer = await managedAgent.invoke(input, { configurable: { thread_id: 'thread-1' } })

const managedGraph = createLangGraphAdapter({ client }).wrap(compiledGraph)
const state = await managedGraph.invoke(input, { configurable: { thread_id: 'thread-1' } })
const events = await managedGraph.stream(input, { configurable: { thread_id: 'thread-1' } })
for await (const event of events) consume(event)
```

Runnable callbacks and configuration pass through. LangGraph thread IDs,
checkpointers, and resume commands remain under LangGraph's ownership.

### Vercel AI SDK and Google ADK

```js
import { createVercelAIAdapter } from '@rooagi/agent-supervisor/frameworks/vercel-ai'
import { createGoogleADKAdapter } from '@rooagi/agent-supervisor/frameworks/google-adk'

const managedAgent = createVercelAIAdapter({ client }).wrap(agent)
const response = await managedAgent.generate({ prompt: 'Summarize this report.' })
const streamed = await managedAgent.stream({ prompt: 'Summarize this report.' })
for await (const part of streamed.textStream) process.stdout.write(part)

const managedRunner = createGoogleADKAdapter({ client }).wrap(runner)
const events = await managedRunner.runAsync(
  { userId, sessionId, newMessage },
  { signal: abortController.signal },
)
for await (const event of events) consume(event)
```

For streaming Vercel results, the adapter preserves the native result object and
keeps the attempt active until the selected stream is consumed. Google ADK
session identity and event payloads pass through unchanged.

For a provider-free example that connects to the service and wraps a fixture
invocation, see [`examples/framework-smoke.mjs`](examples/framework-smoke.mjs).
