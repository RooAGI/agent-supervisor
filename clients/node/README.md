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

Connect with an explicit role. A `view` client can request `snapshot` and attach
to process output/exit streams. A `tui` client can inspect/attach and stop, but
cannot start processes or write stdin. The TUI token authenticates the stop
actor; the request's `actor` field is ignored. A TUI client points `tokenPath`
at only `tui.token`; it never opens either other role file. A `control` client can start,
provide stdin, close stdin, and stop a process. Process controls use the full
Supervisor process ID, never an OS PID. JSONL frames are limited to 1 MiB.

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
