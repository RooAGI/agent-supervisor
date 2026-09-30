# OpenShell developer adapter

This experimental adapter is maintained in `dev/openshell-adapter/` and is not
part of the published `agent-supervisor` crate. It runs one-shot commands in an
existing, ready OpenShell sandbox. It does not provision or delete sandboxes,
and it does not supervise persistent OpenShell processes.

## Install and connect

The main crate is installed from crates.io. The adapter is available only from
a local checkout of this repository because NVIDIA currently distributes its
Rust SDK from Git rather than crates.io. Add the path to the checkout in your
application's `Cargo.toml`:

```toml
[dependencies]
agent-supervisor = "0.1.2"
agent-supervisor-openshell-dev = { path = "../agent-supervisor/dev/openshell-adapter" }
openshell-sdk = { git = "https://github.com/NVIDIA/OpenShell.git", tag = "v0.1.2" }
```

Configure the SDK for your gateway and authentication method, then select an
existing workspace and sandbox:

```rust,ignore
use agent_supervisor::{execute_with_runner, ExecutionRequest};
use agent_supervisor_openshell_dev::OpenShellRunner;
use openshell_sdk::{ClientConfig, OpenShellClient};

let client = OpenShellClient::connect(ClientConfig::new("https://gateway.example.com"))
    .await?;
let runner = OpenShellRunner::new(client, "default", "my-sandbox")?;
let output = execute_with_runner(&runner, &request, b"").await?;
```

OpenShell SDK and gateway versions should be kept aligned. Configure TLS and
authentication using the SDK rather than embedding gateway credentials in
application command arguments.

## What the adapter supports

- Streams the OpenShell `ExecSandbox` response internally, keeping collected
  stdout and stderr within the request limits.
- Sends stdin as a single bounded payload and applies the request timeout both
  as an OpenShell remote execution timeout and as a local wait deadline.
- Requires an observed exit event. A stream that ends or fails without one
  returns a structured execution error with no invented termination result.
- Reports `canonical_path: None` in `ExecutableIdentity`: OpenShell does not
  return the path resolved inside the sandbox.
- Reports the remote exit code as `TerminationReason::Exited`; OpenShell does
  not report a signal or distinguish a timeout from an ordinary exit.
- Stops reading and drops the response stream as soon as stdout or stderr
  exceeds its collection cap. The size error has no termination result because
  closing the stream does not prove the remote command stopped.

This is one-shot result collection through the existing `ProcessRunner` API;
it does not forward output chunks live to the caller. Output limits bound bytes
retained by this library. On overflow, the client stops reading the stream, but
some output beyond the cap may already have transferred or be buffered in the
transport or client. OpenShell does not provide a command-level kill operation
through this RPC, so the remote command may continue until its remote timeout
even after the local stream closes.

## Requirements the adapter rejects

`OpenShellRunner` rejects requests that rely on behavior `ExecSandbox` cannot
apply or verify:

- any per-command `FilesystemPolicy` (`None` leaves the sandbox's preconfigured
  filesystem policy in force);
- `NetworkMode::Disabled` and `EnforcementRequirement::Required`;
- per-command memory, process-count, and CPU limits;
- host environment inheritance and local executable search paths.

Explicit environment variables are sent as remote overrides. The sandbox's
existing environment is not cleared. `NetworkMode::Host` leaves the managed
sandbox's configured network policy in force; it does not mean that the
remote command shares the application host's network.

## Remote completion limits

The adapter's local wait deadline bounds how long the calling task waits. When
it expires, or an output cap closes the stream, the returned error has no
termination result: the remote process may still be running. OpenShell's
execution timeout is also sent so the gateway can stop the command, but the
protocol represents timeout as exit code `124`, which a command can also return
itself. The protocol has no per-command cancel RPC or confirmed process-tree
termination result. Use a finite timeout and treat a lost stream as an unknown
remote outcome. Do not automatically retry a command whose outcome is unknown.

Each request carries a fresh OpenShell request ID for durable launch admission.
The adapter does not retry or reattach after an ambiguous transport failure;
the ID is not an API for querying the eventual result.

The adapter reports `Enforcement::Degraded` because the execution RPC cannot
prove that the requested per-command enforcement was applied. Inspect the
OpenShell sandbox's own policy and status separately when that boundary matters.
