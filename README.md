# RooAGI Agent Supervisor

CONTROL EVERY TOOL PROCESS YOUR APPLICATION STARTS

RooAGI Agent Supervisor gives your application control over every tool process
it starts: bound its output, cancel it, clean up its children, and record how
it finished. It is a Rust library for supervising commands, persistent
subprocesses such as MCP servers, PTY sessions, and pipelines across Linux,
macOS, and Windows.

Tool calls do not always end when their caller times out. A command can keep
running, leave child processes behind, or flood an application's output
buffers. Agent Supervisor brings deadlines, output limits, cancellation,
process-tree cleanup, and structured termination results into one library, so
you do not have to rebuild those behaviors around each subprocess API.

Your application decides what may run and supplies an authorized request.
Agent Supervisor owns native process execution and lifecycle management; your
application remains responsible for identity, authentication, authorization,
and orchestration. Enforcement depends on the operating system and selected
policy; inspect the execution result and platform capabilities.

The current release is **0.1.2**.

## Who it is for

Agent Supervisor is especially useful for:

- **Rust desktop and CLI applications** that embed process control and need
  dependable cancellation, bounded output, and child cleanup;
- **Agent framework developers** who want consistent execution and cleanup
  behavior across tool adapters; and
- **Developers managing MCP servers and other persistent subprocesses** who
  need startup, readiness, shutdown, process groups, and lifecycle events.

Instead of writing bespoke wrappers around operating-system subprocess APIs,
you can use one library for short-lived tool commands, long-running processes,
and multi-process pipelines.

## The problem it solves

Applications launch processes that may outlive a single tool call, create
children, consume unbounded output, or survive cancellation. Each tool adapter
can end up reimplementing deadlines, output collection, process-tree cleanup,
and platform-specific behavior.

RooAGI Agent Supervisor centralizes those responsibilities. It makes execution
policy explicit, supervises process ownership and lifecycle, and returns
structured results and errors that your application can record or act on.

## What reliable execution means

For each execution, the library provides a consistent control surface:

- start only the executable and environment the runtime authorized;
- limit input, output, time, memory, process count, and CPU where supported;
- preserve host networking explicitly when tools need normal connectivity;
- expose liveness, identity, lifecycle, and resource information; and
- finish with a structured termination result, including after cancellation
  or failure.

## Process control in one library

- Run tools and MCP servers with bounded input, output, deadlines, and
  cancellation.
- Clean up complete process trees through shared process groups and native
  containment backends.
- Apply explicit environment, filesystem, network, and resource policies.
- Observe members, identity, liveness, resource statistics, and lifecycle
  events.
- Connect pipelines and report failures with stage-level context.
- Start PTY sessions with the same supervisor-owned lifecycle.
- Use one provider-neutral Rust API from any agent runtime.

## Architecture

```text
agent runtime → authorized ExecutionRequest → agent-supervisor → child process
```

The runtime owns:

- user, project, graph, session, and turn identity;
- authentication and provider-token refresh;
- authorization and path-grant selection;
- MCP configuration and tool metadata;
- hooks, audit records, prompt context, retry, and compaction; and
- agent-level orchestration.

The sandbox owns:

- execution-request validation;
- native process creation and containment;
- environment and filesystem policy enforcement;
- deadlines, output limits, cancellation, and cleanup;
- process-group ownership, introspection, and identity records; and
- structured `SandboxError` values and lifecycle events.

The sandbox does not resolve credentials or make agent authorization decisions.
An MCP client can run through it, but provider authentication remains the
runtime’s responsibility.

## Native capabilities

The supervisor supports:

- Linux cgroup v2 resource limits and Landlock filesystem isolation;
- Windows Job Object containment and AppContainer filesystem isolation;
- macOS filesystem isolation through `/usr/bin/sandbox-exec`;
- host-network passthrough with explicit platform behavior;
- TERM/grace/KILL shutdown;
- process groups, external adoption, and identity checks;
- lifecycle event streams and bounded process snapshots;
- optional resource statistics and sampling;
- connected pipelines with stage-indexed failure receipts;
- TCP, port, and HTTP readiness probes; and
- Unix PTYs and Windows ConPTY.

Inspect `platform_capabilities()` before requiring optional behavior. Select
`EnforcementRequirement::Required` when degraded enforcement is unacceptable.

## OpenShell developer adapter

The OpenShell adapter is a developer preview under `dev/openshell-adapter/`,
outside the published `agent-supervisor` crate. It depends on NVIDIA's Git-only
Rust SDK and can be built from a checkout of this repository. The adapter
bounds collected output and rejects policy requirements that the remote API
cannot apply or verify. Read the [OpenShell adapter contract](docs/openshell.md)
before experimenting with it.

## Quickstart

Add the crate:

```toml
[dependencies]
agent-supervisor = "0.1.2"
```


Create an explicit request and execute it:

```rust
use agent_supervisor::{
    execute, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest,
    NetworkMode, ResourceLimits, SandboxPolicy,
};
use std::path::PathBuf;

let request = ExecutionRequest {
    executable: PathBuf::from("/usr/bin/printf"),
    args: vec!["hello\\n".into()],
    working_directory: None,
    policy: SandboxPolicy {
        environment: EnvironmentPolicy::default(),
        filesystem: None,
        network: NetworkMode::Host,
        limits: ResourceLimits::default(),
        enforcement: EnforcementRequirement::BestEffort,
    },
};

let output = execute(&request, b"").await?;
assert!(output.success);
```

For default-deny filesystem access, set `filesystem` to
`Some(FilesystemPolicy::deny_all())` and add only the required
`FilesystemGrant` entries.

See the [quickstart](docs/quickstart.md), [security model](docs/security-model.md),
and [API boundaries](docs/api-boundaries.md) for the integration contract.

## Python bindings

Version 0.1.2 adds an async Python binding for bounded one-shot execution and
output streaming. The source package is in `python/`; it has not been published
to PyPI yet. Build it from this checkout with maturin:

```bash
python -m pip install maturin
cd python
python -m maturin develop
```

See the [Python API guide](docs/python-api.md) for capture, streaming,
cancellation, and platform behavior.

## Network behavior

`NetworkMode::Host` is the supported compatibility mode in 0.1.0. The child
uses the host resolver, interfaces, routes, firewall rules, and proxy
environment. It does not create a network namespace, proxy, or destination
allow-list.

`NetworkMode::Disabled` denies IP networking while retaining local Unix IPC
where supported. Linux, macOS, and Windows use native enforcement; Windows
requires an explicit filesystem policy because AppContainer provides both
boundaries. See [host network mode](docs/network-host-mode.md) for the
platform-specific contract and test requirements.

## Filesystem and environment policy

Child environments are cleared by default. Variables must be explicitly
inherited or supplied, and dynamic-loader variables such as `LD_PRELOAD` and
`DYLD_*` are rejected.

`filesystem: None` preserves unrestricted filesystem behavior for compatibility.
`Some(FilesystemPolicy::deny_all())` is explicit default-deny access. Filesystem
grants must use absolute paths and explicit access rights.

The sandbox is a process boundary, not a complete container runtime. Its
guarantees depend on the native operating-system backend and the policy passed
by the caller.

## Validation

Run the portable checks locally:

```bash
cargo fmt --all -- --check
cargo test --all-targets -- --test-threads=1
cargo clippy --all-targets -- -D warnings
```

Platform integration tests must execute on their native operating system.
Cross-compilation verifies API bindings but does not replace native Linux,
macOS, or Windows testing.

## Documentation site

The full documentation is published with MkDocs Material and includes the
quickstart, security model, supervisor lifecycle, process groups, platform
behavior, and release notes:

<https://rooagi.github.io/agent-supervisor/>

## Release

See the [0.1.2 release notes](docs/releases/0.1.2.md) for Python bindings and
the earlier [0.1.1 notes](docs/releases/0.1.1.md) for the separate OpenShell
developer adapter and its behavior limits.

## License

RooAGI Agent Supervisor is licensed under the [Apache License, Version 2.0](LICENSE).
You may use, modify, and distribute it, including in open-source and
commercial agent products, subject to the license terms.
