# RooAGI Sandbox

RooAGI Sandbox is the process boundary for agent runtimes: a native,
policy-driven supervisor for tools, MCP servers, subprocesses, PTY sessions,
and pipelines.

It accepts an already-authorized execution request and turns it into a bounded,
observable, and cleanly supervised process. The sandbox handles operating
system enforcement and process lifecycle; the agent runtime remains responsible
for identity, authentication, authorization, and orchestration.

The current release is **0.1.0**.

## The problem it solves

Agent runtimes launch processes that may outlive a single tool call, create
child processes, consume unbounded output, inherit unsafe environment values,
or fail to clean up after cancellation. A runtime needs one consistent process
boundary across Linux, macOS, and Windows without turning every tool adapter
into an operating-system integration.

RooAGI Sandbox provides that boundary. It makes execution policy explicit,
keeps process ownership with a supervisor, and returns structured receipts and
errors that an agent runtime can record or act on.

## Why use it?

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
agent runtime → authorized ExecutionRequest → rooagi-sandbox → child process
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

## Quickstart

Add the crate:

```toml
[dependencies]
rooagi-sandbox = "0.1.0"
```

Create an explicit request and execute it:

```rust
use rooagi_sandbox::{
    execute, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest,
    NetworkMode, ResourceLimits,
};
use std::path::PathBuf;

let request = ExecutionRequest {
    executable: PathBuf::from("/usr/bin/printf"),
    args: vec!["hello\\n".into()],
    environment: EnvironmentPolicy::default(),
    working_directory: None,
    filesystem: None,
    network: NetworkMode::Host,
    limits: ResourceLimits::default(),
    enforcement: EnforcementRequirement::BestEffort,
};

let output = execute(&request, b"").await?;
assert!(output.success);
```

For default-deny filesystem access, set `filesystem` to
`Some(FilesystemPolicy::deny_all())` and add only the required
`FilesystemGrant` entries.

See the [quickstart](docs/quickstart.md), [security model](docs/security-model.md),
and [API boundaries](docs/api-boundaries.md) for the integration contract.

## Network behavior

`NetworkMode::Host` is the supported compatibility mode in 0.1.0. The child
uses the host resolver, interfaces, routes, firewall rules, and proxy
environment. It does not create a network namespace, proxy, or destination
allow-list.

`NetworkMode::Disabled` is reserved and fails closed until native network
denial is implemented. See [host network mode](docs/network-host-mode.md) for
the platform-specific contract and test requirements.

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

<https://rooagidev.github.io/rooagi-sandbox/>

## Release

See the [0.1.0 release notes](docs/releases/0.1.0.md) for the initial public
scope, compatibility notes, and platform limitations.

## License

RooAGI Sandbox is licensed under the [Apache License, Version 2.0](LICENSE).
You may use, modify, and distribute it, including in open-source and
commercial agent products, subject to the license terms.
