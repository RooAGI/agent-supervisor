# RooAGI Sandbox

## The process boundary for agent runtimes

`rooagi-sandbox` gives agent builders a native, policy-driven boundary for
running tools, MCP servers, subprocesses, and pipelines.

It turns an already-authorized execution request into a supervised process:

```text
agent runtime → ExecutionRequest → rooagi-sandbox → child process
```

The sandbox owns process containment and cleanup. The runtime remains
responsible for authorization, credentials, graph identity, and tool policy.

## Why use it?

- bounded stdout, stderr, stdin, deadlines, and cancellation;
- graceful shutdown followed by forced cleanup;
- process groups, adoption, introspection, lifecycle events, and statistics;
- Linux cgroup and Landlock support;
- Windows Job Object and AppContainer support;
- macOS filesystem isolation through the native Seatbelt launcher;
- explicit host-network behavior and fail-closed unsupported modes; and
- one provider-neutral Rust API for runtimes and MCP transports.

## Start here

1. Follow the [quickstart](quickstart.md).
2. Read the [security model](security-model.md).
3. Review the [API boundaries](api-boundaries.md) before integrating it into
   an agent runtime.
4. See the [0.1.0 release notes](releases/0.1.0.md) for the supported scope.

## Release status

The current release is **0.1.0**. The crate is Apache-2.0 licensed and is
intended to be embedded in open-source and commercial agent products.
