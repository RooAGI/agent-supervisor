# RooAGI Sandbox

## Reliable process control for AI agents

The process boundary your agent runtime can trust.

`rooagi-sandbox` gives agent builders a native, policy-driven boundary for
running tools, MCP servers, subprocesses, and pipelines.

It turns an already-authorized execution request into a supervised process:

```text
agent runtime → ExecutionRequest → rooagi-sandbox → child process
```

The sandbox owns process containment and cleanup. The runtime remains
responsible for authorization, credentials, graph identity, and tool policy.

## Built for every agent builder

RooAGI Sandbox is for teams building:

- agent frameworks that need one process boundary across operating systems;
- MCP servers and tool adapters that must be bounded and cleaned up reliably;
- enterprise agent platforms that need lifecycle events, resource controls,
  and inspectable process identity;
- security-sensitive runtimes that require explicit filesystem and environment
  policy; and
- desktop and CLI agents that need cancellation, PTY sessions, and process-tree
  cleanup.

Use the same execution contract whether an agent runs one short-lived tool,
keeps an MCP server alive, or coordinates a multi-process pipeline.

## What reliable execution means

An agent should not have to choose between speed and control. The sandbox
combines native operating-system enforcement with predictable process
semantics:

1. Start only the executable and environment the runtime authorized.
2. Limit input, output, time, memory, process count, and CPU where supported.
3. Preserve host networking explicitly when tools need normal connectivity.
4. Expose liveness, identity, lifecycle, and resource information.
5. Finish with bounded shutdown and a structured result after cancellation or
   failure.

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
