# API boundaries

The sandbox is intentionally lower-level than an agent runtime.

## The runtime owns

- user, project, graph, session, and turn identity;
- authentication and provider-token refresh;
- authorization decisions and path-grant selection;
- MCP configuration and tool metadata;
- hooks, audit records, and conversation context; and
- retry, compaction, and agent-level orchestration.

## The sandbox owns

- validating the execution request shape;
- applying environment and filesystem policy;
- creating and supervising native processes, or invoking an explicitly
  selected one-shot WebAssembly runner;
- enforcing deadlines, output limits, cancellation, and cleanup;
- maintaining native process-group containment and identity records; and
- reporting structured `SandboxError` values and lifecycle events.

The boundary keeps credentials and agent policy out of the process supervisor.
An MCP client may run through the sandbox, but the sandbox does not acquire or
forward provider tokens unless the caller explicitly supplies them through the
child environment or another approved transport.

Filesystem isolation, resource limits, network isolation, and process-tree
containment are independent capabilities. A caller must not infer that one
being enforced means all of them are enforced. Use the execution receipt and
platform capability report for the actual backend result.

The optional `wasm` feature runs pure, one-shot modules without host imports.
It does not replace the native process backend or provide process lifecycle,
WASI, filesystem, or network capabilities. See [WebAssembly runner](wasm-backend.md)
for its ABI and policy limits.
