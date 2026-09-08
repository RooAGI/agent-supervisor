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
- creating and supervising native processes;
- enforcing deadlines, output limits, cancellation, and cleanup;
- maintaining process-group containment and identity records; and
- reporting structured `SandboxError` values and lifecycle events.

The boundary keeps credentials and agent policy out of the process supervisor.
An MCP client may run through the sandbox, but the sandbox does not acquire or
forward provider tokens unless the caller explicitly supplies them through the
child environment or another approved transport.
