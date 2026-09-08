# Supervisor lifecycle

The supervisor owns the child process and its containment resources for the
life of the execution. It is in-process and runtime-scoped; it is not a daemon
or a cross-runtime process manager.

Shutdown follows a bounded ladder:

```text
normal completion → graceful terminate → grace period → forced kill → receipt
```

Cancellation, timeout, output-limit exhaustion, and explicit termination all
produce a structured process receipt. Process groups let a runtime clean up a
tree of children together, while lifecycle event streams expose start,
exit, failure, and cleanup transitions to hooks or audit consumers.

Long-lived MCP servers can retain their pipes while the supervisor continues
to own process cleanup. Short-lived tool calls can use `execute` for bounded
request/response execution.
