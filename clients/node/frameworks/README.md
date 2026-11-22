# Framework adapter contract

`core.mjs` exports `createFrameworkAdapter`, the framework-independent lifecycle helper used by the optional SDK adapters. The helper wraps one framework invocation with Supervisor run, activity, and attempt records. It does not import an agent SDK.

```js
const adapter = createFrameworkAdapter({
  client: supervisorControlClient,
  framework: 'example-framework',
  context: { projectId: 'project-1', flowRunId: 'flow-1', taskId: 'task-1' },
  connect: ({ token, role }) => SupervisorClient.connect({ socketPath, token, role })
})

const result = await adapter.invoke({
  signal: abortController.signal,
  execute: ({ signal, startProcess }) => nativeFramework.invoke(input, {
    signal,
    tools: { runLocal: (command) => startProcess({ ...command, policy: toolProcessPolicy }) }
  })
})
```

The `context` object is host supplied; adapters must not populate trusted identity from model-generated arguments. Each call registers a run, activity, and attempt, then uses the scoped reporter and canceller tokens returned by the service. Framework return values and thrown errors are passed through unchanged. Async iterable results retain their properties and are proxied so terminal reporting happens when iteration finishes, errors, or is closed early. For SDK result objects whose streams are fields rather than the result itself, set `streamResult` to the iterable field or an array of iterable fields to wrap.

Cancellation is cooperative. The helper combines the supplied `AbortSignal` with Supervisor cancellation observed through the attempt-scoped reporter, sends an attempt-scoped cancellation request when the caller signal fires, and passes the combined signal to the framework adapter callback. A cancelled terminal state is recorded only once the invocation or stream has actually ended. A disconnected control client aborts the combined signal; because execution outcome is then not reportable with confidence, the run, activity, and attempt become `unknown` if reporting remains possible. The Supervisor cannot forcibly stop arbitrary in-process JavaScript execution. A framework tool that starts a child process must explicitly use Supervisor process APIs if that process needs supervised ownership; the adapter does not intercept global process APIs or unrelated children.

`execute` also receives `startProcess`. Call it explicitly for a tool child and provide the required Supervisor `policy`; it returns a handle with a stable Supervisor `processId`, `stop`, and `wait`. The helper binds the process to the current attempt and trusted run/activity/framework context. If the invocation fails, is cancelled, or becomes unknown, the helper requests stop for only the children started through this helper and waits up to the configured cleanup bound. After a successful invocation, started children remain owned by the Supervisor and their returned handles remain the caller's responsibility; the adapter does not assume that a successful tool process should be terminated.

Lifecycle reporting failures after invocation starts are delivered to `onLifecycleError` when supplied. They do not replace framework results or exceptions. Failure to create the run/activity/attempt or establish scoped clients prevents invocation from starting.
