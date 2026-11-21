export interface FrameworkInvocationContext {
  runId: string
  activityId: string
  attemptId: string
  generation: number
  signal?: AbortSignal
  /** Explicitly start a child under Supervisor ownership; arbitrary tool children are never intercepted. */
  startProcess(options: FrameworkProcessOptions): Promise<FrameworkStartedProcess>
}

export interface FrameworkProcessOptions {
  executable: string
  args?: string[]
  cwd?: string
  /** Required Supervisor process policy. */
  policy: object
}

export interface FrameworkManagedProcess {
  /** Stable Supervisor process identity, distinct from the operating-system PID. */
  readonly processId: string | null
  stop(graceMs?: number): Promise<unknown>
  wait(): Promise<unknown>
}

export interface FrameworkStartedProcess extends FrameworkManagedProcess {
  /** Process IDs are populated before startProcess() resolves. */
  readonly processId: string
}

export interface FrameworkAdapterClient {
  readonly socketPath?: string
  registerRun(context?: Record<string, unknown>): Promise<{ runId: string }>
  registerActivity(options: { runId: string; kind?: string }): Promise<{ activityId: string }>
  registerAttempt(options: { activityId: string }): Promise<{
    attemptId: string
    generation: number
    reporterToken: string
    cancelToken: string
  }>
  setActivityStatus(options: { activityId: string; status: string; reason?: string }): Promise<unknown>
  setRunStatus(options: { runId: string; status: string }): Promise<unknown>
  start(options: FrameworkProcessOptions & {
    attemptId: string
    generation: number
    metadata: Record<string, unknown>
  }): Promise<FrameworkManagedProcess>
}

export interface FrameworkReporter {
  report(options: { attemptId: string; generation: number; status: string; detail?: unknown }): Promise<unknown>
  pollAttempt(options: { attemptId: string; generation: number }): Promise<{ attempt?: { cancellation?: string } }>
  close?(): void
}

export interface FrameworkCanceller {
  cancelAttempt(options: { attemptId: string; generation: number }): Promise<unknown>
  close?(): void
}

export interface FrameworkScopedClient {
  report?: FrameworkReporter['report']
  cancelAttempt?: FrameworkCanceller['cancelAttempt']
  close?(): void
}

export type FrameworkClientConnector = (options: { token: string; role: 'reporter' | 'canceller' }) => Promise<FrameworkScopedClient>

export interface FrameworkAdapterOptions {
  client: FrameworkAdapterClient
  framework: string
  context?: Record<string, unknown>
  /** Opens clients using the attempt-scoped reporter and cancellation tokens. */
  connect?: FrameworkClientConnector
  /** Grace period used to clean up explicitly started processes after failed/cancelled work. */
  cleanupGraceMs?: number
  /** Maximum time to wait for a started child to exit after targeted cleanup. */
  cleanupWaitMs?: number
}

export interface FrameworkInvokeOptions<T> {
  activityKind?: string
  signal?: AbortSignal
  execute(context: FrameworkInvocationContext): T | Promise<T>
  /** Select one or more iterable stream properties from native result objects. */
  streamResult?(result: Awaited<T>): AsyncIterable<unknown> | readonly AsyncIterable<unknown>[] | undefined
  /** Select a promise-like terminal result that can complete without stream consumption. */
  streamCompletion?(result: Awaited<T>): PromiseLike<unknown> | undefined
  /** Receives telemetry failures; these do not replace framework results/errors. */
  onLifecycleError?(error: unknown): void
}

export interface FrameworkAdapter {
  invoke<T>(options: FrameworkInvokeOptions<T>): Promise<Awaited<T>>
}

/** Track one framework invocation. Framework SDKs remain optional and are never imported here. */
export function createFrameworkAdapter(options: FrameworkAdapterOptions): FrameworkAdapter
