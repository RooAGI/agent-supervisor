import { EventEmitter } from 'node:events'
import { PassThrough, Writable } from 'node:stream'
import { Buffer } from 'node:buffer'

export type SupervisorRole = 'view' | 'control' | 'tui' | 'reporter' | 'canceller' | 'output'
export type ExecutionStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'blocked' | 'unknown'
export type ActivityStatus = 'queued' | 'running' | 'waitingForInput' | 'waitingForApproval' | 'succeeded' | 'failed' | 'cancelled' | 'timedOut' | 'blocked' | 'unknown'
export type Health = 'unknown' | 'healthy' | 'stale'
export type CancellationState = 'none' | 'requested' | 'delivered' | 'completed' | 'failed'
export type ApprovalRequestState = 'pending' | 'approved' | 'denied' | 'timedOut' | 'cancelled' | 'interrupted'
export type WorkerToolRequestState = 'pending' | 'claimed' | 'completed' | 'failed' | 'timedOut' | 'cancelled' | 'interrupted' | 'unknown'
export interface SupervisorMetadata {
  framework?: string
  runLabel?: string
  projectId?: string
  flowRunId?: string
  taskId?: string
  postId?: string
  workerId?: string
  runtimeRunId?: string
}
export interface SupervisorLaunch {
  executable: string
  args?: string[]
  cwd?: string
  metadata?: SupervisorMetadata
  policy: object
  attemptId?: string
  generation?: number
}
export interface ServiceCounts {
  runs: number; activeRuns: number; activities: number; activeActivities: number
  attempts: number; activeAttempts: number; processes: number; activeProcesses: number
}
export interface ServiceRunRecord {
  runId: string; managerId: string; status: ExecutionStatus; context: Record<string, unknown>
  revision: number; updatedAtMs: number
}
export interface ServiceActivityRecord {
  activityId: string; runId: string; kind: string; status: ActivityStatus; health: Health
  lastContactAtMs: number | null; waitingReason: string | null; revision: number; updatedAtMs: number
}
export interface ServiceAttemptRecord {
  attemptId: string; activityId: string; runId: string; generation: number; status: ExecutionStatus
  cancellation: CancellationState; processIds: string[]; reports: Array<{status: ExecutionStatus; detail: unknown; revision: number; reportedAtMs: number}>
  revision: number; updatedAtMs: number
}
export interface ServiceProcessRecord {
  processId: string; runId: string | null; attemptId: string | null; alive: boolean
  exitStatus: string | null; revision: number; updatedAtMs: number
}
export interface ServiceFlowTaskRecord {
  taskId: string; postId: string; title: string; stageId: string | null; state: string; waitingReason: string | null
  attemptId: string | null; generation: number | null; lastSourceCursor: number
  attemptsStarted: number; dispatchId: string | null; dispatchPending: boolean
  cancelRequested?: boolean
  runtimeRunId: string | null; processId: string | null; lastOutcome: ServiceFlowAttemptOutcome | null
  supervisorRunId?: string | null; supervisorActivityId?: string | null
}
export interface ServiceFlowRunRecord {
  flowId: string; boardId: string; coordinatorId: string; coordinatorFence: number
  flowRevision: number; currentStage: string | null; stageTransitions: Record<string, string[]>; waitingReason: string | null; completed: boolean; maxAttempts: number; sourceCursor: number
  tasks: Record<string, ServiceFlowTaskRecord>; pendingSync: boolean; syncError: string | null
}
export interface ServiceFlowTaskProjection {
  taskId: string; postId: string; title: string; stageId?: string | null; dependencies: string[]; assignee: string | null
  state: string; waitingReason: string | null; attemptId: string | null
  generation: number | null; lastSourceCursor: number
}
export interface ServiceFlowRunProjection {
  flowId: string; boardId: string; flowRevision: number; currentStage?: string | null
  stageTransitions?: Record<string, string[]>; maxAttempts?: number
  tasks: ServiceFlowTaskProjection[]
}
export interface ServiceApprovalRequestRecord {
  attemptId: string; generation: number; requestId: string; flowId: string; taskId: string; postId: string
  kind: string; reason: string | null; payload: unknown | null; state: ApprovalRequestState
  requestedAtMs: number; deadlineAtMs: number; resolutionId: string | null; grant: 'once' | 'session' | null
  resolutionReason: string | null; payloadRedacted?: boolean
}
export interface ServiceWorkerToolRequestRecord {
  attemptId: string; generation: number; requestId: string; flowId: string; taskId: string; postId: string
  coordinatorFence: number; toolName: string; arguments: unknown | null; state: WorkerToolRequestState
  requestedAtMs: number; deadlineAtMs: number; claimId: string | null; resolutionId: string | null
  result: unknown | null; error: string | null; argumentsRedacted?: boolean; resultRedacted?: boolean
}
export interface SupervisorServiceSnapshot {
  managerId: string; epoch: string; cursor: number; counts: ServiceCounts
  runs: ServiceRunRecord[]; activities: ServiceActivityRecord[]; attempts: ServiceAttemptRecord[]; processes: ServiceProcessRecord[]
  flowRuns?: ServiceFlowRunRecord[]
  workflowDispatchConfigured?: boolean
  approvals?: ServiceApprovalRequestRecord[]
  toolRequests?: ServiceWorkerToolRequestRecord[]
}
export interface CoordinatorEvent {
  sequence: number; cursor: number; epoch: string; kind: string; runId?: string; activityId?: string
  attemptId?: string; processId?: string; revision: number; occurredAtMs: number; detail: unknown
}
export interface SubscriptionStart {
  epoch: string; cursor: number; resyncRequired: boolean; events: CoordinatorEvent[]; snapshot?: SupervisorServiceSnapshot
}
export interface ServiceFlowAttemptOutcome {
  attemptId: string; generation: number; sourceCursor: number; status: ExecutionStatus
  reason: string | null; workflowCancelRequested: boolean; retryable: boolean; evidence?: unknown
}
export interface RegisteredAttempt {
  attemptId: string; generation: number; reporterToken: string; cancelToken: string
}
export interface ManagedProcessContext {
  projectId?: string
  flowId?: string
  taskId?: string
  postId?: string
  workerId?: string
  runtimeRunId?: string
}
export interface ManagedProcessRequest {
  framework: 'codex' | 'claude'
  executable: string
  args: string[]
  options: { cwd?: string; env?: Record<string, string | undefined> }
  context?: ManagedProcessContext
  attemptId?: string
  generation?: number
  onSupervisorStop?(event: { processId: string; actor: 'tui' }): void
}
export class SupervisorClient extends EventEmitter {
  static connect(options: { socketPath: string; tokenPath?: string; token?: string; role?: SupervisorRole }): Promise<SupervisorClient>
  readonly role: SupervisorRole
  start(options: SupervisorLaunch): Promise<SupervisorProcessHandle>
  attach(processId: string): Promise<SupervisorProcessHandle>
  snapshot(): Promise<SupervisorServiceSnapshot>
  registerRun(context?: Record<string, unknown>): Promise<{ runId: string }>
  registerActivity(options: { runId: string; kind?: string }): Promise<{ activityId: string }>
  registerAttempt(options: { activityId: string }): Promise<RegisteredAttempt>
  setActivityStatus(options: { activityId: string; status: ActivityStatus; reason?: string }): Promise<ServiceActivityRecord>
  setWaiting(options: { activityId: string; reason?: string; status?: 'waitingForInput' | 'waitingForApproval' }): Promise<ServiceActivityRecord>
  setRunStatus(options: { runId: string; status: ExecutionStatus }): Promise<ServiceRunRecord>
  setActivityTimeout(options: { activityId: string; timeoutMs: number }): Promise<{ type: 'activityTimeoutSet'; activityId: string; timeoutMs: number }>
  registerFlowRun(options: { projection: ServiceFlowRunProjection; coordinatorId?: string; invocations?: Record<string, unknown> }): Promise<ServiceFlowRunRecord>
  updateFlowInvocations(options: { flowId: string; invocations: Record<string, unknown>; fence: number }): Promise<{ flowId: string; count: number }>
  claimFlowRun(options: { flowId: string; coordinatorId?: string; expectedFence: number }): Promise<ServiceFlowRunRecord>
  advanceFlowStage(options: { flowId: string; expectedStage: string; nextStage: string; expectedFlowRevision: number; fence: number }): Promise<ServiceFlowRunRecord>
  completeFlowRun(options: { flowId: string; expectedStage: string; expectedFlowRevision: number; evidence: unknown; fence: number }): Promise<ServiceFlowRunRecord>
  listPendingApprovals(): Promise<ServiceApprovalRequestRecord[]>
  waitApproval(options: { attemptId: string; generation: number; approvalRequestId: string }): Promise<ServiceApprovalRequestRecord>
  resolveApproval(options: { attemptId: string; generation: number; approvalRequestId: string; resolutionId: string; decision: 'approved' | 'denied'; grant?: 'once' | 'session'; fence: number }): Promise<ServiceApprovalRequestRecord>
  listPendingToolRequests(): Promise<ServiceWorkerToolRequestRecord[]>
  claimToolRequest(options: { attemptId: string; generation: number; requestId: string; claimId: string; fence: number }): Promise<ServiceWorkerToolRequestRecord>
  waitToolRequest(options: { attemptId: string; generation: number; requestId: string }): Promise<ServiceWorkerToolRequestRecord>
  resolveToolRequest(options: { attemptId: string; generation: number; requestId: string; claimId: string; resolutionId: string; result?: unknown; error?: string; fence: number }): Promise<ServiceWorkerToolRequestRecord>
  bindFlowAttempt(options: { flowId: string; taskId: string; attemptId: string; generation: number; sourceCursor: number; fence: number }): Promise<ServiceFlowRunRecord>
  reconcileFlowAttempt(options: { flowId: string; taskId: string; outcome: { attemptId: string; generation: number; sourceCursor: number; status: ExecutionStatus; reason?: string | null; workflowCancelRequested?: boolean; retryable?: boolean }; fence: number }): Promise<ServiceFlowRunRecord>
  applyFlowReview(options: { flowId: string; taskId: string; flowRevision: number; evidence: unknown; fence: number }): Promise<ServiceFlowRunRecord>
  cancelFlowTask(options: { flowId: string; taskId: string; attemptId: string; generation: number; fence: number }): Promise<ServiceFlowRunRecord>
  cancelAttempt(options: { attemptId: string; generation: number }): Promise<{ attemptId: string; cancellation: CancellationState }>
  shutdown(graceMs?: number): Promise<{ reaped: number }>
  report(options: { attemptId: string; generation: number; status: ExecutionStatus; detail?: unknown }): Promise<{ attemptId: string; revision: number }>
  pollAttempt(options: { attemptId: string; generation: number }): Promise<{ attempt: ServiceAttemptRecord }>
  subscribe(filters?: { epoch?: string; cursor?: number; runId?: string; activityId?: string; attemptId?: string }): Promise<SupervisorSubscription>
  revokeRole(role: 'view' | 'tui'): Promise<{ role: 'view' | 'tui' }>
  close(): void
}
export class SupervisorSubscription extends EventEmitter {
  readonly start: SubscriptionStart
  on(event: 'event', listener: (event: CoordinatorEvent) => void): this
  on(event: 'resyncRequired', listener: (value: { type: 'resyncRequired'; epoch: string; cursor: number; snapshot?: SupervisorServiceSnapshot }) => void): this
  on(event: 'disconnect', listener: (error: Error) => void): this
  close(): void
}
export class SupervisorProcessHandle extends EventEmitter {
  readonly processId: string | null
  /** One-process capability for reconnecting a process output reader. */
  readonly outputToken: string | null
  readonly ready: Promise<this>
  readonly exit: Promise<unknown>
  readonly stdout: PassThrough
  readonly stderr: PassThrough
  readonly stdin: Writable
  pid: number | null
  exitCode: number | null
  signalCode: number | null
  killed: boolean
  stop(graceMs?: number): Promise<{ status: string; processId: string }>
  kill(signal?: NodeJS.Signals): boolean
  wait(): Promise<unknown>
}
export function createManagedProcessLauncher(options: {
  client: SupervisorClient
  policyForRequest(request: ManagedProcessRequest): object
}): {
  start(request: ManagedProcessRequest): SupervisorProcessHandle
  stop(handle: SupervisorProcessHandle, graceMs?: number): Promise<unknown>
}
export function validateProviderEnvironment(request: ManagedProcessRequest, policy: object): void

export interface CommandOptions { cwd?: string }
export interface CommandInput { argv: string[]; cwd?: string }
export interface AgentRunOptions { timeoutMs?: number; shutdownGraceMs?: number }
export interface ExecutionOutput {
  success: boolean
  stdout: Buffer
  stderr: Buffer
  enforcement: unknown
  executable: unknown
  termination: unknown
}
export interface NativeProcessEvent {
  kind: 'started' | 'stdout' | 'stderr' | 'exited'
  processId?: number
  data?: Buffer
  receipt?: unknown
  executable?: unknown
}
export interface ManagedProcessResult {
  stdout: Buffer
  stderr: Buffer
  outputTruncated: boolean
  receipt: unknown
  error?: string | null
}
export interface ManagedProcessInfo {
  supervisorProcessId: string
  processId: number
  alive: boolean
  parentProcessId: number | null
  startTimeTicks: string | null
  executable: string | null
  framework: string | null
  runLabel: string | null
  projectId: string | null
  flowRunId: string | null
  taskId: string | null
  postId: string | null
  workerId: string | null
  runtimeRunId: string | null
}

export class Command {
  constructor(argv: string[], options?: CommandOptions)
  readonly argv: string[]
  readonly cwd?: string
}
export class Policy {
  constructor(options?: Record<string, unknown>)
  environment: Record<string, unknown>
  filesystem: unknown
  network: string
  limits: Record<string, unknown>
  enforcement: string
}
export class AgentRunPolicy {
  constructor(options?: AgentRunOptions)
  readonly timeoutMs: number
  readonly shutdownGraceMs: number
}
export class CancellationToken {
  constructor()
  readonly isCancelled: boolean
  cancel(): void
}
export class SupervisorProcessId {
  constructor(token: string)
  readonly token: string
  toString(): string
  toJSON(): string
}
export class SupervisedProcess implements AsyncIterable<NativeProcessEvent> {
  readonly supervisorProcessId: SupervisorProcessId | null
  write(data: Uint8Array | string): Promise<void>
  closeStdin(): Promise<void>
  wait(): Promise<unknown>
  shutdown(options?: { graceMs?: number }): Promise<unknown>
  nextEvent(): Promise<NativeProcessEvent | null>
  events(): AsyncGenerator<NativeProcessEvent>
  [Symbol.asyncIterator](): AsyncGenerator<NativeProcessEvent>
}
export interface SupervisorEvent {
  sequence: string
  runId?: string | null
  processId?: string | null
  kind: 'runStarted' | 'runEnded' | 'started' | 'stdout' | 'stderr' | 'exited' | 'agent' | 'lagged'
  data?: Buffer | null
  receipt?: unknown
  executable?: unknown
  payload?: unknown
  dropped?: string
}
export class SupervisorEventStream implements AsyncIterable<SupervisorEvent> {
  nextEvent(): Promise<SupervisorEvent | null>
  events(): AsyncGenerator<SupervisorEvent>
  [Symbol.asyncIterator](): AsyncGenerator<SupervisorEvent>
}
export class AgentRun {
  readonly id: string
  readonly activeProcesses: number
  runCommand(command: Command | CommandInput | string[], options?: { policy?: object; input?: Uint8Array }): Promise<ExecutionOutput>
  start(command: Command | CommandInput | string[], options?: { policy?: object }): SupervisedProcess
  cancel(): void
  cancelNow(): void
  waitCleanup(): Promise<void>
  publishAgentEvent(payload: unknown): void
  stream(): SupervisorEventStream
  run<T>(operation: (run: AgentRun, options: { signal: AbortSignal }) => T | Promise<T>, options?: { signal?: AbortSignal }): Promise<T>
}
export class Supervisor {
  constructor()
  readonly activeProcesses: number
  readonly activeAgentRuns: number
  startAgentRun(policy?: AgentRunPolicy | AgentRunOptions): AgentRun
  runCommand(command: Command | CommandInput | string[], options?: { policy?: object; input?: Uint8Array; cancellation?: CancellationToken; signal?: AbortSignal }): Promise<ExecutionOutput>
  start(command: Command | CommandInput | string[], options?: { policy?: object }): SupervisedProcess
  startBackground(command: Command | CommandInput | string[], options?: { policy?: object }): Promise<SupervisorProcessId>
  processStatus(processId: SupervisorProcessId | string): 'running' | 'completed' | 'failed' | null
  processInfo(processId: SupervisorProcessId | string): ManagedProcessInfo | null
  processResult(processId: SupervisorProcessId | string): ManagedProcessResult | null
  takeProcessResult(processId: SupervisorProcessId | string): ManagedProcessResult | null
  forgetProcess(processId: SupervisorProcessId | string): boolean
  waitProcess(processId: SupervisorProcessId | string): Promise<ManagedProcessResult | null>
  writeProcessStdin(processId: SupervisorProcessId | string, data: Uint8Array | string): Promise<void>
  closeProcessStdin(processId: SupervisorProcessId | string): Promise<void>
  shutdownProcess(processId: SupervisorProcessId | string, options?: { graceMs?: number }): Promise<string>
  shutdownAll(options?: { graceMs?: number }): Promise<Array<{ processId: string; stdout: Buffer; stderr: Buffer; outputTruncated: boolean; receipt: unknown; error?: string | null }>>
  startTui(executable: string, args?: string[]): Promise<void>
  stream(options?: { process?: SupervisorProcessId | string }): SupervisorEventStream
}
export function run(command: Command | CommandInput | string[], options?: { policy?: object; input?: Uint8Array; cancellation?: CancellationToken; signal?: AbortSignal }): Promise<ExecutionOutput>
export function platformCapabilities(): Record<string, boolean | string>
