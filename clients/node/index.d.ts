import { EventEmitter } from 'node:events'
import { PassThrough, Writable } from 'node:stream'

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
