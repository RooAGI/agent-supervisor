import { EventEmitter } from 'node:events'
import { PassThrough, Writable } from 'node:stream'

export type SupervisorRole = 'view' | 'control' | 'tui'
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
  onSupervisorStop?(event: { processId: string; actor: 'tui' }): void
}
export class SupervisorClient extends EventEmitter {
  static connect(options: { socketPath: string; tokenPath: string; role?: SupervisorRole }): Promise<SupervisorClient>
  readonly role: SupervisorRole
  start(options: SupervisorLaunch): Promise<SupervisorProcessHandle>
  attach(processId: string): Promise<SupervisorProcessHandle>
  snapshot(): Promise<{ processes: unknown[] }>
  revokeRole(role: 'view' | 'tui'): Promise<{ role: 'view' | 'tui' }>
  close(): void
}
export class SupervisorProcessHandle extends EventEmitter {
  readonly processId: string | null
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
