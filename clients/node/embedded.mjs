import { nativeBinding } from './native.mjs'

const DEFAULT_POLICY = Object.freeze({
  environment: { inherit: [], variables: {}, executableSearchPaths: [] },
  filesystem: null,
  network: 'host',
  limits: {
    timeoutMs: 5000,
    inputBytes: 65536,
    outputBytes: 65536,
    stderrBytes: 16384,
    memoryBytes: null,
    maxProcesses: null,
    cpuQuotaMicros: null
  },
  enforcement: 'best_effort'
})

/** A direct-argv command. The executable and arguments are never shell-parsed. */
export class Command {
  constructor(argv, { cwd } = {}) {
    if (!Array.isArray(argv) || argv.length === 0 || argv.some((part) => typeof part !== 'string')) {
      throw new TypeError('Command expects a non-empty argv string array')
    }
    this.argv = [...argv]
    this.cwd = cwd
  }
}

/** Policy values use the same camelCase JSON representation as the Rust API. */
export class Policy {
  constructor(options = {}) {
    Object.assign(this, structuredClone(DEFAULT_POLICY), options)
  }
}

export class AgentRunPolicy {
  constructor({ timeoutMs = 300_000, shutdownGraceMs = 5_000 } = {}) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw new RangeError('timeoutMs must be a positive safe integer')
    if (!Number.isSafeInteger(shutdownGraceMs) || shutdownGraceMs < 0 || shutdownGraceMs > 60_000) {
      throw new RangeError('shutdownGraceMs must be between 0 and 60000')
    }
    this.timeoutMs = timeoutMs
    this.shutdownGraceMs = shutdownGraceMs
  }
}

/** Cloneable cancellation signal for a one-shot command. */
export class CancellationToken {
  constructor() {
    const { CancellationToken: NativeCancellationToken } = nativeBinding()
    this._native = new NativeCancellationToken()
  }
  cancel() { this._native.cancel() }
  get isCancelled() { return this._native.isCancelled }
}

/** Return the native backend capability report for this operating system. */
export function platformCapabilities() {
  return nativeBinding().platformCapabilities()
}

function commandInput(command) {
  if (command instanceof Command) return { argv: command.argv, cwd: command.cwd }
  if (Array.isArray(command)) return { argv: command }
  if (command && Array.isArray(command.argv)) return { argv: command.argv, cwd: command.cwd }
  throw new TypeError('command must be a Command, argv array, or { argv, cwd } object')
}

function policyInput(policy) {
  return policy instanceof Policy ? { ...policy } : policy ?? undefined
}

/** Embedded process and agent-run owner backed by the Rust supervisor core. */
export class Supervisor {
  constructor() {
    const { Supervisor: NativeSupervisor } = nativeBinding()
    this._native = new NativeSupervisor()
  }

  get activeProcesses() { return this._native.activeProcesses }
  get activeAgentRuns() { return this._native.activeAgentRuns }

  startAgentRun(policy = new AgentRunPolicy()) {
    const normalized = policy instanceof AgentRunPolicy ? policy : new AgentRunPolicy(policy)
    const native = this._native.startAgentRun(normalized.timeoutMs, normalized.shutdownGraceMs)
    return new AgentRun(native, normalized)
  }

  async runCommand(command, { policy, input, cancellation, signal } = {}) {
    const token = cancellation ?? (signal ? new CancellationToken() : undefined)
    if (signal?.aborted) token?.cancel()
    const abortListener = signal ? () => token?.cancel() : undefined
    signal?.addEventListener('abort', abortListener, { once: true })
    try {
      return await this._native.runCommand(
        commandInput(command), policyInput(policy),
        input == null ? undefined : Buffer.from(input), token?._native
      )
    } finally {
      if (abortListener) signal.removeEventListener('abort', abortListener)
    }
  }

  start(command, { policy } = {}) {
    return new SupervisedProcess(this._native.start(commandInput(command), policyInput(policy)))
  }

  async startBackground(command, { policy } = {}) {
    return new SupervisorProcessId(await this._native.startBackground(commandInput(command), policyInput(policy)))
  }

  processStatus(processId) { return this._native.processStatus(tokenOf(processId)) }
  processInfo(processId) { return this._native.processInfo(tokenOf(processId)) }
  processResult(processId) {
    const result = this._native.processResult(tokenOf(processId))
    return result == null ? null : normalizeManagedResult(result)
  }
  takeProcessResult(processId) {
    const result = this._native.takeProcessResult(tokenOf(processId))
    return result == null ? null : normalizeManagedResult(result)
  }
  forgetProcess(processId) { return this._native.forgetProcess(tokenOf(processId)) }
  async waitProcess(processId) {
    const result = await this._native.waitProcess(tokenOf(processId))
    return result == null ? null : normalizeManagedResult(result)
  }
  writeProcessStdin(processId, data) { return this._native.writeProcessStdin(tokenOf(processId), Buffer.from(data)) }
  closeProcessStdin(processId) { return this._native.closeProcessStdin(tokenOf(processId)) }
  shutdownProcess(processId, { graceMs = 5_000 } = {}) { return this._native.shutdownProcess(tokenOf(processId), graceMs) }
  async shutdownAll({ graceMs = 5_000 } = {}) {
    return (await this._native.shutdownAll(graceMs)).map(normalizeManagedResult)
  }
  startTui(executable, args = []) { return this._native.startTui(executable, args) }
  stream({ process } = {}) {
    return new SupervisorEventStream(this._native.stream(process == null ? undefined : tokenOf(process)))
  }
}

/** Lifecycle and cancellation scope for one framework invocation. */
export class AgentRun {
  constructor(native, policy) {
    this._native = native
    this.policy = policy
  }

  get id() { return this._native.id }
  get activeProcesses() { return this._native.activeProcesses }

  runCommand(command, { policy, input } = {}) {
    return this._native.runCommand(commandInput(command), policyInput(policy), input == null ? undefined : Buffer.from(input))
  }

  start(command, { policy } = {}) {
    return new SupervisedProcess(this._native.start(commandInput(command), policyInput(policy)))
  }

  cancel() { this._native.cancel() }
  cancelNow() { this._native.cancelNow() }
  waitCleanup() { return this._native.waitCleanup() }
  publishAgentEvent(payload) { this._native.publishAgentEvent(payload) }
  stream() { return new SupervisorEventStream(this._native.stream()) }

  /**
   * Run one framework operation with a deadline and optional AbortSignal.
   * The run always cleans up its registered processes before settling.
   */
  async run(operation, { signal } = {}) {
    if (typeof operation !== 'function') throw new TypeError('operation must be a function')
    if (signal?.aborted) {
      this.cancel()
      await this.waitCleanup()
      throw signal.reason ?? new DOMException('The operation was aborted', 'AbortError')
    }

    let timer
    const operationController = new AbortController()
    let abortListener
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        operationController.abort(new DOMException('The agent run deadline expired', 'TimeoutError'))
        this.cancel()
        const error = new Error(`agent run exceeded its ${this.policy.timeoutMs}ms deadline`)
        error.name = 'TimeoutError'
        reject(error)
      }, this.policy.timeoutMs)
    })
    const aborted = new Promise((_, reject) => {
      if (!signal) return
      abortListener = () => {
        operationController.abort(signal.reason)
        this.cancel()
        reject(signal.reason ?? new DOMException('The operation was aborted', 'AbortError'))
      }
      signal.addEventListener('abort', abortListener, { once: true })
    })

    try {
      return await Promise.race([Promise.resolve().then(() => operation(this, { signal: operationController.signal })), timeout, aborted])
    } finally {
      clearTimeout(timer)
      if (abortListener) signal.removeEventListener('abort', abortListener)
      await this.waitCleanup()
    }
  }
}

/** Caller-owned interactive child started by an embedded Supervisor. */
export class SupervisedProcess {
  constructor(native) { this._native = native }
  get supervisorProcessId() {
    return this._native.supervisorProcessId == null ? null : new SupervisorProcessId(this._native.supervisorProcessId)
  }
  write(data) { return this._native.write(Buffer.from(data)) }
  closeStdin() { return this._native.closeStdin() }
  wait() { return this._native.wait() }
  shutdown({ graceMs = 5_000 } = {}) { return this._native.shutdown(graceMs) }
  nextEvent() { return this._native.nextEvent() }
  async *events() {
    for (;;) {
      const event = await this.nextEvent()
      if (event == null) return
      yield event
    }
  }
  [Symbol.asyncIterator]() { return this.events() }
}

/** Ordered lifecycle and output events from the embedded supervisor log. */
export class SupervisorEventStream {
  constructor(native) { this._native = native }
  nextEvent() { return this._native.nextEvent() }
  async *events() {
    for (;;) {
      const event = await this.nextEvent()
      if (event == null) return
      yield event
    }
  }
  [Symbol.asyncIterator]() { return this.events() }
}

/** Stable process identity scoped to the Supervisor instance that created it. */
export class SupervisorProcessId {
  constructor(token) {
    if (typeof token !== 'string' || !token) throw new TypeError('process token must be a non-empty string')
    this.token = token
    Object.freeze(this)
  }
  toString() { return this.token }
  toJSON() { return this.token }
}

function tokenOf(value) {
  return value instanceof SupervisorProcessId ? value.token : String(value)
}

function normalizeManagedResult(value) {
  return { ...value, stdout: Buffer.from(value.stdout), stderr: Buffer.from(value.stderr) }
}

export async function run(command, options = {}) {
  return new Supervisor().runCommand(command, options)
}
