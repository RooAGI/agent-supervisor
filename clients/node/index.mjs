import { createReadStream, lstatSync } from 'node:fs'
import { createConnection } from 'node:net'
import { PassThrough, Writable } from 'node:stream'
import { EventEmitter } from 'node:events'

const MAX_FRAME_BYTES = 1024 * 1024

/** Client for the user-owned Agent Supervisor Unix socket service. */
export class SupervisorClient extends EventEmitter {
  static async connect({ socketPath, tokenPath, role = 'control' }) {
    const tokenStat = lstatSync(tokenPath)
    if (!tokenStat.isFile() || (tokenStat.mode & 0o077) !== 0 || (process.getuid && tokenStat.uid !== process.getuid())) {
      throw new Error('Supervisor token file must be a regular mode-0600 file owned by the current user')
    }
    const chunks = []
    for await (const chunk of createReadStream(tokenPath)) chunks.push(chunk)
    if (!['view', 'control', 'tui'].includes(role)) throw new Error('role must be view, control, or tui')
    const token = Buffer.concat(chunks).toString('utf8').trim()
    if (Buffer.byteLength(token) < 32) throw new Error('role token file must contain at least 32 bytes')
    const client = new SupervisorClient(socketPath, token, role)
    await client.ready
    return client
  }

  constructor(socketPath, token, role) {
    super()
    if (Buffer.byteLength(token) < 32) throw new Error('Supervisor token must contain at least 32 bytes')
    this.socket = createConnection(socketPath)
    this.pending = new Map()
    this.role = role
    this.handles = new Map()
    this.nextRequestId = 1
    this.buffer = ''
    this.socket.setEncoding('utf8')
    this.socket.on('data', (chunk) => this.#onData(chunk))
    this.socket.on('error', (error) => this.#fail(error))
    this.socket.on('close', () => this.#fail(new Error('Supervisor service connection closed')))
    this.ready = new Promise((resolve, reject) => {
      this.readyResolve = resolve
      this.readyReject = reject
    })
    this.socket.once('connect', () => this.socket.write(`${JSON.stringify({ type: 'auth', role, token })}\n`))
  }

  async start({ executable, args = [], cwd, env, metadata, policy }) {
    if (this.role !== 'control') throw new Error('control credential required to start processes')
    if (!policy) throw new Error('an explicit Supervisor policy is required')
    const handle = new SupervisorProcessHandle(this)
    await this.#startRequest({ executable, args, cwd, metadata, policy }, handle)
    return handle
  }

  startManaged(request, policy) {
    if (this.role !== 'control') throw new Error('control credential required to start processes')
    if (!policy) throw new Error('an explicit Supervisor policy is required')
    const handle = new SupervisorProcessHandle(this)
    this.#startRequest({
      executable: request.executable,
      args: request.args,
      cwd: typeof request.options?.cwd === 'string' ? request.options.cwd : undefined,
      metadata: providerMetadata(request),
      policy
    }, handle).catch((error) => handle._fail(error))
    return handle
  }

  #startRequest({ executable, args = [], cwd, metadata, policy }, handle) {
    const requestId = this.#requestId()
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject, kind: 'start', handle })
      this.#send({
        type: 'start', requestId,
        command: { executable, args, ...(cwd ? { cwd } : {}) },
        metadata: metadata ?? {}, policy
      })
    })
  }

  async attach(processId) {
    const requestId = this.#requestId()
    const handle = new SupervisorProcessHandle(this)
    const attached = await new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject, kind: 'attach', handle })
      this.#send({ type: 'attach', requestId, processId })
    })
    if (handle.processId !== attached.processId) throw new Error('Supervisor attached an unexpected process')
    return handle
  }

  async snapshot() {
    const requestId = this.#requestId()
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject, kind: 'snapshot' })
      this.#send({ type: 'snapshot', requestId })
    })
  }

  revokeRole(role) {
    if (this.role !== 'control') throw new Error('control credential required to revoke view or TUI access')
    if (role !== 'view' && role !== 'tui') throw new Error('only view and TUI roles can be revoked')
    return this._request({ type: 'revokeRole', role })
  }

  close() { this.socket.end() }

  #requestId() { return `node-${this.nextRequestId++}` }
  _request(value) {
    const requestId = this.#requestId()
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject })
      try { this.#send({ ...value, requestId }) }
      catch (error) { this.pending.delete(requestId); reject(error) }
    })
  }
  #send(value) {
    if (this.socket.destroyed) throw new Error('Supervisor service is disconnected')
    const frame = `${JSON.stringify(value)}\n`
    if (Buffer.byteLength(frame) > MAX_FRAME_BYTES) throw new Error('Supervisor request exceeds the 1 MiB frame limit')
    this.socket.write(frame)
  }
  #onData(chunk) {
    this.buffer += chunk
    if (Buffer.byteLength(this.buffer) > MAX_FRAME_BYTES && !this.buffer.includes('\n')) {
      this.#fail(new Error('Supervisor response exceeds the 1 MiB frame limit'))
      return
    }
    for (;;) {
      const end = this.buffer.indexOf('\n')
      if (end < 0) break
      const line = this.buffer.slice(0, end)
      this.buffer = this.buffer.slice(end + 1)
      if (Buffer.byteLength(line) + 1 > MAX_FRAME_BYTES) { this.#fail(new Error('Supervisor response exceeds the 1 MiB frame limit')); return }
      let message
      try { message = JSON.parse(line) } catch (error) { this.#fail(error); return }
      if (message.type === 'ready') { this.readyResolve(); continue }
      if (message.type === 'error' && message.code === 'unauthorized') {
        const error = new Error(message.message)
        this.readyReject(error)
        this.#fail(error)
        continue
      }
      const pending = this.pending.get(message.requestId)
      if (pending && ['started', 'attached', 'stopped', 'snapshot', 'roleRevoked', 'ack', 'error'].includes(message.type)) {
        this.pending.delete(message.requestId)
        if (message.type === 'error') pending.reject(Object.assign(new Error(message.message), { code: message.code }))
        else if (message.type === 'ack' && !message.ok) pending.reject(new Error('Supervisor rejected stdin request'))
        else {
          if (pending.kind === 'start' || pending.kind === 'attach') {
            const handle = pending.handle ?? new SupervisorProcessHandle(this)
            handle._bind(message.processId)
            this.handles.set(message.processId, handle)
            pending.resolve(message)
          } else pending.resolve(message)
        }
        continue
      }
      const handle = this.handles.get(message.processId)
      if (handle) {
        handle._event(message)
        if (message.type === 'exited') this.handles.delete(message.processId)
      }
      else this.emit('event', message)
    }
  }
  #fail(error) {
    this.readyReject?.(error)
    for (const { reject } of this.pending.values()) reject(error)
    this.pending.clear()
    for (const handle of this.handles.values()) handle._disconnect(error)
    this.emit('disconnect', error)
  }
}

export class SupervisorProcessHandle extends EventEmitter {
  constructor(client, processId = null) {
    super()
    this.client = client
    this.processId = processId
    this.pid = null
    this.exitCode = null
    this.signalCode = null
    this.killed = false
    this.stdout = new PassThrough()
    this.stderr = new PassThrough()
    this.stdin = new Writable({ write: (chunk, encoding, callback) => {
      this.#write(chunk).then(() => callback(), callback)
    }, final: (callback) => this.#closeStdin().then(() => callback(), callback) })
    this.exit = new Promise((resolve) => { this.resolveExit = resolve })
    this.ready = new Promise((resolve, reject) => { this.resolveReady = resolve; this.rejectReady = reject })
    this.ready.catch(() => {})
  }
  async stop(graceMs = 1500) {
    if (this.client.role !== 'control' && this.client.role !== 'tui') throw new Error('control or TUI credential required to stop processes')
    this.killed = true
    return this.#control({ type: 'stop', processId: this.processId, graceMs })
  }
  kill(signal = 'SIGTERM') { void this.stop(signal === 'SIGKILL' ? 0 : 1500); return true }
  wait() { return this.exit }
  async #write(chunk) { await this.ready; return this.#control({ type: 'stdin', processId: this.processId, data: Buffer.from(chunk).toString('base64') }) }
  async #closeStdin() { await this.ready; return this.#control({ type: 'closeStdin', processId: this.processId }) }
  async #control(value) {
    if (this.client.role !== 'control' && !(this.client.role === 'tui' && value.type === 'stop')) throw new Error('credential does not permit this process control')
    await this.ready
    return this.client._request(value)
  }
  _event(message) {
    if (this.processId && message.processId && message.processId !== this.processId) return
    if (message.type === 'exited' && this.exitObserved) return
    if (message.type === 'exited') this.exitObserved = true
    if (message.type === 'processStarted') this.pid = message.pid ?? null
    if (message.type === 'stdout') this.stdout.write(Buffer.from(message.data, 'base64'))
    else if (message.type === 'stderr') this.stderr.write(Buffer.from(message.data, 'base64'))
    else if (message.type === 'exited') {
      const termination = message.receipt?.termination
      if (termination?.Exited) { this.exitCode = termination.Exited.code ?? null; this.signalCode = termination.Exited.signal ?? null }
      this.stdout.end(); this.stderr.end(); this.stdin.destroy()
      this.resolveExit(message.receipt)
      this.emit('receipt', message.receipt)
      this.emit('exit', this.exitCode, this.signalCode)
      this.emit('close', this.exitCode, this.signalCode)
    } else if (message.type === 'stop_requested') {
      if (message.actor === 'tui') this.tuiStopRequested = message
      this.emit(message.type, message)
    } else this.emit(message.type, message)
    if (message.type === 'exited' && !this.tuiStopConfirmed && this.tuiStopRequested?.processId === message.processId) {
      this.tuiStopConfirmed = true
      this.emit('supervisorStop', { ...this.tuiStopRequested, receipt: message.receipt })
    }
  }
  _disconnect(error) { this.stdout.destroy(); this.stderr.destroy(); this.stdin.destroy(); this.emit('disconnect', error) }
  _bind(processId) { this.processId = processId; this.resolveReady(this); this.emit('spawn') }
  _fail(error) {
    this.rejectReady(error)
    this.stdout.destroy(error); this.stderr.destroy(error); this.stdin.destroy(error)
    this.emit('error', error)
    this.emit('close', null, null)
  }
}

/** Adapt provider launch requests to the existing desktop launcher seam. */
export function createManagedProcessLauncher({ client, policyForRequest }) {
  return {
    start(request) {
      const policy = policyForRequest(request)
      validateProviderEnvironment(request, policy)
      const handle = client.startManaged(request, policy)
      if (request.onSupervisorStop) {
        handle.on('supervisorStop', (event) => {
          request.onSupervisorStop({ processId: event.processId, actor: 'tui' })
        })
      }
      return handle
    },
    stop(handle, graceMs = 1500) { return handle.stop(graceMs) }
  }
}

/** Provider auth values are transmitted only when policy explicitly releases the same values. */
export function validateProviderEnvironment(request, policy) {
  const environment = request.options?.env ?? {}
  const released = policy?.environment?.variables ?? {}
  const required = request.framework === 'claude'
    ? ['ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'CLAUDE_CODE_OAUTH_TOKEN']
    : request.framework === 'codex'
      ? ['CODEX_HOME', 'ACCESS_TOKEN']
      : []
  for (const key of required) {
    if (Object.hasOwn(environment, key) && String(environment[key] ?? '') !== String(released[key] ?? '')) {
      throw new Error(`Supervisor policy must explicitly release ${key} for ${request.framework}`)
    }
  }
}

function providerMetadata(request) {
  const context = request.context ?? {}
  return {
    framework: request.framework,
    projectId: context.projectId,
    flowRunId: context.flowId,
    taskId: context.taskId,
      postId: context.postId,
      workerId: context.workerId,
      runtimeRunId: context.runtimeRunId
  }
}
