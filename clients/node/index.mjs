import { createReadStream, lstatSync } from 'node:fs'
import { createConnection } from 'node:net'
import { randomUUID } from 'node:crypto'
import { PassThrough, Writable } from 'node:stream'
import { EventEmitter } from 'node:events'

const MAX_FRAME_BYTES = 1024 * 1024

/** Client for the user-owned Agent Supervisor Unix socket service. */
export class SupervisorClient extends EventEmitter {
  static async connect({ socketPath, tokenPath, token: suppliedToken, role = 'control' }) {
    if (!['view', 'control', 'tui', 'reporter', 'canceller', 'output'].includes(role)) throw new Error('role must be view, control, tui, reporter, canceller, or output')
    let token = suppliedToken
    if (token === undefined) {
      if (!tokenPath) throw new Error('tokenPath or an issued capability token is required')
      const tokenStat = lstatSync(tokenPath)
      if (!tokenStat.isFile() || (tokenStat.mode & 0o077) !== 0 || (process.getuid && tokenStat.uid !== process.getuid())) {
        throw new Error('Supervisor token file must be a regular mode-0600 file owned by the current user')
      }
      const chunks = []
      for await (const chunk of createReadStream(tokenPath)) chunks.push(chunk)
      token = Buffer.concat(chunks).toString('utf8').trim()
    }
    if (typeof token !== 'string') throw new Error('Supervisor token must be a string')
    if (Buffer.byteLength(token) < 32) throw new Error('role token file must contain at least 32 bytes')
    const client = new SupervisorClient(socketPath, token, role)
    await client.ready
    return client
  }

  constructor(socketPath, token, role) {
    super()
    this.socketPath = socketPath
    Object.defineProperty(this, 'token', { value: token, enumerable: false })
    if (Buffer.byteLength(token) < 32) throw new Error('Supervisor token must contain at least 32 bytes')
    this.socket = createConnection(socketPath)
    this.pending = new Map()
    this.role = role
    this.handles = new Map()
    this.coordinatorSubscription = null
    this.pendingCoordinatorMessages = []
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

  async start({ executable, args = [], cwd, env, metadata, policy, attemptId, generation, stopProcessGroupOnExit }) {
    if (this.role !== 'control') throw new Error('control credential required to start processes')
    if (!policy) throw new Error('an explicit Supervisor policy is required')
    if ((attemptId === undefined) !== (generation === undefined)) throw new Error('attemptId and generation must be provided together')
    const handle = new SupervisorProcessHandle(this)
    const started = await this.#startRequest({ executable, args, cwd, metadata, policy, attemptId, generation, stopProcessGroupOnExit }, handle)
    handle._setOutputToken(started.outputToken)
    return handle
  }

  async registerRun(context = {}) {
    this.#requireControl('register runs')
    const response = await this._request({ type: 'registerRun', context })
    return response.runId ? response : { ...response.run, runId: response.run?.runId }
  }
  async registerActivity({ runId, kind = 'activity' }) {
    this.#requireControl('register activities')
    const response = await this._request({ type: 'registerActivity', runId, kind })
    return response.activityId ? response : { ...response.activity, activityId: response.activity?.activityId }
  }
  async registerAttempt({ activityId }) {
    this.#requireControl('register attempts')
    const response = await this._request({ type: 'registerAttempt', activityId })
    return response.attemptId ? response : { ...response.attempt, reporterToken: response.reporterToken, cancelToken: response.cancelToken }
  }
  async setActivityStatus({ activityId, status, reason }) {
    this.#requireControl('report activity status')
    if (typeof activityId !== 'string' || !activityId || typeof status !== 'string') throw new TypeError('activityId and status are required')
    const response = await this._request({ type: 'setActivityStatus', activityId, status, ...(reason !== undefined ? { reason } : {}) })
    if (!response.activity) throw new Error('Supervisor response omitted activity')
    return response.activity
  }
  async setWaiting({ activityId, reason, status = 'waitingForInput' }) {
    if (!['waitingForInput', 'waitingForApproval'].includes(status)) throw new TypeError('status must be waitingForInput or waitingForApproval')
    return this.setActivityStatus({ activityId, status, reason })
  }
  async setRunStatus({ runId, status }) {
    this.#requireControl('report run status')
    if (typeof runId !== 'string' || !runId || typeof status !== 'string') throw new TypeError('runId and status are required')
    const response = await this._request({ type: 'setRunStatus', runId, status })
    if (!response.run) throw new Error('Supervisor response omitted run')
    return response.run
  }
  async setActivityTimeout({ activityId, timeoutMs }) {
    this.#requireControl('set activity timeout')
    if (typeof activityId !== 'string' || !activityId || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 86400000) {
      throw new TypeError('activityId and timeoutMs between 1 and 86400000 are required')
    }
    return this._request({ type: 'setActivityTimeout', activityId, timeoutMs })
  }
  async registerFlowRun({ projection, coordinatorId = 'supervisor', invocations }) {
    this.#requireControl('register FlowRuns')
    if (!projection || typeof projection !== 'object' || typeof projection.flowId !== 'string') {
      throw new TypeError('a valid FlowRun projection is required')
    }
    return this.#flowRunRequest({ type: 'registerFlowRun', projection, coordinatorId, ...(invocations ? { invocations } : {}) })
  }
  async claimFlowRun({ flowId, coordinatorId = 'supervisor', expectedFence }) {
    this.#requireControl('claim FlowRuns')
    if (typeof flowId !== 'string' || !flowId || !Number.isSafeInteger(expectedFence) || expectedFence < 0) {
      throw new TypeError('flowId and a non-negative expectedFence are required')
    }
    return this.#flowRunRequest({ type: 'claimFlowRun', flowId, coordinatorId, expectedFence })
  }
  async advanceFlowStage({ flowId, expectedStage, nextStage, expectedFlowRevision, fence }) {
    this.#requireControl('advance FlowRun stages')
    if (typeof flowId !== 'string' || !flowId || typeof expectedStage !== 'string' || !expectedStage ||
        typeof nextStage !== 'string' || !nextStage ||
        ![expectedFlowRevision, fence].every((value) => Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError('flowId, stage IDs, expectedFlowRevision, and fence are required')
    }
    return this.#flowRunRequest({ type: 'advanceFlowStage', flowId, expectedStage, nextStage, expectedFlowRevision, fence })
  }
  async completeFlowRun({ flowId, expectedStage, expectedFlowRevision, evidence, fence }) {
    this.#requireControl('complete FlowRuns')
    if (typeof flowId !== 'string' || !flowId || typeof expectedStage !== 'string' || !expectedStage ||
        ![expectedFlowRevision, fence].every((value) => Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError('flowId, expectedStage, expectedFlowRevision, and fence are required')
    }
    return this.#flowRunRequest({ type: 'completeFlowRun', flowId, expectedStage, expectedFlowRevision, evidence, fence })
  }
  async listPendingApprovals() {
    this.#requireControl('list pending worker approvals')
    const response = await this._request({ type: 'listPendingApprovals' })
    if (!Array.isArray(response.approvals)) throw new Error('Supervisor response omitted approvals')
    return response.approvals
  }
  async waitApproval({ attemptId, generation, approvalRequestId }) {
    this.#requireControl('wait for worker approval')
    if (typeof attemptId !== 'string' || !attemptId || typeof approvalRequestId !== 'string' || !approvalRequestId || !Number.isSafeInteger(generation) || generation < 0) {
      throw new TypeError('attemptId, generation, and approvalRequestId are required')
    }
    const response = await this._request({ type: 'waitApproval', attemptId, generation, approvalRequestId })
    if (!response.approval) throw new Error('Supervisor response omitted approval')
    return response.approval
  }
  async resolveApproval({ attemptId, generation, approvalRequestId, resolutionId, decision, grant, fence }) {
    this.#requireControl('resolve worker approvals')
    if (typeof attemptId !== 'string' || !attemptId || typeof approvalRequestId !== 'string' || !approvalRequestId ||
        typeof resolutionId !== 'string' || !resolutionId || !['approved', 'denied'].includes(decision) ||
        ![generation, fence].every((value) => Number.isSafeInteger(value) && value >= 0) ||
        (grant !== undefined && !['once', 'session'].includes(grant))) {
      throw new TypeError('approval identity, decision, and current fence are required')
    }
    const response = await this._request({ type: 'resolveApproval', attemptId, generation, approvalRequestId, resolutionId, decision, grant, fence })
    if (!response.approval) throw new Error('Supervisor response omitted approval')
    return response.approval
  }
  async listPendingToolRequests() {
    this.#requireControl('list pending worker tool requests')
    const response = await this._request({ type: 'listPendingToolRequests' })
    if (!Array.isArray(response.toolRequests)) throw new Error('Supervisor response omitted tool requests')
    return response.toolRequests
  }
  async claimToolRequest({ attemptId, generation, requestId, claimId, fence }) {
    this.#requireControl('claim worker tool requests')
    if (typeof attemptId !== 'string' || !attemptId || typeof requestId !== 'string' || !requestId ||
        typeof claimId !== 'string' || !claimId || ![generation, fence].every((value) => Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError('tool request identity, claimId, and current fence are required')
    }
    const response = await this._request({ type: 'claimToolRequest', attemptId, generation, toolRequestId: requestId, claimId, fence })
    if (!response.toolRequest) throw new Error('Supervisor response omitted tool request')
    return response.toolRequest
  }
  async waitToolRequest({ attemptId, generation, requestId }) {
    this.#requireControl('wait for worker tool requests')
    if (typeof attemptId !== 'string' || !attemptId || typeof requestId !== 'string' || !requestId || !Number.isSafeInteger(generation) || generation < 0) {
      throw new TypeError('attemptId, generation, and requestId are required')
    }
    const response = await this._request({ type: 'waitToolRequest', attemptId, generation, toolRequestId: requestId })
    if (!response.toolRequest) throw new Error('Supervisor response omitted tool request')
    return response.toolRequest
  }
  async resolveToolRequest({ attemptId, generation, requestId, claimId, resolutionId, result, error, fence }) {
    this.#requireControl('resolve worker tool requests')
    if (typeof attemptId !== 'string' || !attemptId || typeof requestId !== 'string' || !requestId ||
        typeof claimId !== 'string' || !claimId || typeof resolutionId !== 'string' || !resolutionId ||
        ![generation, fence].every((value) => Number.isSafeInteger(value) && value >= 0) ||
        (error !== undefined && typeof error !== 'string')) {
      throw new TypeError('tool request identity, resolution, and current fence are required')
    }
    const response = await this._request({ type: 'resolveToolRequest', attemptId, generation, toolRequestId: requestId, claimId, resolutionId, result, error, fence })
    if (!response.toolRequest) throw new Error('Supervisor response omitted tool request')
    return response.toolRequest
  }
  async updateFlowInvocations({ flowId, invocations, fence }) {
    this.#requireControl('update FlowRun invocations')
    if (typeof flowId !== 'string' || !flowId || !invocations || typeof invocations !== 'object' || Array.isArray(invocations) || !Number.isSafeInteger(fence) || fence < 0) {
      throw new TypeError('flowId, a task-keyed invocation object, and a non-negative fence are required')
    }
    return this._request({ type: 'updateFlowInvocations', flowId, invocations, fence })
  }
  async bindFlowAttempt({ flowId, taskId, attemptId, generation, sourceCursor, fence }) {
    this.#requireControl('bind FlowRun attempts')
    if (![generation, sourceCursor, fence].every((value) => Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError('generation, sourceCursor, and fence must be non-negative safe integers')
    }
    return this.#flowRunRequest({ type: 'bindFlowAttempt', flowId, taskId, attemptId, generation, sourceCursor, fence })
  }
  async reconcileFlowAttempt({ flowId, taskId, outcome, fence }) {
    this.#requireControl('reconcile FlowRun attempts')
    if (!outcome || typeof outcome !== 'object' || !Number.isSafeInteger(fence) || fence < 0) {
      throw new TypeError('a FlowRun attempt outcome and non-negative fence are required')
    }
    return this.#flowRunRequest({ type: 'reconcileFlowAttempt', flowId, taskId, outcome, fence })
  }
  async applyFlowReview({ flowId, taskId, flowRevision, evidence, fence }) {
    this.#requireControl('review FlowRun tasks')
    if (![flowRevision, fence].every((value) => Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError('flowRevision and fence must be non-negative safe integers')
    }
    return this.#flowRunRequest({ type: 'applyFlowReview', flowId, taskId, flowRevision, evidence, fence })
  }
  async cancelFlowTask({ flowId, taskId, attemptId, generation, fence }) {
    this.#requireControl('cancel FlowRun tasks')
    if (![generation, fence].every((value) => Number.isSafeInteger(value) && value >= 0)) {
      throw new TypeError('generation and fence must be non-negative safe integers')
    }
    return this.#flowRunRequest({ type: 'cancelFlowTask', flowId, taskId, attemptId, generation, fence })
  }
  async #flowRunRequest(request) {
    const response = await this._request(request)
    if (!response.flowRun || typeof response.flowRun !== 'object') {
      throw new Error(`Supervisor response omitted FlowRun for ${request.type}`)
    }
    return response.flowRun
  }
  async cancelAttempt({ attemptId, generation }) {
    if (this.role !== 'canceller') throw new Error('attempt-scoped cancellation credential required')
    if (typeof generation !== 'number') throw new Error('attempt generation is required')
    const response = await this._request({ type: 'cancelAttempt', attemptId, generation })
    const attempt = response.attempt
    return response.cancellation || response.cancelState
      ? response
      : { ...response, attemptId, cancellation: attempt?.cancellation ?? attempt?.cancelState }
  }
  async shutdown(graceMs = 1500) {
    this.#requireControl('shut down the service')
    return this._request({ type: 'shutdown', graceMs })
  }
  async report({ attemptId, generation, status, detail }) {
    if (this.role !== 'reporter') throw new Error('attempt-scoped reporter credential required')
    if (typeof generation !== 'number') throw new Error('attempt generation is required')
    const response = await this._request({ type: 'report', attemptId, generation, status, detail })
    return response.revision !== undefined ? response : { ...response, attemptId, revision: response.attempt?.revision }
  }
  async pollAttempt({ attemptId, generation }) {
    if (this.role !== 'reporter') throw new Error('attempt-scoped reporter credential required')
    if (typeof generation !== 'number') throw new Error('attempt generation is required')
    return this._request({ type: 'pollAttempt', attemptId, generation })
  }
  async subscribe({ epoch, cursor, runId, activityId, attemptId } = {}) {
    if (!['view', 'control', 'tui'].includes(this.role)) throw new Error('view, control, or TUI credential required to subscribe')
    const streamClient = await SupervisorClient.connect({ socketPath: this.socketPath, token: this.token, role: this.role })
    const response = await streamClient._request({
      type: 'subscribe', ...(epoch !== undefined ? { epoch } : {}), ...(cursor !== undefined ? { cursor } : {}),
      ...(runId ? { runId } : {}), ...(activityId ? { activityId } : {}), ...(attemptId ? { attemptId } : {})
    })
    if (!['subscriptionReady', 'subscribed', 'resyncRequired'].includes(response.type)) {
      streamClient.close()
      throw new Error(`unexpected coordinator subscription response ${response.type}`)
    }
    const subscription = new SupervisorSubscription(streamClient, response)
    streamClient.coordinatorSubscription = subscription
    for (const message of streamClient.pendingCoordinatorMessages.splice(0)) subscription._event(message)
    return subscription
  }

  #requireControl(action) {
    if (this.role !== 'control') throw new Error(`control credential required to ${action}`)
  }

  startManaged(request, policy) {
    if (this.role !== 'control') throw new Error('control credential required to start processes')
    if (!policy) throw new Error('an explicit Supervisor policy is required')
    if ((request.attemptId === undefined) !== (request.generation === undefined)) throw new Error('attemptId and generation must be provided together')
    const handle = new SupervisorProcessHandle(this)
    this.#startRequest({
      executable: request.executable,
      args: request.args,
      cwd: typeof request.options?.cwd === 'string' ? request.options.cwd : undefined,
      metadata: providerMetadata(request),
      attemptId: request.attemptId,
      generation: request.generation,
      policy
    }, handle).then((started) => { handle._setOutputToken(started.outputToken) }).catch((error) => handle._fail(error))
    return handle
  }

  #startRequest({ executable, args = [], cwd, metadata, policy, attemptId, generation, stopProcessGroupOnExit }, handle) {
    if ((attemptId === undefined) !== (generation === undefined)) return Promise.reject(new Error('attemptId and generation must be provided together'))
    const requestId = this.#requestId()
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject, kind: 'start', handle })
      this.#send({
        type: 'start', requestId,
        command: { executable, args, ...(cwd ? { cwd } : {}) },
        metadata: metadata ?? {}, policy,
        ...(attemptId !== undefined ? { attemptId, generation } : {}),
        ...(stopProcessGroupOnExit ? { stopProcessGroupOnExit: true } : {})
      })
    })
  }

  async attach(processId) {
    if (!['control', 'tui', 'output'].includes(this.role)) throw new Error('control, TUI, or process output credential required to attach')
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
    if (!['view', 'control', 'tui'].includes(this.role)) throw new Error('view, control, or TUI credential required to read metadata snapshots')
    const requestId = this.#requestId()
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject, kind: 'snapshot' })
      this.#send({ type: 'snapshot', requestId })
    })
  }

  _releaseCoordinatorSubscription(subscription) {
    if (this.coordinatorSubscription === subscription) this.coordinatorSubscription = null
  }

  revokeRole(role) {
    if (this.role !== 'control') throw new Error('control credential required to revoke view or TUI access')
    if (role !== 'view' && role !== 'tui') throw new Error('only view and TUI roles can be revoked')
    return this._request({ type: 'revokeRole', role })
  }

  close() { this.socket.end() }

  #requestId() { return `node-${randomUUID()}` }
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
      if (pending && ['started', 'attached', 'stopped', 'snapshot', 'roleRevoked', 'ack', 'error', 'runRegistered', 'activityRegistered', 'attemptRegistered', 'reported', 'attemptState', 'cancelRequested', 'shutdownComplete', 'activityStatus', 'runStatus', 'activityTimeoutSet', 'subscriptionReady', 'subscribed', 'resyncRequired', 'flowRunRegistered', 'flowInvocationsUpdated', 'flowRunClaimed', 'flowStageAdvanced', 'flowRunCompleted', 'flowAttemptBound', 'flowAttemptReconciled', 'flowReviewApplied', 'flowTaskCancellationRequested', 'approvalRequested', 'pendingApprovals', 'approvalWait', 'approvalResolved', 'pendingToolRequests', 'toolRequestCreated', 'toolRequestClaimed', 'toolRequestWait', 'toolRequestResolved', 'toolRequestFinished'].includes(message.type)) {
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
      if (message.type === 'coordinatorEvent' || message.type === 'resyncRequired') {
        if (this.coordinatorSubscription) this.coordinatorSubscription._event(message)
        else this.pendingCoordinatorMessages.push(message)
      }
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
    this.coordinatorSubscription?._disconnect(error)
    this.emit('disconnect', error)
  }
}

/** One filtered coordinator event stream, with its initial replay or resync snapshot. */
export class SupervisorSubscription extends EventEmitter {
  constructor(client, start) {
    super()
    this.client = client
    this.start = {
      epoch: start.epoch,
      cursor: start.cursor,
      resyncRequired: start.type === 'resyncRequired' || start.resyncRequired === true,
      events: start.events ?? [],
      snapshot: start.snapshot
    }
    this.closed = false
  }
  _event(message) {
    if (message.type === 'coordinatorEvent') this.emit('event', message.event ?? message)
    else if (message.type === 'resyncRequired') this.emit('resyncRequired', message)
  }
  _disconnect(error) { this.emit('disconnect', error) }
  close() {
    if (this.closed) return
    this.closed = true
    this.client._releaseCoordinatorSubscription(this)
    this.client.close()
  }
}

export class SupervisorProcessHandle extends EventEmitter {
  #outputToken = null

  constructor(client, processId = null) {
    super()
    this.client = client
    this.processId = processId
    Object.defineProperty(this, 'outputToken', { get: () => this.#outputToken, enumerable: false })
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
  _setOutputToken(token) { this.#outputToken = token ?? null }
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

export { AgentRun, AgentRunPolicy, CancellationToken, Command, Policy, Supervisor, SupervisorEventStream, SupervisorProcessId, SupervisedProcess, platformCapabilities, run } from './embedded.mjs'
