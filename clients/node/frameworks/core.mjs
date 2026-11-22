import { SupervisorClient } from '../index.mjs'

/**
 * Create the framework-independent lifecycle helper used by Node adapters.
 * This module deliberately does not import any agent framework.
 */
export function createFrameworkAdapter({ client, framework, context = {}, connect = (options) => SupervisorClient.connect({ socketPath: client?.socketPath, ...options }), cleanupGraceMs = 1500, cleanupWaitMs = 5000 }) {
  if (!client || typeof client.registerRun !== 'function' || typeof client.registerActivity !== 'function' || typeof client.registerAttempt !== 'function' || typeof client.setRunStatus !== 'function' || typeof client.start !== 'function') {
    throw new TypeError('client must be a control SupervisorClient')
  }
  if (typeof framework !== 'string' || framework.length === 0) throw new TypeError('framework is required')
  if (typeof connect !== 'function') throw new TypeError('connect must open scoped Supervisor clients')

  return {
    async invoke({ activityKind = framework, signal, execute, streamResult, streamCompletion, onLifecycleError } = {}) {
      if (typeof execute !== 'function') throw new TypeError('execute must be a function')
      if (signal !== undefined && (typeof signal.addEventListener !== 'function' || typeof signal.aborted !== 'boolean')) {
        throw new TypeError('signal must be an AbortSignal')
      }

      const run = await client.registerRun({ ...context, framework })
      const activity = await client.registerActivity({ runId: run.runId, kind: activityKind })
      const registered = await client.registerAttempt({ activityId: activity.activityId })
      const identity = {
        runId: run.runId,
        activityId: activity.activityId,
        attemptId: registered.attemptId,
        generation: registered.generation
      }
      const reporter = await connect({ token: registered.reporterToken, role: 'reporter' })
      if (typeof reporter?.report !== 'function' || typeof reporter?.pollAttempt !== 'function') {
        reporter?.close?.()
        throw new TypeError('connect must return a reporter client with report and pollAttempt for role reporter')
      }
      let canceller
      try {
        canceller = await connect({ token: registered.cancelToken, role: 'canceller' })
        if (typeof canceller?.cancelAttempt !== 'function') throw new TypeError('connect must return a canceller client for role canceller')
      } catch (error) {
        reporter.close?.()
        throw error
      }

      const reportLifecycleError = (error) => {
        try { onLifecycleError?.(error) } catch { /* diagnostics must not replace framework results */ }
      }
      const reportAttempt = async (status, detail) => reporter.report({
        attemptId: identity.attemptId,
        generation: identity.generation,
        status,
        ...(detail === undefined ? {} : { detail })
      })
      const setActivity = async (status, reason) => client.setActivityStatus({
        activityId: identity.activityId,
        status,
        ...(reason === undefined ? {} : { reason })
      })
      const setRun = async (status) => client.setRunStatus({ runId: identity.runId, status })

      try {
        await setRun('running')
        await setActivity('running')
        await reportAttempt('running')
      } catch (error) {
        reporter.close?.()
        canceller.close?.()
        throw error
      }

      let cancelRequested = false
      let finalized = false
      let serviceCancellation = false
      let clientDisconnected = false
      let monitorStopped = false
      const managedProcesses = []
      let cancelRequest = Promise.resolve()
      const invocationController = new AbortController()
      const abortInvocation = (reason) => {
        if (!invocationController.signal.aborted) invocationController.abort(reason)
      }
      const requestCancellation = () => {
        if (cancelRequested) return cancelRequest
        cancelRequested = true
        cancelRequest = Promise.resolve().then(() => canceller.cancelAttempt({
          attemptId: identity.attemptId,
          generation: identity.generation
        })).catch(reportLifecycleError)
        return cancelRequest
      }
      const onAbort = () => {
        void requestCancellation()
        abortInvocation(signal?.reason)
      }
      const onDisconnect = (error) => {
        clientDisconnected = true
        abortInvocation(error ?? new Error('Supervisor service connection closed'))
      }
      const removeClientListener = () => {
        if (typeof client.off === 'function') client.off('disconnect', onDisconnect)
        else if (typeof client.removeListener === 'function') client.removeListener('disconnect', onDisconnect)
      }
      if (typeof client.on === 'function') client.on('disconnect', onDisconnect)
      signal?.addEventListener('abort', onAbort, { once: true })
      if (signal?.aborted) onAbort()

      const monitorCancellation = async () => {
        while (!monitorStopped && !finalized) {
          try {
            const response = await reporter.pollAttempt({ attemptId: identity.attemptId, generation: identity.generation })
            const cancellation = response?.attempt?.cancellation
            if (cancellation && cancellation !== 'none' && cancellation !== 'failed') {
              serviceCancellation = true
              abortInvocation(new Error('Supervisor requested cancellation'))
              return
            }
          } catch (error) {
            reportLifecycleError(error)
          }
          if (!monitorStopped && !finalized) await new Promise((resolve) => setTimeout(resolve, 200))
        }
      }
      void monitorCancellation()

      const startProcess = async ({ executable, args = [], cwd, policy } = {}) => {
        if (typeof executable !== 'string' || executable.length === 0) throw new TypeError('startProcess requires an executable')
        if (!policy || typeof policy !== 'object') throw new TypeError('startProcess requires an explicit Supervisor policy')
        if (!Array.isArray(args) || args.some((arg) => typeof arg !== 'string')) throw new TypeError('startProcess args must be an array of strings')
        if (cwd !== undefined && typeof cwd !== 'string') throw new TypeError('startProcess cwd must be a string')
        const handle = await client.start({
          executable,
          args,
          ...(cwd === undefined ? {} : { cwd }),
          policy,
          attemptId: identity.attemptId,
          generation: identity.generation,
          metadata: { ...context, framework, runId: identity.runId, activityId: identity.activityId }
        })
        if (!handle || typeof handle.stop !== 'function' || typeof handle.wait !== 'function' || typeof handle.processId !== 'string') {
          throw new TypeError('Supervisor start must return a process handle with a stable processId, stop, and wait')
        }
        managedProcesses.push(handle)
        return handle
      }

      const stopManagedProcesses = async () => {
        await Promise.all(managedProcesses.map(async (handle) => {
          try {
            await handle.stop(cleanupGraceMs)
          } catch (error) {
            reportLifecycleError(error)
            return
          }
          let timer
          try {
            await Promise.race([
              handle.wait(),
              new Promise((resolve) => { timer = setTimeout(resolve, cleanupWaitMs) })
            ])
          } catch (error) {
            reportLifecycleError(error)
          } finally {
            clearTimeout(timer)
          }
        }))
      }

      const finalize = async (status, reason) => {
        if (finalized) return
        finalized = true
        monitorStopped = true
        signal?.removeEventListener('abort', onAbort)
        removeClientListener()
        if (status === 'cancelled' && !serviceCancellation) await requestCancellation()
        if (clientDisconnected && !serviceCancellation) status = 'unknown'
        if (status !== 'succeeded') await stopManagedProcesses()
        const detail = reason === undefined ? undefined : { reason: errorMessage(reason) }
        try { await reportAttempt(status, detail) } catch (error) { reportLifecycleError(error) }
        try { await setActivity(status, detail?.reason) } catch (error) { reportLifecycleError(error) }
        try { await setRun(status) } catch (error) { reportLifecycleError(error) }
        reporter.close?.()
        canceller.close?.()
      }

      let result
      try {
        result = await execute({ ...identity, signal: invocationController.signal, startProcess })
      } catch (error) {
        const status = serviceCancellation || signal?.aborted ? 'cancelled' : 'failed'
        await finalize(status, error)
        throw error
      }

      let selected
      let completion
      try {
        selected = streamResult?.(result)
        completion = streamCompletion?.(result)
      } catch (error) {
        await finalize(signal?.aborted ? 'cancelled' : 'failed', error)
        throw error
      }
      const selectedStreams = selected === undefined ? [] : Array.isArray(selected) ? selected : [selected]
      if (selectedStreams.some((stream) => !isAsyncIterable(stream))) {
        await finalize('failed', new TypeError('streamResult must select an async iterable'))
        throw new TypeError('streamResult must select an async iterable')
      }
      if (completion !== undefined && !isPromiseLike(completion)) {
        await finalize('failed', new TypeError('streamCompletion must return a promise-like value'))
        throw new TypeError('streamCompletion must return a promise-like value')
      }

      const streamState = createStreamState()
      let completionOutcome
      if (completion !== undefined) {
        completionOutcome = Promise.resolve(completion).then(async () => {
          await waitForIdle(streamState)
          await finalize(serviceCancellation || invocationController.signal.aborted ? 'cancelled' : 'succeeded')
        }, async (error) => {
          await finalize(serviceCancellation || invocationController.signal.aborted ? 'cancelled' : 'failed', error)
          throw error
        })
        // The result promise remains owned by the SDK and is returned as-is.
        // Observe its rejection here so result-only consumers still close the
        // lifecycle without creating an unhandled rejection.
        completionOutcome.catch(() => {})
      }

      if (selectedStreams.length) return wrapResultStreams(result, selectedStreams, finalize, invocationController.signal, streamState, completionOutcome)
      if (isAsyncIterable(result)) return wrapStream(result, finalize, invocationController.signal, streamState, completionOutcome)
      if (completionOutcome) return result
      await finalize(serviceCancellation || signal?.aborted ? 'cancelled' : 'succeeded')
      return result
    }
  }
}

function isAsyncIterable(value) {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof value[Symbol.asyncIterator] === 'function'
}

function isPromiseLike(value) {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof value.then === 'function'
}

function createStreamState() {
  return { active: 0, waiters: [] }
}

function waitForIdle(state) {
  if (state.active === 0) return Promise.resolve()
  return new Promise((resolve) => state.waiters.push(resolve))
}

function enterStream(state) {
  state.active += 1
}

function leaveStream(state) {
  state.active = Math.max(0, state.active - 1)
  if (state.active === 0) {
    for (const resolve of state.waiters.splice(0)) resolve()
  }
}

function wrapStream(value, finalize, signal, state = createStreamState(), completionOutcome) {
  return new Proxy(value, {
    get(target, property, receiver) {
      if (property !== Symbol.asyncIterator) {
        const value = Reflect.get(target, property, target)
        return typeof value === 'function' ? value.bind(target) : value
      }
      const iteratorFactory = Reflect.get(target, property, target)
      return function (...args) {
        const iterator = Reflect.apply(iteratorFactory, target, args)
        let ended = false
        return (async function* () {
          let active = false
          try {
            enterStream(state)
            active = true
            while (true) {
              const item = await iterator.next()
              if (item.done) {
                ended = true
                leaveStream(state)
                active = false
                if (completionOutcome) await completionOutcome
                else {
                  await waitForIdle(state)
                  await finalize(signal?.aborted ? 'cancelled' : 'succeeded')
                }
                return item.value
              }
              yield item.value
            }
          } catch (error) {
            ended = true
            if (active) leaveStream(state)
            await finalize(signal?.aborted ? 'cancelled' : 'failed', error)
            throw error
          } finally {
            if (!ended) {
              if (active) leaveStream(state)
              try { await iterator.return?.() } catch (error) { await finalize('failed', error); throw error }
              await finalize('cancelled', signal?.reason)
            }
          }
        })()
      }
    }
  })
}

function wrapResultStreams(result, selectedStreams, finalize, signal, state, completionOutcome) {
  const wrappedStreams = new Map(selectedStreams.map((stream) => [stream, wrapStream(stream, finalize, signal, state, completionOutcome)]))
  return new Proxy(result, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, target)
      const wrapped = wrappedStreams.get(value)
      return wrapped ?? (typeof value === 'function' ? value.bind(target) : value)
    }
  })
}

function errorMessage(error) {
  if (error instanceof Error) return error.message.slice(0, 2048)
  return String(error).slice(0, 2048)
}
