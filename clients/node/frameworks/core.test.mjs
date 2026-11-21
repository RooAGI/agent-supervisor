import test from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { createFrameworkAdapter } from './core.mjs'

function fixture() {
  const calls = []
  const client = Object.assign(new EventEmitter(), {
    socketPath: '/unused',
    async registerRun(context) { calls.push(['run', context]); return { runId: 'run-1' } },
    async registerActivity(value) { calls.push(['activity', value]); return { activityId: 'activity-1' } },
    async registerAttempt(value) {
      calls.push(['attempt', value])
      return { attemptId: 'attempt-1', generation: 2, reporterToken: 'report-token', cancelToken: 'cancel-token' }
    },
    async setActivityStatus(value) { calls.push(['activity-status', value]) },
    async setRunStatus(value) { calls.push(['run-status', value]) },
    async start(value) {
      calls.push(['start-process', value])
      return {
        processId: 'process-1',
        async stop(graceMs) { calls.push(['stop-process', 'process-1', graceMs]) },
        async wait() { calls.push(['wait-process', 'process-1']); return { status: 'exited' } }
      }
    }
  })
  const connect = async ({ token, role }) => ({
    async report(value) { calls.push(['report', token, value]) },
    async pollAttempt(value) { calls.push(['poll', token, value]); return { attempt: { cancellation: 'none' } } },
    async cancelAttempt(value) { calls.push(['cancel', token, value]); return { cancellation: 'requested' } },
    close() { calls.push(['close', role]) }
  })
  return { calls, client, connect }
}

test('tracks one-shot success while preserving result and invocation context', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk', context: { flowRunId: 'flow-1' } })
  const expected = { output: 42 }
  const result = await adapter.invoke({ execute: async (context) => {
    assert.deepEqual({ ...context, signal: undefined, startProcess: undefined }, { runId: 'run-1', activityId: 'activity-1', attemptId: 'attempt-1', generation: 2, signal: undefined, startProcess: undefined })
    return expected
  } })
  assert.equal(result, expected)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'run-status').map(([, value]) => value.status), ['running', 'succeeded'])
  assert.deepEqual(calls.find(([kind]) => kind === 'run')[1], { flowRunId: 'flow-1', framework: 'fixture-sdk' })
})

test('keeps stream lifecycle running until iteration ends and preserves result properties', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const stream = {
    label: 'native-stream',
    async *[Symbol.asyncIterator]() { yield 'a'; yield 'b' }
  }
  const result = await adapter.invoke({ execute: () => stream })
  assert.equal(result.label, 'native-stream')
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running'])
  assert.deepEqual(await Array.fromAsync(result), ['a', 'b'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})

test('tracks an async iterable field on a native stream result object', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const nativeResult = { text: 'preview', textStream: (async function* () { yield 'a'; yield 'b' })() }
  const result = await adapter.invoke({ execute: () => nativeResult, streamResult: (value) => value.textStream })
  assert.equal(result.text, 'preview')
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running'])
  assert.deepEqual(await Array.fromAsync(result.textStream), ['a', 'b'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})

test('native result methods retain their original receiver through stream tracking', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  class NativeResult {
    #secret = 'native receiver'
    textStream = (async function* () { yield 'chunk' })()
    readSecret() { return this.#secret }
  }
  const result = await adapter.invoke({ execute: () => new NativeResult(), streamResult: (value) => value.textStream })
  assert.equal(result.readSecret(), 'native receiver')
  assert.deepEqual(await Array.fromAsync(result.textStream), ['chunk'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})

test('tracks any consumed stream selected from a native multi-stream result', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const resultValue = {
    textStream: (async function* () { yield 'text' })(),
    fullStream: (async function* () { yield { type: 'text', text: 'text' } })()
  }
  const result = await adapter.invoke({ execute: () => resultValue, streamResult: (value) => [value.textStream, value.fullStream] })
  assert.deepEqual(await Array.fromAsync(result.fullStream), [{ type: 'text', text: 'text' }])
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
  assert.deepEqual(await Array.fromAsync(result.textStream), ['text'])
})

test('reports failure and rethrows the original framework error', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const error = new Error('provider fixture failed')
  await assert.rejects(adapter.invoke({ execute: () => { throw error } }), (received) => received === error)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'failed'])
})

test('startProcess requires policy and binds the stable process to trusted attempt context', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({
    client,
    connect,
    framework: 'fixture-sdk',
    context: { projectId: 'project-1', flowRunId: 'flow-1', taskId: 'task-1' }
  })
  let launched
  const processId = await adapter.invoke({ execute: async ({ startProcess }) => {
    await assert.rejects(startProcess({ executable: 'fixture-tool' }), /explicit Supervisor policy/)
    launched = await startProcess({ executable: 'fixture-tool', args: ['--safe'], cwd: '/tmp', policy: { outputLimit: 512 } })
    return launched.processId
  } })
  assert.equal(processId, 'process-1')
  const launch = calls.find(([kind]) => kind === 'start-process')[1]
  assert.deepEqual(launch, {
    executable: 'fixture-tool',
    args: ['--safe'],
    cwd: '/tmp',
    policy: { outputLimit: 512 },
    attemptId: 'attempt-1',
    generation: 2,
    metadata: { projectId: 'project-1', flowRunId: 'flow-1', taskId: 'task-1', framework: 'fixture-sdk', runId: 'run-1', activityId: 'activity-1' }
  })
  assert.equal(calls.some(([kind]) => kind === 'stop-process'), false, 'successful runs leave process lifetime to the caller/Supervisor')
})

test('failed invocation stops only its explicitly started process and waits for exit', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const failure = new Error('tool result failed')
  await assert.rejects(adapter.invoke({ execute: async ({ startProcess }) => {
    await startProcess({ executable: 'fixture-tool', policy: { outputLimit: 512 } })
    throw failure
  } }), (error) => error === failure)
  assert.ok(calls.some(([kind, processId, grace]) => kind === 'stop-process' && processId === 'process-1' && grace === 1500))
  assert.ok(calls.some(([kind, processId]) => kind === 'wait-process' && processId === 'process-1'))
})

test('requests scoped cancellation on abort and reports it after the operation settles', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const controller = new AbortController()
  let finish
  const pending = adapter.invoke({ signal: controller.signal, execute: () => new Promise((resolve) => { finish = resolve }) })
  await new Promise((resolve) => setImmediate(resolve))
  controller.abort(new Error('user cancelled'))
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(calls.some(([kind, _token, value]) => kind === 'report' && value.status === 'cancelled'), false)
  finish('done')
  assert.equal(await pending, 'done')
  assert.ok(calls.some(([kind, token]) => kind === 'cancel' && token === 'cancel-token'))
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'cancelled'])
})

test('delivers Supervisor-requested cancellation through the invocation signal', async () => {
  const { calls, client, connect: defaultConnect } = fixture()
  const connect = async ({ token, role }) => {
    const scoped = await defaultConnect({ token, role })
    if (role === 'reporter') scoped.pollAttempt = async () => ({ attempt: { cancellation: 'requested' } })
    return scoped
  }
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const sawCancellation = await adapter.invoke({ execute: ({ signal }) => new Promise((resolve) => {
    if (signal.aborted) resolve(true)
    else signal.addEventListener('abort', () => resolve(true), { once: true })
  }) })
  assert.equal(sawCancellation, true)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'cancelled'])
  assert.equal(calls.some(([kind]) => kind === 'cancel'), false)
})

test('control client disconnect aborts work and reports unknown rather than success', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const pending = adapter.invoke({ execute: ({ signal }) => new Promise((resolve) => {
    signal.addEventListener('abort', () => resolve('stopped cooperatively'), { once: true })
  }) })
  await new Promise((resolve) => setImmediate(resolve))
  client.emit('disconnect', new Error('socket closed'))
  assert.equal(await pending, 'stopped cooperatively')
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'unknown'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'activity-status').map(([, value]) => value.status), ['running', 'unknown'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'run-status').map(([, value]) => value.status), ['running', 'unknown'])
})

test('stream errors fail the invocation and preserve the stream error', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const error = new Error('stream failed')
  const result = await adapter.invoke({ execute: async function* () { yield 'partial'; throw error } })
  const received = []
  await assert.rejects(async () => { for await (const item of result) received.push(item) }, (value) => value === error)
  assert.deepEqual(received, ['partial'])
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'failed'])
})

test('early stream close marks the invocation cancelled', async () => {
  const { calls, client, connect } = fixture()
  const adapter = createFrameworkAdapter({ client, connect, framework: 'fixture-sdk' })
  const result = await adapter.invoke({ execute: async function* () { yield 'first'; yield 'second' } })
  for await (const _item of result) break
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'cancelled'])
  assert.ok(calls.some(([kind]) => kind === 'cancel'))
})
