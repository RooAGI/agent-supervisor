import test from 'node:test'
import assert from 'node:assert/strict'
import { createOpenAIAgentsAdapter } from './openai.mjs'

function fixture() {
  const calls = []
  const client = {
    socketPath: '/unused',
    role: 'control',
    async registerRun(context) { calls.push(['run', context]); return { runId: 'run-1' } },
    async registerActivity(value) { calls.push(['activity', value]); return { activityId: 'activity-1' } },
    async registerAttempt(value) {
      calls.push(['attempt', value])
      return { attemptId: 'attempt-1', generation: 3, reporterToken: 'report-token', cancelToken: 'cancel-token' }
    },
    async setActivityStatus(value) { calls.push(['activity-status', value]) },
    async setRunStatus(value) { calls.push(['run-status', value]) },
    async start(value) {
      calls.push(['start', value])
      return { processId: 'process-1', async stop(graceMs) { calls.push(['stop', graceMs]) }, async wait() { calls.push(['wait']) } }
    },
  }
  const connect = async ({ token, role }) => ({
    async report(value) { calls.push(['report', token, value]) },
    async pollAttempt(value) { calls.push(['poll', token, value]); return { attempt: { cancellation: 'none' } } },
    async cancelAttempt(value) { calls.push(['cancel', token, value]); return { cancellation: 'requested' } },
    close() { calls.push(['close', role]) },
  })
  return { calls, client, connect }
}

test('OpenAI Agents one-shot run preserves options, handoff result and error identity', async () => {
  const { calls, client, connect } = fixture()
  const controller = new AbortController()
  const session = { id: 'session-1' }
  const context = { requestId: 'request-1' }
  const result = { finalOutput: 'done', lastAgent: { name: 'specialist' }, newItems: [{ type: 'handoff' }] }
  let received
  const adapter = createOpenAIAgentsAdapter({
    client, connect, context: { projectId: 'project-1' },
    run: async (...args) => { received = args; return result },
  })
  const wrapped = adapter.wrap({ name: 'triage' })
  assert.equal(await wrapped.run('help', { signal: controller.signal, session, context, maxTurns: 4 }), result)
  assert.equal(received[0].name, 'triage')
  assert.equal(received[1], 'help')
  assert.equal(received[2].session, session)
  assert.equal(received[2].context, context)
  assert.equal(received[2].maxTurns, 4)
  assert.notEqual(received[2].signal, controller.signal)
  assert.equal(received[2].signal.aborted, false)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
  assert.equal(calls.find(([kind]) => kind === 'activity')[1].kind, 'openai-agents.run')
  assert.deepEqual(calls.find(([kind]) => kind === 'run')[1], { projectId: 'project-1', framework: 'openai-agents' })
})

test('OpenAI streaming waits for the SDK completed promise before succeeding', async () => {
  const { calls, client, connect } = fixture()
  let finishCompletion
  const completed = new Promise((resolve) => { finishCompletion = resolve })
  const nativeStream = {
    state: { turn: 1 },
    finalOutput: undefined,
    completed,
    async *[Symbol.asyncIterator]() { yield { type: 'run_item_stream_event', item: { type: 'handoff' } } },
  }
  const adapter = createOpenAIAgentsAdapter({ client, connect, run: async () => nativeStream })
  const stream = await adapter.wrap({ name: 'planner' }).runStream('task', { session: { id: 's' } })
  const iteration = Array.fromAsync(stream)
  await new Promise((resolve) => setImmediate(resolve))
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running'])
  finishCompletion()
  assert.deepEqual(await iteration, [{ type: 'run_item_stream_event', item: { type: 'handoff' } }])
  assert.equal(stream.state.turn, 1)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})

test('OpenAI stream completion failure remains the original stream error', async () => {
  const { calls, client, connect } = fixture()
  const error = new Error('completion callback failed')
  const adapter = createOpenAIAgentsAdapter({
    client, connect,
    run: async () => ({ completed: Promise.reject(error), async *[Symbol.asyncIterator]() { yield 'partial' } }),
  })
  const stream = await adapter.wrap({ name: 'assistant' }).runStream('task')
  await assert.rejects(Array.fromAsync(stream), (value) => value === error)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'failed'])
})

test('OpenAI cancellation reaches the SDK signal and is reported after the run settles', async () => {
  const { calls, client, connect } = fixture()
  const controller = new AbortController()
  const error = new Error('OpenAI run aborted')
  let signalReady
  const started = new Promise((resolve) => { signalReady = resolve })
  const adapter = createOpenAIAgentsAdapter({
    client, connect,
    run: (_agent, _input, options) => new Promise((resolve, reject) => {
      signalReady(options.signal)
      options.signal.addEventListener('abort', () => reject(error), { once: true })
    }),
  })
  const pending = adapter.wrap({ name: 'assistant' }).run('task', { signal: controller.signal })
  const sdkSignal = await started
  controller.abort(new Error('caller stopped'))
  assert.equal(sdkSignal.aborted, true)
  await assert.rejects(pending, (value) => value === error)
  assert.ok(calls.some(([kind]) => kind === 'cancel'))
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'cancelled'])
})

test('OpenAI per-call execute can launch a scoped managed subprocess and preserves its result', async () => {
  const { calls, client, connect } = fixture()
  const result = { finalOutput: 'process finished', custom: true }
  let invocationContext
  const adapter = createOpenAIAgentsAdapter({
    client, connect,
    run: async () => { throw new Error('default SDK execution must be bypassed') },
  })
  const wrapped = adapter.wrap({ name: 'assistant' })
  const received = await wrapped.run('task', {}, {
    execute: async (context) => {
      invocationContext = context
      await context.startProcess({ executable: '/bin/echo', args: ['hello'], policy: { timeoutMs: 1000 } })
      return result
    },
  })
  assert.equal(received, result)
  const process = calls.find(([kind]) => kind === 'start')[1]
  assert.equal(process.attemptId, invocationContext.attemptId)
  assert.equal(process.generation, invocationContext.generation)
  assert.equal(process.metadata.runId, invocationContext.runId)
  assert.equal(process.metadata.activityId, invocationContext.activityId)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})
