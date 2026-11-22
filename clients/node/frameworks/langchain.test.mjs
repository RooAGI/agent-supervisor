import test from 'node:test'
import assert from 'node:assert/strict'
import { createLangChainAdapter } from './langchain.mjs'

function fixture() {
  const calls = []
  const client = {
    socketPath: '/unused',
    role: 'control',
    async registerRun(context) { calls.push(['run', context]); return { runId: 'run-1' } },
    async registerActivity(value) { calls.push(['activity', value]); return { activityId: 'activity-1' } },
    async registerAttempt(value) {
      calls.push(['attempt', value])
      return { attemptId: 'attempt-1', generation: 2, reporterToken: 'report-token', cancelToken: 'cancel-token' }
    },
    async setActivityStatus(value) { calls.push(['activity-status', value]) },
    async setRunStatus(value) { calls.push(['run-status', value]) },
    async start(value) {
      calls.push(['start', value])
      return { processId: 'process-2', async stop(graceMs) { calls.push(['stop', graceMs]) }, async wait() { calls.push(['wait']) } }
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

test('LangChain invoke preserves result and runnable config including callbacks and checkpoint identity', async () => {
  const { calls, client, connect } = fixture()
  const controller = new AbortController()
  const callbacks = [{ handleToolEnd() {} }]
  const config = { signal: controller.signal, callbacks, metadata: { source: 'unit' }, configurable: { thread_id: 'thread-9', checkpoint_id: 'checkpoint-2' } }
  const result = { messages: [{ role: 'assistant', content: 'done' }], structuredResponse: { accepted: true } }
  let received
  const agent = { async invoke(...args) { assert.equal(this, agent); received = args; return result } }
  const wrapped = createLangChainAdapter({ client, connect, context: { taskId: 'task-1' } }).wrap(agent)
  assert.equal(await wrapped.invoke({ messages: [{ role: 'user', content: 'go' }] }, config), result)
  assert.notEqual(received[1], config)
  assert.notEqual(received[1].signal, controller.signal)
  assert.equal(received[1].callbacks, callbacks)
  assert.equal(received[1].configurable.thread_id, 'thread-9')
  assert.equal(calls.find(([kind]) => kind === 'activity')[1].kind, 'langchain.invoke')
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})

test('LangChain stream preserves chunks and adds adapter signal without mutating caller config', async () => {
  const { calls, client, connect } = fixture()
  const controller = new AbortController()
  const callbacks = [{ handleLLMNewToken() {} }]
  const config = { callbacks, metadata: { trace: true }, configurable: { thread_id: 'thread-10' } }
  let received
  const agent = { async *stream(...args) { assert.equal(this, agent); received = args; yield { event: 'on_chain_stream', data: { chunk: 'a' } }; yield { event: 'on_tool_end', data: { output: 'b' } } } }
  const wrapped = createLangChainAdapter({ client, connect }).wrap(agent)
  const events = await wrapped.stream({ messages: [] }, config, { signal: controller.signal })
  assert.deepEqual(await Array.fromAsync(events), [
    { event: 'on_chain_stream', data: { chunk: 'a' } },
    { event: 'on_tool_end', data: { output: 'b' } },
  ])
  assert.notEqual(received[1].signal, controller.signal)
  assert.equal(received[1].signal.aborted, false)
  assert.notEqual(received[1], config)
  assert.equal(received[1].callbacks, callbacks)
  assert.equal(received[1].configurable, config.configurable)
  assert.equal(config.signal, undefined)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'succeeded'])
})

test('LangChain wrapper reports original framework errors and rejects missing invoke method', async () => {
  const { calls, client, connect } = fixture()
  const error = new Error('fixture tool failed')
  const wrapped = createLangChainAdapter({ client, connect }).wrap({ async invoke() { throw error } })
  await assert.rejects(wrapped.invoke({ messages: [] }), (value) => value === error)
  assert.deepEqual(calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status), ['running', 'failed'])
  assert.throws(() => createLangChainAdapter({ client, connect }).wrap({}).invoke({}), /must implement invoke\(\)/)
})

test('LangChain forwards cooperative cancellation and keeps sibling invocations independent', async () => {
  const { calls, client, connect } = fixture()
  const controller = new AbortController()
  const error = new Error('LangChain invocation aborted')
  let notifyStarted
  const started = new Promise((resolve) => { notifyStarted = resolve })
  const agent = {
    invoke(input, config) {
      if (input === 'cancel') {
        return new Promise((resolve, reject) => {
          notifyStarted(config.signal)
          config.signal.addEventListener('abort', () => reject(error), { once: true })
        })
      }
      return Promise.resolve({ input })
    },
  }
  const wrapped = createLangChainAdapter({ client, connect }).wrap(agent)
  const cancelled = wrapped.invoke('cancel', {}, { signal: controller.signal })
  const sibling = wrapped.invoke('sibling')
  assert.deepEqual(await sibling, { input: 'sibling' })
  const invocationSignal = await started
  controller.abort(new Error('caller stopped'))
  assert.equal(invocationSignal.aborted, true)
  await assert.rejects(cancelled, (value) => value === error)
  assert.ok(calls.some(([kind]) => kind === 'cancel'))
  const statuses = calls.filter(([kind]) => kind === 'report').map(([, , value]) => value.status)
  assert.equal(statuses.filter((status) => status === 'succeeded').length, 1)
  assert.equal(statuses.filter((status) => status === 'cancelled').length, 1)
  assert.equal(calls.filter(([kind]) => kind === 'attempt').length, 2)
})

test('LangChain per-call execute can start a scoped process and return a native fixture result', async () => {
  const { calls, client, connect } = fixture()
  const result = { messages: [{ role: 'assistant', content: 'custom run' }] }
  let invocationContext
  const agent = { async invoke() { throw new Error('default runnable must be bypassed') } }
  const wrapped = createLangChainAdapter({ client, connect }).wrap(agent)
  const received = await wrapped.invoke({ messages: [] }, {}, {
    execute: async (context) => {
      invocationContext = context
      await context.startProcess({ executable: '/bin/echo', args: ['fixture'], policy: { timeoutMs: 1000 } })
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
