import test from 'node:test'
import assert from 'node:assert/strict'
import { createLangGraphAdapter } from './langgraph.mjs'
import { createVercelAIAdapter } from './vercel-ai.mjs'
import { createGoogleADKAdapter } from './google-adk.mjs'

function fixture() {
  const events = []
  let next = 0
  const client = {
    async registerRun(context) { events.push(['registerRun', context]); return { runId: `run-${++next}` } },
    async registerActivity(options) { events.push(['registerActivity', options]); return { activityId: `activity-${next}` } },
    async registerAttempt(options) {
      events.push(['registerAttempt', options])
      return { attemptId: `attempt-${next}`, generation: 1, reporterToken: 'report', cancelToken: 'cancel' }
    },
    async setRunStatus(options) { events.push(['run', options]) },
    async setActivityStatus(options) { events.push(['activity', options]) },
    async start(options) {
      events.push(['startProcess', options])
      return { processId: `process-${next}`, async stop(graceMs) { events.push(['stopProcess', graceMs]) }, async wait() { events.push(['waitProcess']) } }
    },
  }
  const connect = async ({ role }) => role === 'reporter'
    ? {
      async report(options) { events.push(['report', options]) },
      async pollAttempt(options) { events.push(['poll', options]); return { attempt: { cancellation: 'none' } } },
      close() {},
    }
    : { async cancelAttempt(options) { events.push(['cancel', options]) }, close() {} }
  return { events, client, connect }
}

async function waitUntil(predicate, message) {
  for (let index = 0; index < 100; index += 1) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  assert.fail(message)
}

test('LangGraph adapter passes through graph config and output', async () => {
  const f = fixture()
  const threadConfig = { configurable: { thread_id: 'thread-7', checkpoint_id: 'checkpoint-2' }, tags: ['kept'] }
  const result = { state: { answer: 42 } }
  const graph = { async invoke(input, config) {
    assert.equal(input, 'question')
    assert.deepEqual(config.configurable, threadConfig.configurable)
    assert.deepEqual(config.tags, threadConfig.tags)
    assert.ok(config.signal instanceof AbortSignal)
    return result
  } }
  const wrapped = createLangGraphAdapter({ client: f.client, connect: f.connect }).wrap(graph)

  assert.equal(await wrapped.invoke('question', threadConfig), result)
  assert.equal(f.events.find(([kind]) => kind === 'registerRun')[1].framework, 'langgraph')
  assert.ok(f.events.some(([kind, value]) => kind === 'registerActivity' && value.kind === 'langgraph.invoke'))
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'))
})

test('LangGraph stream lifecycle completes after stream consumption and forwards signal', async () => {
  const f = fixture()
  const controller = new AbortController()
  const config = { configurable: { thread_id: 'thread-stream' } }
  const graph = {
    async stream(_input, received) {
      assert.equal(received.configurable.thread_id, 'thread-stream')
      assert.ok(received.signal instanceof AbortSignal)
      return (async function* () { yield { updates: { step: 1 } }; yield { updates: { step: 2 } } })()
    },
  }
  const wrapped = createLangGraphAdapter({ client: f.client, connect: f.connect }).wrap(graph)
  const stream = await wrapped.stream({}, config, { signal: controller.signal })
  assert.equal(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), false)
  const values = []
  for await (const item of stream) values.push(item)
  assert.deepEqual(values, [{ updates: { step: 1 } }, { updates: { step: 2 } }])
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'))
})

test('Vercel AI generate preserves result and receives the cooperative abort signal', async () => {
  const f = fixture()
  const controller = new AbortController()
  const result = { text: 'answer', steps: [{ toolCalls: [] }] }
  const call = { prompt: 'hello', experimental_context: { session: 'owned-by-caller' } }
  const agent = { async generate(options) {
    assert.equal(options.prompt, 'hello')
    assert.ok(options.abortSignal instanceof AbortSignal)
    return result
  } }
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap(agent)

  assert.equal(await wrapped.generate(call, { signal: controller.signal }), result)
  assert.deepEqual(call, { prompt: 'hello', experimental_context: { session: 'owned-by-caller' } })
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'))
})

test('Vercel AI stream keeps result shape and reports completion after stream consumption', async () => {
  const f = fixture()
  const chunks = (async function* () { yield { type: 'text-delta', text: 'hello' }; yield { type: 'finish' } })()
  const result = { id: 'stream-1', textStream: chunks, fullStream: chunks }
  const agent = { stream() { return result } }
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap(agent)
  const tracked = await wrapped.stream({ prompt: 'hello' })

  assert.equal(tracked.id, 'stream-1')
  assert.equal(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), false)
  const observed = []
  for await (const chunk of tracked.fullStream) observed.push(chunk)
  assert.deepEqual(observed, [{ type: 'text-delta', text: 'hello' }, { type: 'finish' }])
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'))
})

test('Vercel AI textStream is tracked when callers choose it over fullStream', async () => {
  const f = fixture()
  const textStream = (async function* () { yield 'text delta' })()
  const fullStream = (async function* () { yield { type: 'text-delta', text: 'text delta' } })()
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap({ stream: () => ({ textStream, fullStream }) })
  const tracked = await wrapped.stream({ prompt: 'hello' })
  const chunks = []
  for await (const chunk of tracked.textStream) chunks.push(chunk)
  assert.deepEqual(chunks, ['text delta'])
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'))
})

test('Vercel AI terminal result promise completes lifecycle when streams are not consumed', async () => {
  const f = fixture()
  let resolveText
  const text = new Promise((resolve) => { resolveText = resolve })
  const result = {
    text,
    textStream: (async function* () { yield 'unused text' })(),
    fullStream: (async function* () { yield { type: 'text-delta', text: 'unused text' } })(),
  }
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap({ stream: () => result })
  const tracked = await wrapped.stream({ prompt: 'hello' })
  assert.equal(tracked.id, undefined)
  assert.equal(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), false)

  resolveText('complete')
  await waitUntil(() => f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), 'terminal text promise should complete the attempt')
})

test('Vercel AI structured output promise is a terminal fallback when text is not promise-like', async () => {
  const f = fixture()
  let resolveOutput
  const output = new Promise((resolve) => { resolveOutput = resolve })
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap({
    stream: () => ({ text: 'structured result', output, textStream: (async function* () {})(), fullStream: (async function* () {})() }),
  })
  await wrapped.stream({ prompt: 'hello' })
  resolveOutput({ ok: true })
  await waitUntil(() => f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), 'terminal output promise should complete the attempt')
})

test('Vercel AI waits for all active streams after the terminal promise resolves', async () => {
  const f = fixture()
  let resolveText
  let releaseTextStream
  let releaseFullStream
  const text = new Promise((resolve) => { resolveText = resolve })
  const textGate = new Promise((resolve) => { releaseTextStream = resolve })
  const fullGate = new Promise((resolve) => { releaseFullStream = resolve })
  const result = {
    text,
    textStream: (async function* () { yield 'text'; await textGate })(),
    fullStream: (async function* () { yield { type: 'text-delta', text: 'text' }; await fullGate })(),
  }
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap({ stream: () => result })
  const tracked = await wrapped.stream({ prompt: 'hello' })
  const textConsumer = (async () => { for await (const _chunk of tracked.textStream) {} })()
  const fullConsumer = (async () => { for await (const _chunk of tracked.fullStream) {} })()
  await new Promise(setImmediate)

  resolveText('complete')
  releaseTextStream()
  await new Promise(setImmediate)
  assert.equal(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), false)

  releaseFullStream()
  await textConsumer
  await fullConsumer
  await waitUntil(() => f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'), 'both active streams should settle the attempt')
})

test('Vercel AI stream failure preserves the original error identity', async () => {
  const f = fixture()
  const failure = new Error('provider stream failed')
  const brokenStream = (async function* () { throw failure })()
  const wrapped = createVercelAIAdapter({ client: f.client, connect: f.connect }).wrap({
    stream: () => ({ text: new Promise(() => {}), textStream: brokenStream, fullStream: brokenStream }),
  })
  const tracked = await wrapped.stream({ prompt: 'hello' })
  let observed
  try {
    for await (const _chunk of tracked.fullStream) {}
  } catch (error) {
    observed = error
  }
  assert.equal(observed, failure)
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'failed' && value.detail?.reason === failure.message))
})

test('Google ADK adapter preserves request/session data, native signal, and event stream', async () => {
  const f = fixture()
  const controller = new AbortController()
  const request = { userId: 'u-1', sessionId: 's-2', newMessage: { parts: [{ text: 'hi' }] }, resume: { invocationId: 'i-3' } }
  const runner = {
    runAsync(actualRequest, options) {
      assert.equal(actualRequest, request)
      assert.ok(options.signal instanceof AbortSignal)
      return (async function* () { yield { author: 'agent', text: 'hello' } })()
    },
  }
  const wrapped = createGoogleADKAdapter({ client: f.client, connect: f.connect }).wrap(runner)
  const events = []
  for await (const event of wrapped.runAsync(request, { signal: controller.signal })) events.push(event)

  assert.deepEqual(events, [{ author: 'agent', text: 'hello' }])
  assert.ok(f.events.some(([kind, value]) => kind === 'registerActivity' && value.kind === 'google-adk.runAsync'))
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'succeeded'))
})

test('Google ADK cancellation is cooperative and reported after runAsync stops', async () => {
  const f = fixture()
  const controller = new AbortController()
  const runner = {
    runAsync(_request, options) {
      return (async function* () {
        yield { text: 'partial' }
        await new Promise((resolve) => {
          if (options.signal.aborted) resolve()
          else options.signal.addEventListener('abort', resolve, { once: true })
        })
      })()
    },
  }
  const wrapped = createGoogleADKAdapter({ client: f.client, connect: f.connect }).wrap(runner)
  const observed = []
  for await (const event of wrapped.runAsync({ userId: 'u', sessionId: 's', newMessage: {} }, { signal: controller.signal })) {
    observed.push(event)
    controller.abort('user stopped')
  }
  assert.deepEqual(observed, [{ text: 'partial' }])
  assert.ok(f.events.some(([kind]) => kind === 'cancel'))
  assert.ok(f.events.some(([kind, value]) => kind === 'report' && value.status === 'cancelled'))
})

test('per-invocation execute escape hatch can explicitly launch and clean up a supervised tool process', async () => {
  const f = fixture()
  let graphInvoked = false
  const wrapped = createLangGraphAdapter({ client: f.client, connect: f.connect }).wrap({
    async invoke() { graphInvoked = true; return 'unreachable' },
  })
  await assert.rejects(wrapped.invoke('input', {}, {
    async execute({ startProcess, attemptId, generation }) {
      assert.equal(typeof startProcess, 'function')
      const process = await startProcess({ executable: 'fixture-worker', args: ['--once'], policy: { environment: { variables: {} } } })
      assert.equal(process.processId, 'process-1')
      assert.equal(typeof attemptId, 'string')
      assert.equal(generation, 1)
      throw new Error('tool failed')
    },
  }), /tool failed/)

  assert.equal(graphInvoked, false, 'explicit execute callback controls this invocation')
  const launch = f.events.find(([kind]) => kind === 'startProcess')?.[1]
  assert.equal(launch.executable, 'fixture-worker')
  assert.equal(launch.metadata.framework, 'langgraph')
  assert.equal(typeof launch.attemptId, 'string')
  assert.equal(launch.generation, 1)
  assert.ok(f.events.some(([kind]) => kind === 'stopProcess'))
  assert.ok(f.events.some(([kind]) => kind === 'waitProcess'))
})
