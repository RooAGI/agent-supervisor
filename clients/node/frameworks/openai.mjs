import { createFrameworkAdapter } from './core.mjs'

let defaultRun

async function resolveRun() {
  if (!defaultRun) {
    defaultRun = import('@openai/agents').then((sdk) => sdk.run)
  }
  const run = await defaultRun
  if (typeof run !== 'function') throw new TypeError('@openai/agents does not export run()')
  return run
}

/**
 * Wraps OpenAI Agents SDK runs in Supervisor run/activity/attempt records.
 * The SDK is loaded lazily; callers may inject `run` for tests or custom runners.
 */
export function createOpenAIAgentsAdapter({ client, context = {}, connect, run: runOverride, runner } = {}) {
  const framework = createFrameworkAdapter({
    client,
    framework: 'openai-agents',
    context,
    ...(connect ? { connect } : {}),
  })

  return {
    /** Wrap an OpenAI Agent while preserving the SDK's native result objects. */
    wrap(agent) {
      async function invoke(input, options = {}, streaming = false, adapterOptions = {}) {
        const signal = options.signal
        return framework.invoke({
          activityKind: adapterOptions.activityKind ?? (streaming ? 'openai-agents.stream' : 'openai-agents.run'),
          signal: adapterOptions.signal ?? signal,
          execute: async (invocationContext) => {
            let result
            if (adapterOptions.execute) {
              result = await adapterOptions.execute(invocationContext)
            } else {
              const run = runner?.run ? runner.run.bind(runner) : (runOverride ?? await resolveRun())
              const forwardedOptions = {
                ...options,
                ...(streaming ? { stream: true } : {}),
                ...(invocationContext.signal ? { signal: invocationContext.signal } : {}),
              }
              result = await run(agent, input, forwardedOptions)
            }
            return streaming ? awaitCompletionForLifecycle(result) : result
          },
        })
      }

      return {
        run(input, options = {}, adapterOptions = {}) {
          return invoke(input, options, options.stream === true, adapterOptions)
        },
        runStream(input, options = {}, adapterOptions = {}) {
          return invoke(input, options, true, adapterOptions)
        },
        original: agent,
      }
    },
  }
}

function awaitCompletionForLifecycle(result) {
  if (!result || typeof result !== 'object' || !('completed' in result) || typeof result[Symbol.asyncIterator] !== 'function') {
    return result
  }
  const iteratorFactory = result[Symbol.asyncIterator]
  return new Proxy(result, {
    get(target, property, receiver) {
      if (property !== Symbol.asyncIterator) return Reflect.get(target, property, receiver)
      return function (...args) {
        const iterator = Reflect.apply(iteratorFactory, target, args)
        return (async function* () {
          while (true) {
            const item = await iterator.next()
            if (item.done) {
              await target.completed
              return item.value
            }
            yield item.value
          }
        })()
      }
    },
  })
}
