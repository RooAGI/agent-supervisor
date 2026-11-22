import { createFrameworkAdapter } from './core.mjs'

function requireMethod(agent, method) {
  if (!agent || typeof agent[method] !== 'function') {
    throw new TypeError(`LangChain runnable must implement ${method}()`)
  }
}

/**
 * Wraps LangChain.js Runnables, including agents created by `createAgent`.
 * The LangChain package is intentionally not imported here; pass the runnable
 * created by the optional `langchain` peer dependency.
 */
export function createLangChainAdapter({ client, context = {}, connect } = {}) {
  const framework = createFrameworkAdapter({
    client,
    framework: 'langchain',
    context,
    ...(connect ? { connect } : {}),
  })

  return {
    /** Wrap a LangChain Runnable (including `createAgent()` results). */
    wrap(agent) {
      return {
        /** Invoke and return the Runnable's native value unchanged. */
        invoke(input, config = {}, adapterOptions = {}) {
          requireMethod(agent, 'invoke')
          const signal = adapterOptions.signal ?? config.signal
          return framework.invoke({
            activityKind: adapterOptions.activityKind ?? 'langchain.invoke',
            signal,
            execute: (invocationContext) => adapterOptions.execute
              ? adapterOptions.execute(invocationContext)
              : agent.invoke(input, withSignal(config, invocationContext.signal)),
          })
        },
        /** Stream events without changing their values. */
        stream(input, config = {}, adapterOptions = {}) {
          requireMethod(agent, 'stream')
          const signal = adapterOptions.signal ?? config.signal
          return framework.invoke({
            activityKind: adapterOptions.activityKind ?? 'langchain.stream',
            signal,
            execute: (invocationContext) => adapterOptions.execute
              ? adapterOptions.execute(invocationContext)
              : agent.stream(input, withSignal(config, invocationContext.signal)),
          })
        },
        original: agent,
      }
    },
  }
}

function withSignal(config, signal) {
  if (!signal || config.signal === signal) return config
  return { ...config, signal }
}
