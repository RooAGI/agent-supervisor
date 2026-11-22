import { createFrameworkAdapter } from './core.mjs'

/**
 * Create an adapter for compiled LangGraph.js graphs.
 *
 * Graph inputs, RunnableConfig (including thread_id/checkpointer settings),
 * streamed values, and errors are passed through unchanged. The adapter adds
 * only the caller's AbortSignal to RunnableConfig when supplied.
 */
export function createLangGraphAdapter(options) {
  const lifecycle = createFrameworkAdapter({ ...options, framework: options.framework ?? 'langgraph' })

  return {
    wrap(graph) {
      return {
        invoke(input, config = {}, adapterOptions = {}) {
          const signal = adapterOptions.signal ?? config.signal
          return lifecycle.invoke({
            activityKind: adapterOptions.activityKind ?? 'langgraph.invoke',
            signal,
            execute: (context) => adapterOptions.execute
              ? adapterOptions.execute(context)
              : graph.invoke(input, withSignal(config, context.signal)),
          })
        },
        stream(input, config = {}, adapterOptions = {}) {
          const signal = adapterOptions.signal ?? config.signal
          return lifecycle.invoke({
            activityKind: adapterOptions.activityKind ?? 'langgraph.stream',
            signal,
            execute: (context) => adapterOptions.execute
              ? adapterOptions.execute(context)
              : graph.stream(input, withSignal(config, context.signal)),
          })
        },
        original: graph,
      }
    },
  }
}

function withSignal(config, signal) {
  if (!signal || config.signal === signal) return config
  return { ...config, signal }
}
