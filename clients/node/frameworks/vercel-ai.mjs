import { createFrameworkAdapter } from './core.mjs'

/**
 * Add Supervisor lifecycle tracking to a Vercel AI SDK Agent/ToolLoopAgent.
 * The SDK remains responsible for model calls, tools, output types and state.
 */
export function createVercelAIAdapter(options) {
  const lifecycle = createFrameworkAdapter({ ...options, framework: options.framework ?? 'vercel-ai' })

  return {
    wrap(agent) {
      return new Proxy(agent, {
        get(target, property, receiver) {
          if (property === 'generate' || property === 'stream') {
            const method = Reflect.get(target, property, target)
            if (typeof method !== 'function') return method
            return (callOptions = {}, adapterOptions = {}) => {
              const signal = adapterOptions.signal ?? callOptions.abortSignal
              return lifecycle.invoke({
                activityKind: adapterOptions.activityKind ?? `vercel-ai.${String(property)}`,
                signal,
                ...(property === 'stream' ? { streamResult: (result) => [result.textStream, result.fullStream].filter(isAsyncIterable) } : {}),
                ...(property === 'stream' ? { streamCompletion: getStreamCompletion } : {}),
                execute: (context) => adapterOptions.execute
                  ? adapterOptions.execute(context)
                  : method.call(target, withAbortSignal(callOptions, context.signal)),
              })
            }
          }
          return Reflect.get(target, property, receiver)
        },
      })
    },
  }
}

function isAsyncIterable(value) {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof value[Symbol.asyncIterator] === 'function'
}

function isPromiseLike(value) {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof value.then === 'function'
}

function getStreamCompletion(result) {
  const text = result?.text
  if (isPromiseLike(text)) return text
  const output = result?.output
  if (isPromiseLike(output)) return output
  return undefined
}

function withAbortSignal(options, signal) {
  if (!signal || options.abortSignal === signal) return options
  return { ...options, abortSignal: signal }
}
