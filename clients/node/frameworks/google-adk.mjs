import { createFrameworkAdapter } from './core.mjs'

/**
 * Add Supervisor lifecycle tracking to a Google ADK TypeScript Runner.
 * Runner request data and its session identity remain owned by ADK; runAsync
 * events are yielded untouched and its native AbortSignal support is used.
 */
export function createGoogleADKAdapter(options) {
  const lifecycle = createFrameworkAdapter({ ...options, framework: options.framework ?? 'google-adk' })

  return {
    wrap(runner) {
      return new Proxy(runner, {
        get(target, property, receiver) {
          if (property === 'runAsync') {
            const method = Reflect.get(target, property, target)
            if (typeof method !== 'function') return method
            return (request, runOptions = {}, adapterOptions = {}) => (async function* () {
              const signal = adapterOptions.signal ?? runOptions.signal
              const stream = await lifecycle.invoke({
                activityKind: adapterOptions.activityKind ?? 'google-adk.runAsync',
                signal,
                execute: (context) => adapterOptions.execute
                  ? adapterOptions.execute(context)
                  : method.call(target, request, withSignal(runOptions, context.signal)),
              })
              yield* stream
            })()
          }
          return Reflect.get(target, property, receiver)
        },
      })
    },
  }
}

function withSignal(options, signal) {
  if (!signal || options.signal === signal) return options
  return { ...options, signal }
}
