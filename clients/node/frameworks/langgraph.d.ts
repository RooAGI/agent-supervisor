import type { FrameworkAdapterOptions, FrameworkInvocationContext, FrameworkInvokeOptions } from './core.mjs'

export interface LangGraphAdapterOptions extends Omit<FrameworkAdapterOptions, 'framework'> {
  framework?: string
}

export interface LangGraphCallOptions extends Omit<FrameworkInvokeOptions<unknown>, 'execute'> {
  activityKind?: string
  execute?: (context: FrameworkInvocationContext) => unknown | Promise<unknown>
}

export interface LangGraphGraph<Input = unknown, Output = unknown, Config extends { signal?: AbortSignal } = { signal?: AbortSignal }> {
  invoke(input: Input, config?: Config): Promise<Output>
  stream(input: Input, config?: Config): Promise<AsyncIterable<unknown>> | AsyncIterable<unknown>
}

export function createLangGraphAdapter(options: LangGraphAdapterOptions): {
  wrap<Input, Output, Config extends { signal?: AbortSignal }>(graph: LangGraphGraph<Input, Output, Config>): {
    invoke(input: Input, config?: Config, options?: LangGraphCallOptions): Promise<Output>
    stream(input: Input, config?: Config, options?: LangGraphCallOptions): Promise<AsyncIterable<unknown>> | AsyncIterable<unknown>
    readonly original: LangGraphGraph<Input, Output, Config>
  }
}
