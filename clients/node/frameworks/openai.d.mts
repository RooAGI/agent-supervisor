import type { FrameworkAdapterClient, FrameworkClientConnector, FrameworkInvocationContext } from './core.mjs'

export interface OpenAIAgentsRunOptions extends Record<string, unknown> {
  signal?: AbortSignal
  stream?: boolean
}

export interface OpenAIAgentsRun<TAgent = unknown, TInput = unknown, TResult = unknown> {
  (agent: TAgent, input: TInput, options?: OpenAIAgentsRunOptions): Promise<TResult>
}

export interface OpenAIAgentsRunner<TAgent = unknown, TInput = unknown, TResult = unknown> {
  run(agent: TAgent, input: TInput, options?: OpenAIAgentsRunOptions): Promise<TResult>
}

export interface OpenAIAgentsCallOptions<TResult = unknown> {
  signal?: AbortSignal
  activityKind?: string
  /** Replace the SDK call for this invocation; use `context.startProcess()` for explicit managed children. */
  execute?(context: FrameworkInvocationContext): TResult | Promise<TResult>
}

export interface OpenAIAgentsWrapped<TAgent, TInput = unknown, TResult = unknown, TEvent = unknown> {
  readonly original: TAgent
  run(input: TInput, options?: OpenAIAgentsRunOptions, adapterOptions?: OpenAIAgentsCallOptions<TResult>): Promise<TResult>
  runStream(input: TInput, options?: OpenAIAgentsRunOptions, adapterOptions?: OpenAIAgentsCallOptions<TResult & AsyncIterable<TEvent>>): Promise<TResult & AsyncIterable<TEvent>>
}

export interface OpenAIAgentsAdapter<TAgent = unknown, TInput = unknown, TResult = unknown, TEvent = unknown> {
  wrap(agent: TAgent): OpenAIAgentsWrapped<TAgent, TInput, TResult, TEvent>
}

export interface OpenAIAgentsAdapterOptions<TAgent = unknown, TInput = unknown, TResult = unknown> {
  client: FrameworkAdapterClient
  context?: Record<string, unknown>
  connect?: FrameworkClientConnector
  /** Optional injected SDK `run()` function or provider-free test implementation. */
  run?: OpenAIAgentsRun<TAgent, TInput, TResult>
  /** Optional SDK Runner instance; its `run` method is called with the correct receiver. */
  runner?: OpenAIAgentsRunner<TAgent, TInput, TResult>
}

/** Wrap OpenAI Agents SDK `run()` calls. `@openai/agents` is loaded on first use. */
export function createOpenAIAgentsAdapter<TAgent = unknown, TInput = unknown, TResult = unknown, TEvent = unknown>(
  options: OpenAIAgentsAdapterOptions<TAgent, TInput, TResult>,
): OpenAIAgentsAdapter<TAgent, TInput, TResult, TEvent>
