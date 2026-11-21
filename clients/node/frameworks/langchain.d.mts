import type { FrameworkAdapterClient, FrameworkClientConnector, FrameworkInvocationContext } from './core.mjs'

export interface LangChainRunnableConfig {
  signal?: AbortSignal
  callbacks?: unknown
  metadata?: Record<string, unknown>
  configurable?: Record<string, unknown>
}

export interface LangChainRunnable<TInput = unknown, TOutput = unknown, TChunk = unknown, TConfig extends object = LangChainRunnableConfig> {
  invoke(input: TInput, config?: TConfig): TOutput | Promise<TOutput>
  stream(input: TInput, config?: TConfig): AsyncIterable<TChunk> | Promise<AsyncIterable<TChunk>>
}

export interface LangChainAdapterOptions {
  client: FrameworkAdapterClient
  context?: Record<string, unknown>
  connect?: FrameworkClientConnector
}

export interface LangChainCallOptions<TResult = unknown> {
  signal?: AbortSignal
  activityKind?: string
  /** Replace this invocation; use `context.startProcess()` for explicit managed children. */
  execute?(context: FrameworkInvocationContext): TResult | Promise<TResult>
}

export interface LangChainWrapped<TInput = unknown, TOutput = unknown, TChunk = unknown, TConfig extends object = LangChainRunnableConfig, TAgent extends LangChainRunnable<TInput, TOutput, TChunk, TConfig> = LangChainRunnable<TInput, TOutput, TChunk, TConfig>> {
  readonly original: TAgent
  invoke(input: TInput, config?: TConfig, adapterOptions?: LangChainCallOptions<TOutput>): Promise<Awaited<TOutput>>
  stream(input: TInput, config?: TConfig, adapterOptions?: LangChainCallOptions<AsyncIterable<TChunk>>): Promise<AsyncIterable<TChunk>>
}

export interface LangChainAdapter {
  wrap<TInput, TOutput, TChunk, TConfig extends object, TAgent extends LangChainRunnable<TInput, TOutput, TChunk, TConfig>>(
    agent: TAgent,
  ): LangChainWrapped<TInput, TOutput, TChunk, TConfig, TAgent>
}

/** Wrap LangChain.js Runnables, including agents from `createAgent()`. */
export function createLangChainAdapter(options: LangChainAdapterOptions): LangChainAdapter
