import type { FrameworkAdapterOptions, FrameworkInvocationContext, FrameworkInvokeOptions } from './core.mjs'

export interface VercelAIAdapterOptions extends Omit<FrameworkAdapterOptions, 'framework'> {
  framework?: string
}

export interface VercelAICallOptions extends Omit<FrameworkInvokeOptions<unknown>, 'execute'> {
  activityKind?: string
  execute?: (context: FrameworkInvocationContext) => unknown | Promise<unknown>
}

export interface VercelAIAgent<GenerateOptions = unknown, GenerateResult = unknown, StreamOptions = unknown, StreamResult = unknown> {
  generate(options?: GenerateOptions): Promise<GenerateResult>
  stream(options?: StreamOptions): StreamResult | Promise<StreamResult>
}

export function createVercelAIAdapter(options: VercelAIAdapterOptions): {
  wrap<GenerateOptions, GenerateResult, StreamOptions, StreamResult, Agent extends VercelAIAgent<GenerateOptions, GenerateResult, StreamOptions, StreamResult>>(
    agent: Agent,
  ): Omit<Agent, 'generate' | 'stream'> & {
    generate(options?: GenerateOptions, adapterOptions?: VercelAICallOptions): Promise<GenerateResult>
    stream(options?: StreamOptions, adapterOptions?: VercelAICallOptions): Promise<StreamResult>
  }
}
