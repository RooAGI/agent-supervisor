import type { FrameworkAdapterOptions, FrameworkInvocationContext } from './core.mjs'

export interface GoogleADKAdapterOptions extends Omit<FrameworkAdapterOptions, 'framework'> {
  framework?: string
}

export interface GoogleADKRunOptions {
  signal?: AbortSignal
  [key: string]: unknown
}

export interface GoogleADKAdapterCallOptions {
  activityKind?: string
  signal?: AbortSignal
  execute?: (context: FrameworkInvocationContext) => AsyncIterable<unknown> | Promise<AsyncIterable<unknown>>
}

export interface GoogleADKRunner<Request = unknown, Event = unknown> {
  runAsync(request: Request, options?: GoogleADKRunOptions): AsyncIterable<Event>
}

export function createGoogleADKAdapter(options: GoogleADKAdapterOptions): {
  wrap<Request, Event, Runner extends GoogleADKRunner<Request, Event>>(runner: Runner): Omit<Runner, 'runAsync'> & {
    runAsync(request: Request, runOptions?: GoogleADKRunOptions, adapterOptions?: GoogleADKAdapterCallOptions): AsyncIterable<Event>
  }
}
