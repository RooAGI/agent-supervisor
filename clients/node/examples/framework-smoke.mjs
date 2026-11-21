import { SupervisorClient } from '@rooagi/agent-supervisor'
import { createLangChainAdapter } from '@rooagi/agent-supervisor/frameworks/langchain'

const socketPath = process.env.SUPERVISOR_SOCKET
const tokenPath = process.env.SUPERVISOR_CONTROL_TOKEN_FILE
if (!socketPath || !tokenPath) {
  throw new Error('Set SUPERVISOR_SOCKET and SUPERVISOR_CONTROL_TOKEN_FILE for a running Agent Supervisor service')
}

const client = await SupervisorClient.connect({ socketPath, tokenPath, role: 'control' })
const fixtureRunnable = {
  async invoke(input, config) {
    return { input, threadId: config?.configurable?.thread_id ?? null }
  },
  async *stream(input, config) {
    yield { input, threadId: config?.configurable?.thread_id ?? null }
  },
}
const managedAgent = createLangChainAdapter({ client }).wrap(fixtureRunnable)

try {
  const result = await managedAgent.invoke(
    { prompt: 'provider-free fixture' },
    { configurable: { thread_id: 'smoke-thread' } },
  )
  console.log(JSON.stringify(result))
} finally {
  client.close()
}
