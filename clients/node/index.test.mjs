import test from 'node:test'
import assert from 'node:assert/strict'
import { createManagedProcessLauncher, SupervisorProcessHandle, validateProviderEnvironment } from './index.mjs'

test('provider cancellation callback waits for the same TUI-stopped process exit', () => {
  const client = {
    role: 'control',
    startManaged() { return new SupervisorProcessHandle(this) }
  }
  let callbacks = 0
  const launcher = createManagedProcessLauncher({ client, policyForRequest: () => ({ environment: { variables: {} } }) })
  const handle = launcher.start({
    framework: 'other', executable: 'tool', args: [], options: {},
    onSupervisorStop() { callbacks++ }
  })
  handle._bind('v1:supervisor:17')

  handle._event({ type: 'stop_requested', processId: handle.processId, actor: 'tui' })
  assert.equal(callbacks, 0, 'request intent is not completion')
  handle._event({ type: 'exited', processId: 'v1:other:1', receipt: {} })
  assert.equal(callbacks, 0, 'another process exit cannot settle this launch')
  handle._event({ type: 'exited', processId: handle.processId, receipt: {} })
  assert.equal(callbacks, 1, 'matching exit after TUI intent confirms cancellation')
  handle._event({ type: 'exited', processId: handle.processId, receipt: {} })
  assert.equal(callbacks, 1, 'completion callback is emitted once')
})

test('ordinary exit and client stop intent do not report Supervisor cancellation', () => {
  const client = { role: 'control', startManaged() { return new SupervisorProcessHandle(this) } }
  let callbacks = 0
  const launcher = createManagedProcessLauncher({ client, policyForRequest: () => ({ environment: { variables: {} } }) })
  const handle = launcher.start({ framework: 'other', executable: 'tool', args: [], options: {}, onSupervisorStop() { callbacks++ } })
  handle._bind('v1:supervisor:18')
  handle._event({ type: 'stop_requested', processId: handle.processId, actor: 'client' })
  handle._event({ type: 'exited', processId: handle.processId, receipt: {} })
  assert.equal(callbacks, 0)
})

test('Codex and Claude auth environment values must be released exactly by policy', () => {
  const claude = { framework: 'claude', options: { env: { ANTHROPIC_API_KEY: '', CLAUDE_CODE_OAUTH_TOKEN: 'oauth' } } }
  assert.throws(() => validateProviderEnvironment(claude, { environment: { variables: {} } }), /CLAUDE_CODE_OAUTH_TOKEN/)
  assert.doesNotThrow(() => validateProviderEnvironment(claude, { environment: { variables: { ANTHROPIC_API_KEY: '', CLAUDE_CODE_OAUTH_TOKEN: 'oauth' } } }))
  const codex = { framework: 'codex', options: { env: { CODEX_HOME: '/tmp/codex', ACCESS_TOKEN: 'token' } } }
  assert.throws(() => validateProviderEnvironment(codex, { environment: { variables: { CODEX_HOME: '/tmp/codex' } } }), /ACCESS_TOKEN/)
  assert.doesNotThrow(() => validateProviderEnvironment(codex, { environment: { variables: { CODEX_HOME: '/tmp/codex', ACCESS_TOKEN: 'token' } } }))
})
