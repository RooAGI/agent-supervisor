import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFileSync, spawn } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'
import { SupervisorClient } from './index.mjs'

const binary = process.argv[2]
if (!binary) throw new Error('usage: node clients/node/service.e2e.mjs /path/to/agent-supervisor-service')
const dir = await mkdtemp(join(tmpdir(), 'agent-supervisor-e2e-'))
const socketPath = join(dir, 'supervisor.sock')
const credentialsDir = join(dir, 'credentials')
const issuedPaths = execFileSync(binary, ['--init-credentials-dir', credentialsDir], { encoding: 'utf8' })
const tokens = Object.fromEntries(issuedPaths.trim().split('\n').map((line) => {
  const [key, value] = line.split('=', 2)
  return [key.replace('-token-file', ''), value]
}))
for (const role of ['view', 'control', 'tui']) {
  assert.ok(tokens[role])
  assert.ok(!issuedPaths.includes((await readFile(tokens[role], 'utf8')).trim()), 'credential initializer never prints token values')
}
const service = spawn(binary, [
  '--socket', socketPath,
  '--view-token-file', tokens.view,
  '--control-token-file', tokens.control,
  '--tui-token-file', tokens.tui
], { stdio: 'ignore' })
let client
try {
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      client = await SupervisorClient.connect({ socketPath, tokenPath: tokens.control, role: 'control' })
      break
    } catch (error) {
      if (service.exitCode !== null) throw error
      await delay(20)
    }
  }
  assert.ok(client, 'service socket becomes ready')
  const policy = {
    environment: { inherit: [], variables: {}, executableSearchPaths: [] },
    filesystem: null,
    network: 'host',
    limits: { timeoutMs: 5000, inputBytes: 65536, outputBytes: 65536, stderrBytes: 65536, memoryBytes: null, maxProcesses: null, cpuQuotaMicros: null },
    enforcement: 'best_effort'
  }
  const child = await client.start({
    executable: '/bin/sh', args: ['-c', 'read value; printf "out:%s\\n" "$value"; printf "err:%s\\n" "$value" >&2'],
    metadata: { framework: 'node-e2e', projectId: 'project', flowRunId: 'flow', taskId: 'task' },
    policy
  })
  let stdout = ''
  let stderr = ''
  child.stdout.setEncoding('utf8').on('data', (chunk) => { stdout += chunk })
  child.stderr.setEncoding('utf8').on('data', (chunk) => { stderr += chunk })
  child.stdin.end('input\n')
  const receipt = await child.wait()
  assert.match(stdout, /out:input/)
  assert.match(stderr, /err:input/)
  assert.equal(receipt.termination.Exited.code, 0)

  const detached = await client.start({ executable: '/bin/sh', args: ['-c', 'sleep 0.1; echo survived'], policy })
  const detachedId = detached.processId
  client.close()
  client = await SupervisorClient.connect({ socketPath, tokenPath: tokens.control, role: 'control' })
  const reattached = await client.attach(detachedId)
  let replayed = ''
  reattached.stdout.setEncoding('utf8').on('data', (chunk) => { replayed += chunk })
  await reattached.wait()
  assert.match(replayed, /survived/, 'the service owns output draining after client disconnect')

  const flowboardPostId = 'board:flowboard%3Aa6e188d5a21606a1c071b0a7a8f2b33a17a5fb43387093523c9d48655ad27f7e:/Users/louis/sources/rooagi-sandbox:agent-supervisor:post:00000000000000000017'
  assert.ok(Buffer.byteLength(flowboardPostId) > 128)
  const stoppable = await client.start({
    executable: '/bin/sleep', args: ['30'],
    metadata: { framework: 'codex', runLabel: 'provider-run', projectId: 'project', flowRunId: 'flow', taskId: 'task', postId: flowboardPostId, workerId: 'worker', runtimeRunId: 'runtime' },
    policy
  })
  const view = await SupervisorClient.connect({ socketPath, tokenPath: tokens.view, role: 'view' })
  const row = (await view.snapshot()).processes.find((process) => process.processId === stoppable.processId)
  assert.equal(row?.postId, flowboardPostId)
  assert.equal(row?.workerId, 'worker')
  assert.equal(row?.runtimeRunId, 'runtime')
  assert.equal(row?.runLabel, 'provider-run')
  let confirmedTuiStop = 0
  stoppable.on('supervisorStop', () => { confirmedTuiStop++ })
  const tui = await SupervisorClient.connect({ socketPath, tokenPath: tokens.tui, role: 'tui' })
  const tuiHandle = await tui.attach(stoppable.processId)
  await tuiHandle.stop(0)
  await stoppable.wait()
  assert.equal(confirmedTuiStop, 1, 'only authenticated TUI stop followed by reaped exit confirms cancellation')

  assert.equal(view.role, 'view')
  await assert.rejects(() => view.start({ executable: 'true', args: [], policy }), /control credential/)
  const viewDisconnected = new Promise((resolve) => view.once('disconnect', resolve))
  assert.equal((await client.revokeRole('view')).role, 'view')
  await viewDisconnected
  await assert.rejects(() => SupervisorClient.connect({ socketPath, tokenPath: tokens.view, role: 'view' }), /authentication failed/)
  const tuiDisconnected = new Promise((resolve) => tui.once('disconnect', resolve))
  assert.equal((await client.revokeRole('tui')).role, 'tui')
  await tuiDisconnected
  await assert.rejects(() => SupervisorClient.connect({ socketPath, tokenPath: tokens.tui, role: 'tui' }), /authentication failed/)
  console.log('Node client/service start, stdio, exit/reap, reconnect, TUI stop, and view-only checks passed')
} finally {
  client?.close()
  service.kill('SIGTERM')
  await new Promise((resolve) => service.once('exit', resolve))
  await rm(dir, { recursive: true, force: true })
}
