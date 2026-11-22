import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm, writeFile, chmod, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { execFileSync, spawn } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'
import { SupervisorClient } from './index.mjs'

const binary = process.argv[2]
const runtimeRoot = process.argv[3]
if (!binary || !runtimeRoot) {
  throw new Error('usage: node clients/node/provider-tool.e2e.mjs /path/to/agent-supervisor-service /path/to/roo-runtime')
}

const runtimePackage = resolve(runtimeRoot, 'packages/runtime-node')
const serviceBinary = resolve(binary)
const workerEntry = join(runtimePackage, 'src/supervisor-worker.ts')
const dir = await mkdtemp(join(tmpdir(), 'rooagi-provider-tool-e2e-'))
const socketPath = join(dir, 'supervisor.sock')
const credentialsDir = join(dir, 'credentials')
const durableSessionDirectory = join(dir, 'framework-session')
const codexFixture = join(dir, 'codex-fixture')
const workerLauncher = join(dir, 'worker-launcher.mjs')
const workerDiagnostic = join(dir, 'worker-stderr.log')
await mkdir(durableSessionDirectory, { recursive: true })
await writeFile(codexFixture, `#!/usr/bin/env node
let buffer = '';
process.stdin.setEncoding('utf8');
const send = frame => process.stdout.write(JSON.stringify(frame) + '\\n');
process.stdin.on('data', chunk => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf('\\n'); if (newline < 0) break;
    const frame = JSON.parse(buffer.slice(0, newline)); buffer = buffer.slice(newline + 1);
    if (typeof frame.id !== 'number') continue;
    let result = {};
    if (frame.method === 'thread/start') result = { thread: { id: 'e2e-thread' } };
    send({ jsonrpc: '2.0', id: frame.id, result });
    if (frame.method === 'turn/start') void (async () => {
      const fs = require('node:fs');
      const config = fs.readFileSync(process.env.CODEX_HOME + '/config.toml', 'utf8');
      const url = config.match(/url = "([^"]+)"/)[1];
      const auth = config.match(/Authorization = "([^"]+)"/)[1];
      const headers = { authorization: auth, 'content-type': 'application/json' };
      const init = await fetch(url, { method:'POST', headers, body:JSON.stringify({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-03-26',capabilities:{},clientInfo:{name:'fixture',version:'1'}}}) });
      const session = init.headers.get('mcp-session-id');
      await fetch(url, { method:'POST',headers:{...headers,'mcp-session-id':session},body:JSON.stringify({jsonrpc:'2.0',id:2,method:'tools/call',params:{name:'desktop__tools_search',arguments:{query:'browser open'}}}) });
      send({ jsonrpc:'2.0', method:'item/agentMessage/delta', params:{delta:'Supervisor tool result reached provider.'} });
      send({ jsonrpc:'2.0', method:'turn/completed', params:{turn:{status:'completed'}} });
    })();
  }
});
`)
await chmod(codexFixture, 0o700)
await writeFile(workerLauncher, `import { appendFileSync } from 'node:fs';
import { spawn } from 'node:child_process';
const child = spawn(process.execPath, process.argv.slice(2), { cwd: process.cwd(), env: process.env, stdio: ['pipe', 'pipe', 'pipe'] });
process.stdin.pipe(child.stdin);
child.stdout.pipe(process.stdout);
let remaining = 8192;
child.stderr.on('data', chunk => {
  if (remaining <= 0) return;
  const bounded = chunk.subarray(0, remaining);
  appendFileSync(process.env.ROOAGI_WORKER_DIAGNOSTIC_FILE, bounded);
  remaining -= bounded.byteLength;
});
child.once('error', error => { process.stderr.write(String(error.message).slice(0, 1024)); process.exitCode = 1; });
child.once('close', code => { process.exitCode = code ?? 1; });
`)

const issuedPaths = execFileSync(serviceBinary, ['--init-credentials-dir', credentialsDir], { encoding: 'utf8' })
const tokens = Object.fromEntries(issuedPaths.trim().split('\n').map((line) => {
  const [key, value] = line.split('=', 2)
  return [key.replace('-token-file', ''), value]
}))
process.env.ROOAGI_CODEX_EXECUTABLE = codexFixture
process.env.ROOAGI_WORKER_DIAGNOSTIC_FILE = workerDiagnostic
const service = spawn(serviceBinary, [
  '--socket', socketPath,
  '--view-token-file', tokens.view,
  '--control-token-file', tokens.control,
  '--tui-token-file', tokens.tui,
  '--workflow-worker-command', process.execPath,
  '--workflow-worker-arg', workerLauncher,
  '--workflow-worker-arg', '--import',
  '--workflow-worker-arg', 'tsx',
  '--workflow-worker-arg', workerEntry,
  '--workflow-worker-cwd', runtimePackage,
  '--workflow-worker-env', 'PATH',
  '--workflow-worker-env', 'ROOAGI_CODEX_EXECUTABLE',
  '--workflow-worker-env', 'ROOAGI_WORKER_DIAGNOSTIC_FILE'
], {
  cwd: runtimePackage,
  stdio: ['ignore', 'pipe', 'pipe'],
  env: process.env
})
let client
let serviceLogs = ''
let currentStage = 'service startup'
for (const stream of [service.stdout, service.stderr]) {
  stream.setEncoding('utf8')
  stream.on('data', (chunk) => {
    if (serviceLogs.length < 8192) serviceLogs += String(chunk).slice(0, 8192 - serviceLogs.length)
  })
}

try {
  for (let attempt = 0; attempt < 150; attempt++) {
    try {
      client = await SupervisorClient.connect({ socketPath, tokenPath: tokens.control, role: 'control' })
      break
    } catch (error) {
      if (service.exitCode !== null) throw error
      await delay(20)
    }
  }
  assert.ok(client, 'service socket becomes ready')
  const status = await client.snapshot()
  assert.equal(status.workflowDispatchConfigured, true)

  currentStage = 'register FlowRun'
  const registered = await client.registerFlowRun({
    coordinatorId: 'desktop-e2e',
    projection: {
      flowId: 'provider-tool-flow',
      boardId: 'provider-tool-board',
      flowRevision: 0,
      tasks: [{
        taskId: 'provider-tool-task', postId: 'provider-tool-post', title: 'Browser task',
        dependencies: [], assignee: null, state: 'ready', waitingReason: null,
        attemptId: null, generation: null, lastSourceCursor: 0
      }]
    },
    invocations: {
      'provider-tool-task': {
        protocolVersion: 1,
        workerKind: 'codex',
        providerInvocation: {
          profile: {
            provider: 'openai', authMode: 'chatgptOAuth', model: 'fixture-model',
            apiKey: 'fixture-api-key', executable: codexFixture
          },
          messages: [{ role: 'user', content: 'Search for browser tools.' }],
          mcpTools: [{
            type: 'function',
            function: {
              name: 'desktop__tools_search',
              description: 'Search connected tools.',
              parameters: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] }
            }
          }],
          durableSessionDirectory
        }
      }
    }
  })
  currentStage = 'claim FlowRun'
  const claimed = await client.claimFlowRun({
    flowId: registered.flowId, coordinatorId: 'desktop-e2e', expectedFence: registered.coordinatorFence
  })

  currentStage = 'wait for worker tool request'
  let pending
  for (let attempt = 0; attempt < 400; attempt++) {
    const snapshot = await client.snapshot()
    pending = snapshot.toolRequests?.find((request) => request.state === 'pending')
    if (pending) break
    await delay(25)
  }
  assert.ok(pending, `the service received the worker MCP request: ${JSON.stringify(await client.snapshot())}`)
  assert.equal(pending.flowId, 'provider-tool-flow')
  assert.equal(pending.taskId, 'provider-tool-task')
  assert.equal(pending.toolName, 'desktop__tools_search')
  assert.equal(pending.arguments.query, 'browser open')

  // Simulate Desktop coordinator teardown and recreation while the provider
  // process and framework protocol loop remain owned by Supervisor.
  currentStage = 'reconnect Control client'
  client.close()
  client = undefined
  await delay(50)
  client = await SupervisorClient.connect({ socketPath, tokenPath: tokens.control, role: 'control' })
  currentStage = 'list pending request after reconnect'
  const durableRequest = (await client.listPendingToolRequests()).find((item) => item.requestId === pending.requestId)
  assert.ok(durableRequest, 'pending tool request survives coordinator teardown')
  assert.equal(durableRequest.attemptId, pending.attemptId)
  assert.equal(durableRequest.generation, pending.generation)
  const key = `${durableRequest.attemptId}:${durableRequest.generation}:${durableRequest.requestId}`
  const claimId = `desktop-tool-claim:${key}`
  const resolutionId = `desktop-tool-result:${key}`
  currentStage = 'resolve worker tool request'
  const requestClaim = await client.claimToolRequest({
    attemptId: durableRequest.attemptId,
    generation: durableRequest.generation,
    requestId: durableRequest.requestId,
    claimId,
    fence: claimed.coordinatorFence
  })
  assert.equal(requestClaim.state, 'claimed')
  const result = JSON.stringify({ results: [{ toolId: 'browser.open', name: 'open' }], status: 'ok' })
  const resolved = await client.resolveToolRequest({
    attemptId: durableRequest.attemptId,
    generation: durableRequest.generation,
    requestId: durableRequest.requestId,
    claimId,
    resolutionId,
    result,
    fence: claimed.coordinatorFence
  })
  assert.equal(resolved.state, 'completed')
  assert.equal(resolved.result, result)

  currentStage = 'wait for worker completion'
  let task
  for (let attempt = 0; attempt < 400; attempt++) {
    const snapshot = await client.snapshot()
    task = snapshot.flowRuns?.find((flow) => flow.flowId === registered.flowId)?.tasks['provider-tool-task']
    if (task?.state === 'readyForReview') break
    await delay(25)
  }
  assert.equal(task?.state, 'readyForReview', 'the provider worker completed after the Desktop result returned')
  assert.match(String(task?.lastOutcome?.evidence), /Supervisor tool result reached provider/)
  assert.ok(Buffer.byteLength(String(task?.lastOutcome?.evidence)) <= 16 * 1024)
  assert.ok((await readFile(join(durableSessionDirectory, 'codex', 'thread.json'), 'utf8')).includes('e2e-thread'))
  currentStage = 'close Desktop client after successful worker result'
  client.close()
  client = undefined
  service.kill('SIGTERM')
  await new Promise((resolvePromise) => service.once('exit', resolvePromise))
} catch (error) {
  let workerStderr = ''
  try { workerStderr = await readFile(workerDiagnostic, 'utf8') } catch {}
  throw new Error(`stage=${currentStage}; ${error instanceof Error ? error.message : String(error)}; service exit=${service.exitCode}; bounded service output=${serviceLogs}; bounded worker stderr=${workerStderr}`)
} finally {
  client?.close()
  if (service.exitCode === null && service.signalCode === null) {
    service.kill('SIGTERM')
    await new Promise((resolvePromise) => service.once('exit', resolvePromise))
  }
  await rm(dir, { recursive: true, force: true })
}

console.log('Supervisor service, Roo Runtime Codex provider worker, durable MCP request reconnect/result, and bounded FlowRun evidence passed')
