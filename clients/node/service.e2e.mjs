import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFileSync, spawn, spawnSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
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
let reporter
let canceller
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
  const longPolicy = { ...policy, limits: { ...policy.limits, timeoutMs: 30000 } }
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
  const outputClient = await SupervisorClient.connect({ socketPath, token: detached.outputToken, role: 'output' })
  await assert.rejects(() => outputClient.snapshot(), /credential required to read metadata snapshots/)
  const reattached = await outputClient.attach(detachedId)
  let replayed = ''
  reattached.stdout.setEncoding('utf8').on('data', (chunk) => { replayed += chunk })
  await reattached.wait()
  assert.match(replayed, /survived/, 'the service owns output draining after client disconnect')
  outputClient.close()

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
  await assert.rejects(() => view.attach(stoppable.processId), /process output credential required to attach/)
  let confirmedTuiStop = 0
  stoppable.on('supervisorStop', () => { confirmedTuiStop++ })
  const tui = await SupervisorClient.connect({ socketPath, tokenPath: tokens.tui, role: 'tui' })
  const tuiHandle = await tui.attach(stoppable.processId)
  try { await tuiHandle.stop(100) } catch (error) { throw new Error(`TUI targeted stop failed: ${error.message}`) }
  await stoppable.wait()
  assert.equal(confirmedTuiStop, 1, 'only authenticated TUI stop followed by reaped exit confirms cancellation')

  assert.equal(view.role, 'view')
  await assert.rejects(() => view.start({ executable: 'true', args: [], policy }), /control credential/)
  const run = await client.registerRun({ projectId: 'project', flowRunId: 'flow' })
  const activity = await client.registerActivity({ runId: run.runId, kind: 'worker' })
  const attempt = await client.registerAttempt({ activityId: activity.activityId })
  const nodeFlow = await client.registerFlowRun({
    coordinatorId: 'node-e2e',
    projection: {
      flowId: 'flow-node-e2e', boardId: 'board-node-e2e', flowRevision: 0,
      tasks: [{ taskId: 'task-node-e2e', postId: 'post-node-e2e', title: 'Node FlowRun task', dependencies: [], assignee: null, state: 'ready', waitingReason: null, attemptId: null, generation: null, lastSourceCursor: 0 }]
    }
  })
  assert.equal(nodeFlow.flowId, 'flow-node-e2e')
  const claimedNodeFlow = await client.claimFlowRun({ flowId: nodeFlow.flowId, coordinatorId: 'node-e2e', expectedFence: 0 })
  assert.equal(claimedNodeFlow.coordinatorFence, 1)
  const nodeInvocation = { runtimeOptions: { checkpointEndpoint: 'http://127.0.0.1/checkpoints' }, graph: { id: 'node-e2e' }, input: { messages: [] } }
  assert.equal((await client.updateFlowInvocations({ flowId: nodeFlow.flowId, invocations: { 'task-node-e2e': nodeInvocation }, fence: claimedNodeFlow.coordinatorFence })).count, 1)
  await client.bindFlowAttempt({
    flowId: nodeFlow.flowId, taskId: 'task-node-e2e', attemptId: 'attempt-node-e2e',
    generation: 1, sourceCursor: 1, fence: 1
  })
  await client.reconcileFlowAttempt({
    flowId: nodeFlow.flowId, taskId: 'task-node-e2e', fence: 1,
    outcome: { attemptId: 'attempt-node-e2e', generation: 1, sourceCursor: 2, status: 'succeeded', workflowCancelRequested: false }
  })
  const reviewedNodeFlow = await client.applyFlowReview({
    flowId: nodeFlow.flowId, taskId: 'task-node-e2e', flowRevision: 1, fence: 1,
    evidence: { verdict: 'approved', summary: 'Node client contract test' }
  })
  assert.equal(reviewedNodeFlow.tasks['task-node-e2e'].state, 'completed')
  const cancelFlow = await client.registerFlowRun({
    coordinatorId: 'node-cancel-e2e',
    projection: {
      flowId: 'flow-node-cancel-e2e', boardId: 'board-node-cancel-e2e', flowRevision: 0,
      tasks: [{ taskId: 'task-node-cancel-e2e', postId: 'post-node-cancel-e2e', title: 'Cancelable task', dependencies: [], assignee: null, state: 'ready', waitingReason: null, attemptId: null, generation: null, lastSourceCursor: 0 }]
    }
  })
  const claimedCancelFlow = await client.claimFlowRun({ flowId: cancelFlow.flowId, coordinatorId: 'node-cancel-e2e', expectedFence: 0 })
  await client.bindFlowAttempt({ flowId: cancelFlow.flowId, taskId: 'task-node-cancel-e2e', attemptId: 'attempt-node-cancel-e2e', generation: 1, sourceCursor: 1, fence: claimedCancelFlow.coordinatorFence })
  const cancelRequestedFlow = await client.cancelFlowTask({ flowId: cancelFlow.flowId, taskId: 'task-node-cancel-e2e', attemptId: 'attempt-node-cancel-e2e', generation: 1, fence: claimedCancelFlow.coordinatorFence })
  assert.equal(cancelRequestedFlow.tasks['task-node-cancel-e2e'].cancelRequested, true)
  const cancelledFlow = await client.reconcileFlowAttempt({
    flowId: cancelFlow.flowId, taskId: 'task-node-cancel-e2e', fence: claimedCancelFlow.coordinatorFence,
    outcome: { attemptId: 'attempt-node-cancel-e2e', generation: 1, sourceCursor: 2, status: 'cancelled', workflowCancelRequested: true }
  })
  assert.equal(cancelledFlow.tasks['task-node-cancel-e2e'].state, 'cancelled')
  const interoperable = await client.start({
    executable: '/bin/sh', args: ['-c', 'printf "interop-output\\n"; sleep 30'],
    metadata: { framework: 'node-e2e', projectId: 'project', flowRunId: 'flow', taskId: 'interop' },
    policy: longPolicy, attemptId: attempt.attemptId, generation: attempt.generation
  })
  const foreignProcess = await client.start({ executable: '/bin/sleep', args: ['30'], policy: longPolicy })
  reporter = await SupervisorClient.connect({ socketPath, role: 'reporter', token: attempt.reporterToken })
  canceller = await SupervisorClient.connect({ socketPath, role: 'canceller', token: attempt.cancelToken })
  await assert.rejects(() => reporter.cancelAttempt({ attemptId: attempt.attemptId, generation: attempt.generation }), /cancellation credential/)
  await assert.rejects(() => canceller.report({ attemptId: attempt.attemptId, generation: attempt.generation, status: 'running' }), /reporter credential/)
  const scopedOutput = await SupervisorClient.connect({ socketPath, role: 'output', token: interoperable.outputToken })
  await assert.rejects(() => scopedOutput.attach(foreignProcess.processId), (error) => error.code === 'forbidden')
  scopedOutput.close()
  await canceller.cancelAttempt({ attemptId: attempt.attemptId, generation: attempt.generation })
  const python = spawnSync('python3', [fileURLToPath(new URL('./interop_observer.py', import.meta.url)),
    socketPath, tokens.view, tokens.control, attempt.reporterToken, interoperable.outputToken,
    interoperable.processId, foreignProcess.processId, run.runId, activity.activityId,
    attempt.attemptId, String(attempt.generation)], {
    encoding: 'utf8', env: { ...process.env, PYTHONPATH: fileURLToPath(new URL('../../python', import.meta.url)) }, timeout: 15000
  })
  assert.equal(python.status, 0, `Python SDK cross-client observer failed: ${python.stderr}`)
  assert.match(python.stdout, /cross-client snapshot, FlowRun registration\/claim, task cancellation reconciliation, output replay, and scoped cancellation passed/)
  try { await interoperable.stop(100) } catch (error) { throw new Error(`stopping attempt-associated process failed: ${error.message}`) }
  try { await foreignProcess.stop(100) } catch (error) { throw new Error(`stopping process without attempt association failed: ${error.message}`) }
  await interoperable.wait()
  await foreignProcess.wait()
  const coordinatorSnapshot = await client.snapshot()
  assert.equal(coordinatorSnapshot.runs.find((value) => value.runId === run.runId).context.flowRunId, 'flow')
  assert.equal(coordinatorSnapshot.attempts.find((value) => value.attemptId === attempt.attemptId).status, 'cancelled')
  const snapshotFlow = coordinatorSnapshot.flowRuns.find((value) => value.flowId === 'flow-node-e2e')
  assert.equal(snapshotFlow.coordinatorFence, 1)
  assert.equal(snapshotFlow.tasks['task-node-e2e'].state, 'completed')
  assert.ok(!JSON.stringify(coordinatorSnapshot).includes(attempt.reporterToken))
  assert.ok(!JSON.stringify(coordinatorSnapshot).includes(attempt.cancelToken))

  const viewDisconnected = new Promise((resolve) => view.once('disconnect', resolve))
  assert.equal((await client.revokeRole('view')).role, 'view')
  await viewDisconnected
  await assert.rejects(() => SupervisorClient.connect({ socketPath, tokenPath: tokens.view, role: 'view' }), /authentication failed/)
  const tuiDisconnected = new Promise((resolve) => tui.once('disconnect', resolve))
  assert.equal((await client.revokeRole('tui')).role, 'tui')
  await tuiDisconnected
  await assert.rejects(() => SupervisorClient.connect({ socketPath, tokenPath: tokens.tui, role: 'tui' }), /authentication failed/)

  reporter.close()
  canceller.close()
  assert.equal((await client.shutdown(0)).type, 'shutdownComplete')
  await new Promise((resolve, reject) => {
    service.once('exit', (code) => code === 0 ? resolve() : reject(new Error(`service exited with ${code}`)))
  })
  console.log('Node/Python client-service cross-client snapshot, FlowRun coordination, process output capability, authorized cancellation, reattach, stdio, revocation, and shutdown checks passed')
} finally {
  reporter?.close()
  canceller?.close()
  client?.close()
  if (service.exitCode === null) {
    service.kill('SIGTERM')
    await new Promise((resolve) => service.once('exit', resolve))
  }
  await rm(dir, { recursive: true, force: true })
}
