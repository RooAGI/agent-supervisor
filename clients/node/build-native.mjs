import { spawnSync } from 'node:child_process'
import { copyFileSync, mkdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const manifest = join(here, 'native', 'Cargo.toml')
const build = spawnSync('cargo', ['build', '--locked', '--release', '--manifest-path', manifest], { stdio: 'inherit' })
if (build.status !== 0) process.exit(build.status ?? 1)

const extension = process.platform === 'darwin' ? 'dylib' : process.platform === 'win32' ? 'dll' : 'so'
const prefix = process.platform === 'win32' ? '' : 'lib'
const source = join(here, 'native', 'target', 'release', `${prefix}agent_supervisor_node_native.${extension}`)
const outputDir = join(here, 'native')
mkdirSync(outputDir, { recursive: true })
copyFileSync(source, join(outputDir, 'agent_supervisor_node_native.node'))
console.log('Built clients/node/native/agent_supervisor_node_native.node')
