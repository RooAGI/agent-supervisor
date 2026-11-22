import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'

const require = createRequire(import.meta.url)
let binding

export function nativeBinding() {
  if (binding) return binding
  try {
    binding = require(fileURLToPath(new URL('./native/agent_supervisor_node_native.node', import.meta.url)))
    return binding
  } catch (cause) {
    const error = new Error('The embedded Agent Supervisor native binding is not built. Run `npm run build:native` from the package source or install a package containing a prebuilt binding.')
    error.cause = cause
    throw error
  }
}
