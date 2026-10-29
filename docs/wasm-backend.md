# WebAssembly runner

The optional `wasm` Cargo feature adds `WasmRunner`, a one-shot `ProcessRunner`
for self-contained WebAssembly tools. Native execution remains the default.

```toml
[dependencies]
agent-supervisor = { version = "0.1.3", features = ["wasm"] }
```

The runner uses Wasmi and exposes no WASI or other host imports. It accepts
only modules with these exports:

- `memory`
- `allocate(i32) -> i32`
- `execute(i32, i32) -> ()`
- `get_result_ptr() -> i32`
- `get_result_len() -> i32`

The runner serializes the request as JSON with `args` (an array of strings) and
`stdin` (an array of byte values). The module's result buffer is returned as
stdout. Stderr is empty. A module that requests host imports is rejected.

```rust,no_run
use agent_supervisor::{
    execute_with_runner, EnvironmentPolicy, ExecutionRequest, FilesystemPolicy,
    NetworkMode, ResourceLimits, SandboxPolicy, WasmRunner, WasmRunnerConfig,
};
use std::{fs, path::PathBuf};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let bytes = fs::read("calculator.wasm")?;
let runner = WasmRunner::from_bytes(
    "calculator.wasm",
    &bytes,
    WasmRunnerConfig::default(),
)?;
let request = ExecutionRequest {
    executable: PathBuf::from("calculator.wasm"),
    args: vec!["{\"operation\":\"add\",\"a\":2,\"b\":3}".into()],
    working_directory: None,
    policy: SandboxPolicy {
        environment: EnvironmentPolicy::default(),
        filesystem: Some(FilesystemPolicy::deny_all()),
        network: NetworkMode::Disabled,
        limits: ResourceLimits::default(),
        ..SandboxPolicy::default()
    },
};
let result = execute_with_runner(&runner, &request, b"").await?;
assert!(result.success);
# Ok(())
# }
```

The runner rejects host networking, filesystem grants, environment variables,
working directories, and input or output beyond the request limits. Its own
configuration caps guest linear memory and Wasmi fuel. The request timeout
bounds how long the caller waits; Wasmi runs on a blocking worker, and a timed
out invocation can continue in that isolated worker until it consumes its fuel
budget. It cannot be force-stopped like a native child process.

This backend is intended for pure, one-shot computations. It does not provide
WASI, networking, filesystem access, process creation, persistent services,
MCP server supervision, PTYs, or native process-tree lifecycle control. Use
`NativeRunner` for workloads that need those process features.
