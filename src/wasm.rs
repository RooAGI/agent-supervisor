//! Opt-in WebAssembly runner for self-contained, one-shot tools.
//!
//! This backend uses a small guest ABI and intentionally exposes no WASI
//! imports. It is separate from native process supervision: it cannot start
//! child processes, host MCP servers, or PTY sessions.

use crate::{
    Enforcement, ExecutableIdentity, ExecutionOutput, ExecutionRequest, NetworkMode, ProcessRunner,
    SandboxError, SandboxErrorCode, SandboxPhase, TerminationReason,
};
use serde::Serialize;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use wasmi::{Config, Engine, Linker, Module, Store, StoreLimitsBuilder};

/// Resource limits for a WebAssembly module.
#[derive(Debug, Clone, Copy)]
pub struct WasmRunnerConfig {
    /// Maximum guest memory in bytes.
    pub max_memory_bytes: usize,
    /// Maximum Wasmi fuel consumed by one invocation.
    pub fuel_limit: u64,
}

impl Default for WasmRunnerConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024,
            fuel_limit: 20_000_000,
        }
    }
}

/// Executes a precompiled module using the Agent Supervisor guest ABI.
///
/// A module must export `memory`, `allocate(i32) -> i32`,
/// `execute(i32, i32) -> ()`, `get_result_ptr() -> i32`, and
/// `get_result_len() -> i32`. The module receives a JSON object containing
/// the request arguments and stdin bytes. Its result bytes become stdout.
/// Modules with imports are rejected because this runner provides no host
/// capabilities.
pub struct WasmRunner {
    engine: Arc<Engine>,
    module: Arc<Module>,
    module_name: PathBuf,
    config: WasmRunnerConfig,
}

impl WasmRunner {
    /// Validates and compiles module bytes before accepting requests.
    pub fn from_bytes(
        module_name: impl Into<PathBuf>,
        wasm_bytes: &[u8],
        config: WasmRunnerConfig,
    ) -> Result<Self, SandboxError> {
        if config.max_memory_bytes == 0 || config.fuel_limit == 0 {
            return Err(SandboxError::new(
                SandboxErrorCode::Unsupported,
                SandboxPhase::Validate,
                "WASM memory and fuel limits must be greater than zero",
            ));
        }

        let mut engine_config = Config::default();
        engine_config.consume_fuel(true);
        let engine = Engine::new(&engine_config);
        let module = Module::new(&engine, &mut &wasm_bytes[..]).map_err(|error| {
            SandboxError::new(
                SandboxErrorCode::InvalidExecutable,
                SandboxPhase::Validate,
                format!("invalid WebAssembly module: {error}"),
            )
        })?;

        if module.imports().next().is_some() {
            return Err(SandboxError::new(
                SandboxErrorCode::Unsupported,
                SandboxPhase::Validate,
                "WASM modules with imports are unsupported; this runner grants no host capabilities",
            ));
        }

        Ok(Self {
            engine: Arc::new(engine),
            module: Arc::new(module),
            module_name: module_name.into(),
            config,
        })
    }

    fn execute_module(
        &self,
        request: &ExecutionRequest,
        input: &[u8],
    ) -> Result<Vec<u8>, SandboxError> {
        let identity = ExecutableIdentity {
            requested_path: request.executable.clone(),
            canonical_path: None,
        };
        if request.executable != self.module_name {
            return Err(wasm_error(
                SandboxErrorCode::InvalidExecutable,
                SandboxPhase::Validate,
                "request executable does not match the module bound to this WasmRunner",
                &identity,
            ));
        }
        validate_request(request, input, &identity)?;

        let payload = InvocationPayload {
            args: &request.args,
            stdin: input,
        };
        let payload = serde_json::to_vec(&payload).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Validate,
                format!("failed to encode WASM input: {error}"),
                &identity,
            )
        })?;
        if payload.len() > request.policy.limits.input_bytes {
            return Err(wasm_error(
                SandboxErrorCode::InputTooLarge,
                SandboxPhase::Validate,
                "encoded WASM input exceeds the configured limit",
                &identity,
            ));
        }
        let payload_len = i32::try_from(payload.len()).map_err(|_| {
            wasm_error(
                SandboxErrorCode::InputTooLarge,
                SandboxPhase::Validate,
                "WASM input exceeds the ABI size limit",
                &identity,
            )
        })?;

        let memory_limit = request
            .policy
            .limits
            .memory_bytes
            .and_then(|limit| usize::try_from(limit).ok())
            .map_or(self.config.max_memory_bytes, |limit| {
                limit.min(self.config.max_memory_bytes)
            });
        if memory_limit == 0 {
            return Err(wasm_error(
                SandboxErrorCode::Unsupported,
                SandboxPhase::Validate,
                "WASM memory limit must be greater than zero",
                &identity,
            ));
        }

        let limits = StoreLimitsBuilder::new().memory_size(memory_limit).build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        store.set_fuel(self.config.fuel_limit).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Spawn,
                format!("failed to configure WASM fuel: {error}"),
                &identity,
            )
        })?;

        let linker = Linker::new(&self.engine);
        let instance = linker
            .instantiate_and_start(&mut store, &self.module)
            .map_err(|error| {
                wasm_error(
                    SandboxErrorCode::ExecutionFailed,
                    SandboxPhase::Spawn,
                    format!("failed to instantiate WASM module: {error}"),
                    &identity,
                )
            })?;

        let memory = instance.get_memory(&store, "memory").ok_or_else(|| {
            wasm_error(
                SandboxErrorCode::InvalidExecutable,
                SandboxPhase::Spawn,
                "WASM module does not export memory",
                &identity,
            )
        })?;
        let allocate = instance
            .get_typed_func::<i32, i32>(&store, "allocate")
            .map_err(|error| abi_error("allocate", error, &identity))?;
        let execute = instance
            .get_typed_func::<(i32, i32), ()>(&store, "execute")
            .map_err(|error| abi_error("execute", error, &identity))?;
        let result_ptr = instance
            .get_typed_func::<(), i32>(&store, "get_result_ptr")
            .map_err(|error| abi_error("get_result_ptr", error, &identity))?;
        let result_len = instance
            .get_typed_func::<(), i32>(&store, "get_result_len")
            .map_err(|error| abi_error("get_result_len", error, &identity))?;

        let ptr = allocate.call(&mut store, payload_len).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdin,
                format!("WASM allocate call failed: {error}"),
                &identity,
            )
        })?;
        let ptr = usize::try_from(ptr).map_err(|_| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdin,
                "WASM allocate returned a negative pointer",
                &identity,
            )
        })?;
        memory.write(&mut store, ptr, &payload).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdin,
                format!("failed to write WASM input: {error}"),
                &identity,
            )
        })?;

        execute
            .call(&mut store, (ptr as i32, payload_len))
            .map_err(|error| {
                wasm_error(
                    SandboxErrorCode::ExecutionFailed,
                    SandboxPhase::Wait,
                    format!("WASM execution failed: {error}"),
                    &identity,
                )
            })?;

        let ptr = result_ptr.call(&mut store, ()).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdout,
                format!("failed to read WASM result pointer: {error}"),
                &identity,
            )
        })?;
        let len = result_len.call(&mut store, ()).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdout,
                format!("failed to read WASM result length: {error}"),
                &identity,
            )
        })?;
        let ptr = usize::try_from(ptr).map_err(|_| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdout,
                "WASM result pointer is negative",
                &identity,
            )
        })?;
        let len = usize::try_from(len).map_err(|_| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdout,
                "WASM result length is negative",
                &identity,
            )
        })?;
        if len > request.policy.limits.output_bytes {
            return Err(wasm_error(
                SandboxErrorCode::OutputTooLarge,
                SandboxPhase::Stdout,
                "WASM result exceeds the configured output limit",
                &identity,
            ));
        }
        let mut output = vec![0; len];
        memory.read(&store, ptr, &mut output).map_err(|error| {
            wasm_error(
                SandboxErrorCode::ExecutionFailed,
                SandboxPhase::Stdout,
                format!("failed to read WASM result: {error}"),
                &identity,
            )
        })?;
        Ok(output)
    }
}

#[derive(Serialize)]
struct InvocationPayload<'a> {
    args: &'a [String],
    stdin: &'a [u8],
}

impl ProcessRunner for WasmRunner {
    fn execute<'a>(
        &'a self,
        request: &'a ExecutionRequest,
        input: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionOutput, SandboxError>> + 'a>> {
        Box::pin(async move {
            let runner = WasmRunner {
                engine: Arc::clone(&self.engine),
                module: Arc::clone(&self.module),
                module_name: self.module_name.clone(),
                config: self.config,
            };
            let request = request.clone();
            let input = input.to_vec();
            let identity = ExecutableIdentity {
                requested_path: request.executable.clone(),
                canonical_path: None,
            };
            let timeout = Duration::from_millis(request.policy.limits.timeout_ms);
            let task = tokio::task::spawn_blocking(move || runner.execute_module(&request, &input));
            let stdout = match tokio::time::timeout(timeout, task).await {
                Ok(Ok(Ok(stdout))) => stdout,
                Ok(Ok(Err(error))) => return Err(error),
                Ok(Err(error)) => {
                    return Err(wasm_error(
                        SandboxErrorCode::ExecutionFailed,
                        SandboxPhase::Wait,
                        format!("WASM worker failed: {error}"),
                        &identity,
                    ));
                }
                Err(_) => {
                    return Err(wasm_error(
                        SandboxErrorCode::TimedOut,
                        SandboxPhase::Wait,
                        "WASM invocation exceeded its deadline; the isolated worker may continue until its fuel limit is reached",
                        &identity,
                    ));
                }
            };
            Ok(ExecutionOutput {
                success: true,
                stdout,
                stderr: Vec::new(),
                enforcement: Enforcement::Enforced,
                executable: identity,
                termination: TerminationReason::Exited {
                    code: Some(0),
                    signal: None,
                },
            })
        })
    }
}

fn validate_request(
    request: &ExecutionRequest,
    input: &[u8],
    identity: &ExecutableIdentity,
) -> Result<(), SandboxError> {
    let fail = |message: &str| {
        wasm_error(
            SandboxErrorCode::Unsupported,
            SandboxPhase::Validate,
            message,
            identity,
        )
    };
    if request.working_directory.is_some() {
        return Err(fail("WASM execution does not use a working directory"));
    }
    if request.policy.network != NetworkMode::Disabled {
        return Err(fail("WASM execution requires NetworkMode::Disabled"));
    }
    if request
        .policy
        .filesystem
        .as_ref()
        .is_none_or(|policy| !policy.grants.is_empty())
    {
        return Err(fail(
            "WASM execution requires an explicit deny-all filesystem policy",
        ));
    }
    if !request.policy.environment.variables.is_empty()
        || !request.policy.environment.inherit.is_empty()
        || !request
            .policy
            .environment
            .executable_search_paths
            .is_empty()
    {
        return Err(fail("WASM execution does not expose environment variables"));
    }
    if request.policy.limits.cpu_quota_micros.is_some() {
        return Err(fail(
            "WASM execution does not apply native CPU quotas; use the fuel limit",
        ));
    }
    if input.len() > request.policy.limits.input_bytes {
        return Err(wasm_error(
            SandboxErrorCode::InputTooLarge,
            SandboxPhase::Validate,
            "execution input exceeds the configured limit",
            identity,
        ));
    }
    Ok(())
}

fn abi_error(
    error_name: &str,
    error: impl std::fmt::Display,
    identity: &ExecutableIdentity,
) -> SandboxError {
    wasm_error(
        SandboxErrorCode::InvalidExecutable,
        SandboxPhase::Spawn,
        format!("WASM module has an invalid {error_name} export: {error}"),
        identity,
    )
}

fn wasm_error(
    code: SandboxErrorCode,
    phase: SandboxPhase,
    message: impl Into<String>,
    identity: &ExecutableIdentity,
) -> SandboxError {
    SandboxError::new(code, phase, message).with_executable(identity.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execute_with_runner, EnforcementRequirement, EnvironmentPolicy, FilesystemPolicy,
        ResourceLimits, SandboxPolicy,
    };
    use std::collections::BTreeMap;

    const ECHO_MODULE: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $result_ptr (mut i32) (i32.const 0))
          (global $result_len (mut i32) (i32.const 0))
          (func (export "allocate") (param i32) (result i32) (i32.const 1024))
          (func (export "execute") (param $ptr i32) (param $len i32)
            local.get $ptr
            global.set $result_ptr
            local.get $len
            global.set $result_len)
          (func (export "get_result_ptr") (result i32)
            global.get $result_ptr)
          (func (export "get_result_len") (result i32)
            global.get $result_len))
    "#;

    fn make_request(module_name: &str) -> ExecutionRequest {
        ExecutionRequest {
            executable: PathBuf::from(module_name),
            args: Vec::new(),
            working_directory: None,
            policy: SandboxPolicy {
                environment: EnvironmentPolicy::default(),
                filesystem: Some(FilesystemPolicy::deny_all()),
                network: NetworkMode::Disabled,
                limits: ResourceLimits::default(),
                enforcement: EnforcementRequirement::Required,
            },
        }
    }

    fn runner(wat_source: &str, config: WasmRunnerConfig) -> WasmRunner {
        WasmRunner::from_bytes("test.wasm", &wat::parse_str(wat_source).unwrap(), config).unwrap()
    }

    #[tokio::test]
    async fn executes_abi_and_returns_serialized_arguments_and_stdin() {
        let runner = runner(ECHO_MODULE, WasmRunnerConfig::default());
        let mut request = make_request("test.wasm");
        request.args = vec!["alpha".into(), "beta".into()];
        let output = execute_with_runner(&runner, &request, &[1, 2, 255])
            .await
            .unwrap();

        assert_eq!(
            output.stdout,
            br#"{"args":["alpha","beta"],"stdin":[1,2,255]}"#
        );
        assert!(output.stderr.is_empty());
        assert!(output.success);
        assert_eq!(output.enforcement, Enforcement::Enforced);
        assert_eq!(output.executable.requested_path, PathBuf::from("test.wasm"));
        assert_eq!(output.executable.canonical_path, None);
        assert_eq!(
            output.termination,
            TerminationReason::Exited {
                code: Some(0),
                signal: None,
            }
        );
    }

    #[test]
    fn rejects_modules_with_host_imports() {
        let module = wat::parse_str(r#"(module (import "host" "clock" (func)))"#).unwrap();
        let error = WasmRunner::from_bytes("imports.wasm", &module, WasmRunnerConfig::default())
            .err()
            .unwrap();

        assert_eq!(error.code(), "unsupported");
        assert!(error.message().contains("host capabilities"));
    }

    #[test]
    fn rejects_invalid_module_bytes() {
        let error = WasmRunner::from_bytes("broken.wasm", b"not wasm", WasmRunnerConfig::default())
            .err()
            .unwrap();

        assert_eq!(error.code(), "invalid_executable");
    }

    #[test]
    fn rejects_zero_resource_limits() {
        let config = WasmRunnerConfig {
            max_memory_bytes: 0,
            fuel_limit: 1,
        };
        let error =
            WasmRunner::from_bytes("test.wasm", &wat::parse_str(ECHO_MODULE).unwrap(), config)
                .err()
                .unwrap();

        assert_eq!(error.code(), "unsupported");
    }

    #[test]
    fn rejects_requests_that_do_not_match_runner_authority() {
        let runner = runner(ECHO_MODULE, WasmRunnerConfig::default());
        let mut request = make_request("other.wasm");
        let error = runner.execute_module(&request, b"").unwrap_err();
        assert_eq!(error.code(), "invalid_executable");

        request.executable = PathBuf::from("test.wasm");
        request.policy.network = NetworkMode::Host;
        assert_eq!(
            runner.execute_module(&request, b"").unwrap_err().code(),
            "unsupported"
        );

        request.policy.network = NetworkMode::Disabled;
        request.policy.filesystem = None;
        assert_eq!(
            runner.execute_module(&request, b"").unwrap_err().code(),
            "unsupported"
        );

        request.policy.filesystem = Some(FilesystemPolicy::deny_all());
        request.policy.environment.variables = BTreeMap::from([("TOKEN".into(), "secret".into())]);
        assert_eq!(
            runner.execute_module(&request, b"").unwrap_err().code(),
            "unsupported"
        );

        request.policy.environment = EnvironmentPolicy::default();
        request.policy.limits.cpu_quota_micros = Some(1000);
        assert_eq!(
            runner.execute_module(&request, b"").unwrap_err().code(),
            "unsupported"
        );
    }

    #[test]
    fn enforces_input_and_output_limits() {
        let runner = runner(ECHO_MODULE, WasmRunnerConfig::default());
        let mut request = make_request("test.wasm");
        request.policy.limits.input_bytes = 2;
        let error = runner.execute_module(&request, b"long").unwrap_err();
        assert_eq!(error.code(), "input_too_large");

        request.policy.limits.input_bytes = 1024;
        request.policy.limits.output_bytes = 1;
        let error = runner.execute_module(&request, b"").unwrap_err();
        assert_eq!(error.code(), "output_too_large");
    }

    #[test]
    fn enforces_guest_memory_limit() {
        let module = r#"
            (module
              (memory (export "memory") 2)
              (global $p (mut i32) (i32.const 0))
              (global $n (mut i32) (i32.const 0))
              (func (export "allocate") (param i32) (result i32) (i32.const 1024))
              (func (export "execute") (param i32 i32))
              (func (export "get_result_ptr") (result i32) global.get $p)
              (func (export "get_result_len") (result i32) global.get $n))
        "#;
        let runner = runner(
            module,
            WasmRunnerConfig {
                max_memory_bytes: 64 * 1024,
                fuel_limit: 1000,
            },
        );
        let error = runner
            .execute_module(&make_request("test.wasm"), b"")
            .unwrap_err();
        assert_eq!(error.code(), "execution_failed");
    }

    #[test]
    fn fuel_limit_stops_infinite_guest_loop() {
        let module = r#"
            (module
              (memory (export "memory") 1)
              (global $p (mut i32) (i32.const 0))
              (global $n (mut i32) (i32.const 0))
              (func (export "allocate") (param i32) (result i32) (i32.const 1024))
              (func (export "execute") (param i32 i32)
                (loop $forever (br $forever)))
              (func (export "get_result_ptr") (result i32) global.get $p)
              (func (export "get_result_len") (result i32) global.get $n))
        "#;
        let runner = runner(
            module,
            WasmRunnerConfig {
                max_memory_bytes: 64 * 1024,
                fuel_limit: 100,
            },
        );
        let error = runner
            .execute_module(&make_request("test.wasm"), b"")
            .unwrap_err();

        assert_eq!(error.code(), "execution_failed");
        assert!(error.message().contains("fuel"));
    }
}
