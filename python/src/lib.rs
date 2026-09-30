use agent_supervisor::{
    execute_with_cancellation, spawn, Enforcement, EnforcementRequirement, EnvironmentPolicy,
    ExecutionRequest, FilesystemAccess, FilesystemGrant, FilesystemPolicy, NetworkMode,
    PlatformCapabilities, ProcessEvent, ProcessEventStream, ProcessReceipt, ResourceLimits,
    SandboxError, SandboxPolicy, TerminationReason,
};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken as RustCancellationToken;

create_exception!(_native, SupervisorError, PyException);

#[pyclass(module = "agent_supervisor._native", frozen, from_py_object)]
#[derive(Clone)]
struct Command {
    argv: Vec<String>,
    cwd: Option<PathBuf>,
    environment: BTreeMap<String, String>,
    inherit_environment: BTreeSet<String>,
    timeout_ms: u64,
    input_bytes: usize,
    stdout_bytes: usize,
    stderr_bytes: usize,
}

#[pymethods]
impl Command {
    #[new]
    #[pyo3(signature = (argv, *, cwd=None, env=None, inherit_env=None, timeout=5.0, max_stdin_bytes=65536, max_stdout_bytes=65536, max_stderr_bytes=16384))]
    fn new(
        argv: Vec<String>,
        cwd: Option<PathBuf>,
        env: Option<HashMap<String, String>>,
        inherit_env: Option<Vec<String>>,
        timeout: f64,
        max_stdin_bytes: usize,
        max_stdout_bytes: usize,
        max_stderr_bytes: usize,
    ) -> PyResult<Self> {
        if argv.is_empty() || argv[0].is_empty() {
            return Err(PyValueError::new_err("argv must contain an executable"));
        }
        if !timeout.is_finite() || timeout <= 0.0 || timeout * 1000.0 > u64::MAX as f64 {
            return Err(PyValueError::new_err(
                "timeout must be a finite positive number of seconds",
            ));
        }
        if max_stdin_bytes == 0 || max_stdout_bytes == 0 || max_stderr_bytes == 0 {
            return Err(PyValueError::new_err(
                "byte limits must be greater than zero",
            ));
        }
        Ok(Self {
            argv,
            cwd,
            environment: env.unwrap_or_default().into_iter().collect(),
            inherit_environment: inherit_env.unwrap_or_default().into_iter().collect(),
            timeout_ms: (timeout * 1000.0).ceil() as u64,
            input_bytes: max_stdin_bytes,
            stdout_bytes: max_stdout_bytes,
            stderr_bytes: max_stderr_bytes,
        })
    }

    #[getter]
    fn argv(&self) -> Vec<String> {
        self.argv.clone()
    }

    #[getter]
    fn cwd(&self) -> Option<PathBuf> {
        self.cwd.clone()
    }
}

#[pyclass(module = "agent_supervisor._native", frozen, from_py_object)]
#[derive(Clone)]
struct Policy {
    network: NetworkMode,
    enforcement: EnforcementRequirement,
    filesystem: Option<FilesystemPolicy>,
}

#[pymethods]
impl Policy {
    #[new]
    #[pyo3(signature = (*, network="host", enforcement="best_effort", filesystem=None))]
    fn new(
        network: &str,
        enforcement: &str,
        filesystem: Option<Vec<(PathBuf, Vec<String>)>>,
    ) -> PyResult<Self> {
        let network = match network {
            "host" => NetworkMode::Host,
            "disabled" => NetworkMode::Disabled,
            _ => {
                return Err(PyValueError::new_err(
                    "network must be 'host' or 'disabled'",
                ))
            }
        };
        let enforcement = match enforcement {
            "best_effort" => EnforcementRequirement::BestEffort,
            "required" => EnforcementRequirement::Required,
            _ => {
                return Err(PyValueError::new_err(
                    "enforcement must be 'best_effort' or 'required'",
                ));
            }
        };
        let filesystem = filesystem
            .map(|grants| {
                grants
                    .into_iter()
                    .map(|(root, access)| {
                        let access = access
                            .iter()
                            .map(|value| match value.as_str() {
                                "read" => Ok(FilesystemAccess::Read),
                                "write" => Ok(FilesystemAccess::Write),
                                "append" => Ok(FilesystemAccess::Append),
                                _ => Err(PyValueError::new_err(format!(
                                    "unknown filesystem access {value:?}; use read, write, or append"
                                ))),
                            })
                            .collect::<PyResult<Vec<_>>>()?;
                        Ok(FilesystemGrant { root, access })
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?
            .map(FilesystemPolicy::new);
        Ok(Self {
            network,
            enforcement,
            filesystem,
        })
    }
}

#[pyclass(module = "agent_supervisor._native", frozen, from_py_object)]
#[derive(Clone)]
struct RunResult {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    success: bool,
    termination: String,
    exit_code: Option<i32>,
    signal: Option<i32>,
    enforcement: String,
    requested_executable: String,
    canonical_executable: Option<String>,
}

#[pymethods]
impl RunResult {
    #[getter]
    fn stdout<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.stdout)
    }

    #[getter]
    fn stderr<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.stderr)
    }

    #[getter]
    fn success(&self) -> bool {
        self.success
    }

    #[getter]
    fn termination(&self) -> &str {
        &self.termination
    }

    #[getter]
    fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }

    #[getter]
    fn signal(&self) -> Option<i32> {
        self.signal
    }

    #[getter]
    fn enforcement(&self) -> &str {
        &self.enforcement
    }

    #[getter]
    fn requested_executable(&self) -> &str {
        &self.requested_executable
    }

    #[getter]
    fn canonical_executable(&self) -> Option<&str> {
        self.canonical_executable.as_deref()
    }

    fn stdout_text(&self, encoding: &str, errors: &str) -> PyResult<String> {
        let codec = encoding.to_owned();
        let error_mode = errors.to_owned();
        Python::attach(|py| {
            let codecs = py.import("codecs")?;
            let decode = codecs.getattr("decode")?;
            decode
                .call1((PyBytes::new(py, &self.stdout), codec, error_mode))?
                .extract()
        })
    }

    fn stderr_text(&self, encoding: &str, errors: &str) -> PyResult<String> {
        let codec = encoding.to_owned();
        let error_mode = errors.to_owned();
        Python::attach(|py| {
            let codecs = py.import("codecs")?;
            let decode = codecs.getattr("decode")?;
            decode
                .call1((PyBytes::new(py, &self.stderr), codec, error_mode))?
                .extract()
        })
    }
}

#[pyclass(module = "agent_supervisor._native", frozen)]
struct OutputEvent {
    kind: String,
    data: Vec<u8>,
    result: Option<Py<RunResult>>,
}

#[pymethods]
impl OutputEvent {
    #[getter]
    fn kind(&self) -> &str {
        &self.kind
    }

    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    #[getter]
    fn result(&self, py: Python<'_>) -> Option<Py<RunResult>> {
        self.result.as_ref().map(|value| value.clone_ref(py))
    }
}

#[pyclass(module = "agent_supervisor._native")]
struct CancellationToken {
    token: RustCancellationToken,
}

#[pymethods]
impl CancellationToken {
    #[new]
    fn new() -> Self {
        Self {
            token: RustCancellationToken::new(),
        }
    }

    fn cancel(&self) {
        self.token.cancel();
    }

    fn cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

enum StreamItem {
    Event(ProcessEvent, Option<RunResult>),
    Error(SandboxError),
}

#[pyclass(module = "agent_supervisor._native")]
struct OutputStream {
    receiver: Arc<Mutex<tokio::sync::mpsc::Receiver<StreamItem>>>,
    cancellation: RustCancellationToken,
    completed: Arc<Mutex<Option<RunResult>>>,
}

#[pymethods]
impl OutputStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = slf.borrow(py).receiver.clone();
        let completed = slf.borrow(py).completed.clone();
        future_into_py(py, async move {
            let item = receiver.lock().await.recv().await;
            let Some(item) = item else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            match item {
                StreamItem::Error(error) => Err(to_py_error(error)),
                StreamItem::Event(event, result) => {
                    if let Some(value) = result.as_ref() {
                        *completed.lock().await = Some(value.clone());
                    }
                    Python::attach(|py| {
                        let (kind, data) = match event {
                            ProcessEvent::Started { .. } => ("started", Vec::new()),
                            ProcessEvent::Stdout(bytes) => ("stdout", bytes),
                            ProcessEvent::Stderr(bytes) => ("stderr", bytes),
                            ProcessEvent::Exited(_) => ("exited", Vec::new()),
                        };
                        let result = result.map(|value| Py::new(py, value)).transpose()?;
                        Py::new(
                            py,
                            OutputEvent {
                                kind: kind.into(),
                                data,
                                result,
                            },
                        )
                    })
                }
            }
        })
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.cancellation.cancel();
        let receiver = self.receiver.clone();
        let completed = self.completed.clone();
        future_into_py(py, async move {
            if let Some(result) = completed.lock().await.clone() {
                return Python::attach(|py| Py::new(py, result).map(Some));
            }
            let mut receiver = receiver.lock().await;
            loop {
                match receiver.recv().await {
                    Some(StreamItem::Event(ProcessEvent::Exited(_), Some(result))) => {
                        *completed.lock().await = Some(result.clone());
                        return Python::attach(|py| Py::new(py, result).map(Some));
                    }
                    Some(StreamItem::Error(error)) => return Err(to_py_error(error)),
                    Some(_) => continue,
                    None => return Ok(None::<Py<RunResult>>),
                }
            }
        })
    }
}

async fn drive_stream(
    stream: ProcessEventStream,
    sender: tokio::sync::mpsc::Sender<StreamItem>,
    cancellation: RustCancellationToken,
    done: RustCancellationToken,
    timeout: std::time::Duration,
    grace: std::time::Duration,
    capture: bool,
) {
    let mut stream = Some(stream);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    loop {
        let next = tokio::select! {
            _ = tokio::time::sleep_until(deadline) => None,
            _ = cancellation.cancelled() => None,
            _ = sender.closed() => None,
            event = stream.as_mut().expect("stream remains active").next() => Some(event),
        };
        let Some(next) = next else {
            let reason = if tokio::time::Instant::now() >= deadline {
                Some(TerminationReason::TimedOut)
            } else if cancellation.is_cancelled() {
                Some(TerminationReason::Cancelled)
            } else {
                None
            };
            if let Some(reason) = reason {
                finish_stream(
                    stream.take().expect("stream remains active"),
                    &sender,
                    reason,
                    grace,
                    capture,
                    &mut stdout,
                    &mut stderr,
                )
                .await;
            } else if let Some(stream) = stream.take() {
                let _ = stream.shutdown(grace).await;
            }
            break;
        };
        let event = match next {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(error) => {
                let _ = sender.send(StreamItem::Error(error)).await;
                break;
            }
        };
        match &event {
            ProcessEvent::Stdout(bytes) if capture => stdout.extend_from_slice(bytes),
            ProcessEvent::Stderr(bytes) if capture => stderr.extend_from_slice(bytes),
            _ => {}
        }
        if let ProcessEvent::Exited(receipt) = &event {
            let result = RunResult::from_receipt(receipt.clone(), stdout, stderr);
            let _ = sender.send(StreamItem::Event(event, Some(result))).await;
            break;
        }
        let send = tokio::select! {
            _ = tokio::time::sleep_until(deadline) => SendState::TimedOut,
            _ = cancellation.cancelled() => SendState::Cancelled,
            _ = sender.closed() => SendState::Closed,
            result = sender.send(StreamItem::Event(event, None)) => {
                if result.is_ok() { SendState::Sent } else { SendState::Closed }
            }
        };
        match send {
            SendState::Sent => {}
            SendState::TimedOut => {
                finish_stream(
                    stream.take().expect("stream remains active"),
                    &sender,
                    TerminationReason::TimedOut,
                    grace,
                    capture,
                    &mut stdout,
                    &mut stderr,
                )
                .await;
                break;
            }
            SendState::Cancelled => {
                finish_stream(
                    stream.take().expect("stream remains active"),
                    &sender,
                    TerminationReason::Cancelled,
                    grace,
                    capture,
                    &mut stdout,
                    &mut stderr,
                )
                .await;
                break;
            }
            SendState::Closed => {
                if let Some(stream) = stream.take() {
                    let _ = stream.shutdown(grace).await;
                }
                break;
            }
        }
    }
    done.cancel();
}

enum SendState {
    Sent,
    TimedOut,
    Cancelled,
    Closed,
}

async fn finish_stream(
    stream: ProcessEventStream,
    sender: &tokio::sync::mpsc::Sender<StreamItem>,
    termination: TerminationReason,
    grace: std::time::Duration,
    capture: bool,
    stdout: &mut Vec<u8>,
    stderr: &mut Vec<u8>,
) {
    if let Ok(mut receipt) = stream.shutdown(grace).await {
        receipt.termination = termination;
        let result = RunResult::from_receipt(
            receipt.clone(),
            if capture {
                std::mem::take(stdout)
            } else {
                Vec::new()
            },
            if capture {
                std::mem::take(stderr)
            } else {
                Vec::new()
            },
        );
        let _ = sender
            .send(StreamItem::Event(
                ProcessEvent::Exited(receipt),
                Some(result),
            ))
            .await;
    }
}

struct CancelOperationOnDrop {
    token: RustCancellationToken,
    armed: bool,
}

impl Drop for CancelOperationOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.token.cancel();
        }
    }
}

impl RunResult {
    fn from_output(output: agent_supervisor::ExecutionOutput) -> Self {
        let (termination, exit_code, signal) = termination_fields(&output.termination);
        Self {
            stdout: output.stdout,
            stderr: output.stderr,
            success: output.success,
            termination,
            exit_code,
            signal,
            enforcement: enforcement_name(output.enforcement).into(),
            requested_executable: output.executable.requested_path.display().to_string(),
            canonical_executable: output
                .executable
                .canonical_path
                .map(|path| path.display().to_string()),
        }
    }

    fn from_receipt(receipt: ProcessReceipt, stdout: Vec<u8>, stderr: Vec<u8>) -> Self {
        let (termination, exit_code, signal) = termination_fields(&receipt.termination);
        Self {
            stdout,
            stderr,
            success: termination == "exited" && exit_code == Some(0),
            termination,
            exit_code,
            signal,
            enforcement: enforcement_name(receipt.enforcement).into(),
            requested_executable: receipt.executable.requested_path.display().to_string(),
            canonical_executable: receipt
                .executable
                .canonical_path
                .map(|path| path.display().to_string()),
        }
    }
}

fn termination_fields(value: &TerminationReason) -> (String, Option<i32>, Option<i32>) {
    match value {
        TerminationReason::Exited { code, signal } => ("exited".into(), *code, *signal),
        TerminationReason::TimedOut => ("timed_out".into(), None, None),
        TerminationReason::Cancelled => ("cancelled".into(), None, None),
        TerminationReason::StdoutLimitExceeded => ("stdout_limit_exceeded".into(), None, None),
        TerminationReason::StderrLimitExceeded => ("stderr_limit_exceeded".into(), None, None),
    }
}

fn enforcement_name(value: Enforcement) -> &'static str {
    match value {
        Enforcement::Enforced => "enforced",
        Enforcement::Degraded => "degraded",
        Enforcement::Unavailable => "unavailable",
        Enforcement::Trusted => "trusted",
    }
}

fn request_from(command: &Command, policy: Option<&Policy>) -> ExecutionRequest {
    let defaults = Policy {
        network: NetworkMode::Host,
        enforcement: EnforcementRequirement::BestEffort,
        filesystem: None,
    };
    let policy = policy.unwrap_or(&defaults);
    let mut environment = EnvironmentPolicy::default();
    environment.variables = command.environment.clone();
    environment.inherit = command.inherit_environment.clone();
    ExecutionRequest {
        executable: PathBuf::from(&command.argv[0]),
        args: command.argv[1..].to_vec(),
        working_directory: command.cwd.clone(),
        policy: SandboxPolicy {
            environment,
            filesystem: policy.filesystem.clone(),
            network: policy.network,
            limits: ResourceLimits {
                timeout_ms: command.timeout_ms,
                input_bytes: command.input_bytes,
                output_bytes: command.stdout_bytes,
                stderr_bytes: command.stderr_bytes,
                memory_bytes: None,
                max_processes: None,
                cpu_quota_micros: None,
            },
            enforcement: policy.enforcement,
        },
    }
}

fn to_py_error(error: SandboxError) -> PyErr {
    let code = error.code().to_owned();
    let phase = format!("{:?}", error.phase()).to_lowercase();
    let retryable = error.retryable();
    let message = error.message().to_owned();
    let exception = SupervisorError::new_err(message);
    Python::attach(|py| {
        let value = exception.value(py);
        let _ = value.setattr("code", code);
        let _ = value.setattr("phase", phase);
        let _ = value.setattr("retryable", retryable);
        if let Some(identity) = error.executable() {
            let _ = value.setattr("executable", identity.requested_path.display().to_string());
        }
        if let Some(termination) = error.termination() {
            let (termination, _, _) = termination_fields(termination);
            let _ = value.setattr("termination", termination);
        }
        if let Some(stderr) = error.stderr_tail() {
            let _ = value.setattr("stderr_tail", stderr);
        }
    });
    exception
}

#[pyfunction]
#[pyo3(signature = (command, *, policy=None, input=None, cancellation=None))]
fn run<'py>(
    py: Python<'py>,
    command: PyRef<'py, Command>,
    policy: Option<PyRef<'py, Policy>>,
    input: Option<&Bound<'py, PyBytes>>,
    cancellation: Option<PyRef<'py, CancellationToken>>,
) -> PyResult<Bound<'py, PyAny>> {
    let command = command.clone();
    let policy = policy.map(|value| value.clone());
    let request = request_from(&command, policy.as_ref());
    let input = input
        .map(|value| value.as_bytes().to_vec())
        .unwrap_or_default();
    let caller_token = cancellation
        .map(|value| value.token.clone())
        .unwrap_or_default();
    future_into_py(py, async move {
        let worker_token = caller_token.clone();
        let worker =
            tokio::spawn(
                async move { execute_with_cancellation(&request, &input, worker_token).await },
            );
        let mut cancel_on_drop = CancelOperationOnDrop {
            token: caller_token,
            armed: true,
        };
        let output = worker
            .await
            .map_err(|error| PyRuntimeError::new_err(format!("execution worker failed: {error}")))?
            .map_err(to_py_error)?;
        cancel_on_drop.armed = false;
        Python::attach(|py| Py::new(py, RunResult::from_output(output)))
    })
}

#[pyfunction]
#[pyo3(signature = (command, *, policy=None, cancellation=None, capture=false))]
fn stream<'py>(
    py: Python<'py>,
    command: PyRef<'py, Command>,
    policy: Option<PyRef<'py, Policy>>,
    cancellation: Option<PyRef<'py, CancellationToken>>,
    capture: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let command = command.clone();
    let policy = policy.map(|value| value.clone());
    let request = request_from(&command, policy.as_ref());
    let caller_token = cancellation
        .map(|value| value.token.clone())
        .unwrap_or_default();
    future_into_py(py, async move {
        let mut child = spawn(&request).map_err(to_py_error)?;
        // This output-streaming helper is for commands that do not need stdin.
        // Dropping the writer sends EOF so commands waiting for input cannot
        // hang indefinitely.
        drop(child.take_stdin());
        let event_stream = child.into_event_stream().map_err(to_py_error)?;
        let token = RustCancellationToken::new();
        let done = RustCancellationToken::new();
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        tokio::spawn(drive_stream(
            event_stream,
            sender,
            token.clone(),
            done.clone(),
            std::time::Duration::from_millis(command.timeout_ms),
            std::time::Duration::from_millis(250),
            capture,
        ));
        if caller_token.is_cancelled() {
            token.cancel();
        } else {
            let caller_token = caller_token.clone();
            let operation_token = token.clone();
            let done_token = done.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = caller_token.cancelled() => operation_token.cancel(),
                    _ = done_token.cancelled() => {},
                }
            });
        }
        Python::attach(|py| {
            Py::new(
                py,
                OutputStream {
                    receiver: Arc::new(Mutex::new(receiver)),
                    cancellation: token,
                    completed: Arc::new(Mutex::new(None)),
                },
            )
        })
    })
}

#[pyfunction]
fn platform_capabilities(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let caps: PlatformCapabilities = agent_supervisor::platform_capabilities();
    let result = PyDict::new(py);
    result.set_item("containment", enforcement_name(caps.containment))?;
    result.set_item("host_network", caps.host_network)?;
    result.set_item("network_isolation", caps.network_isolation)?;
    result.set_item(
        "network_isolation_requires_filesystem",
        caps.network_isolation_requires_filesystem,
    )?;
    result.set_item("graceful_shutdown", caps.graceful_shutdown)?;
    result.set_item("process_group_cleanup", caps.process_group_cleanup)?;
    result.set_item("parent_death_cleanup", caps.parent_death_cleanup)?;
    result.set_item("filesystem_isolation", caps.filesystem_isolation)?;
    result.set_item("memory_limits", caps.memory_limits)?;
    result.set_item("process_limits", caps.process_limits)?;
    result.set_item("cpu_limits", caps.cpu_limits)?;
    Ok(result.into())
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("SupervisorError", module.py().get_type::<SupervisorError>())?;
    module.add_class::<Command>()?;
    module.add_class::<Policy>()?;
    module.add_class::<RunResult>()?;
    module.add_class::<OutputEvent>()?;
    module.add_class::<OutputStream>()?;
    module.add_class::<CancellationToken>()?;
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(stream, module)?)?;
    module.add_function(wrap_pyfunction!(platform_capabilities, module)?)?;
    Ok(())
}
