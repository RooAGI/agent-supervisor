mod mcp_stdio;

use agent_supervisor::{
    execute_with_cancellation, spawn_with_supervisor, Enforcement, EnforcementRequirement,
    EnvironmentPolicy, EventStream as CoreEventStream, ExecutionRequest, FilesystemAccess,
    FilesystemGrant, FilesystemPolicy, ManagedProcessResult, ManagedProcessStatus, NetworkMode,
    PlatformCapabilities, ProcessEvent, ProcessEventStream, ProcessReceipt, ProcessStopResult,
    ResourceLimits, SandboxError, SandboxPolicy, StreamScope, Supervisor as RustSupervisor,
    SupervisorEvent as CoreSupervisorEvent, SupervisorEventKind,
    SupervisorProcessId as RustSupervisorProcessId, TerminationReason,
};
use pyo3::class::basic::CompareOp;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
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
    // Mirrors the keyword-only Python constructor, so the arguments cannot be grouped.
    #[allow(clippy::too_many_arguments)]
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
    error: Option<String>,
    output_truncated: bool,
}

#[pymethods]
impl RunResult {
    /// True when a background job wrote more than its limits, so ``stdout`` or
    /// ``stderr`` holds only the newest bytes.
    #[getter]
    fn output_truncated(&self) -> bool {
        self.output_truncated
    }

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

    #[getter]
    fn error(&self) -> Option<&str> {
        self.error.as_deref()
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

#[pyclass(module = "agent_supervisor._native", frozen, from_py_object)]
#[derive(Clone)]
struct SupervisorProcessId {
    inner: RustSupervisorProcessId,
}

#[pymethods]
impl SupervisorProcessId {
    /// Versioned, lossless identifier suitable for JSON or checkpoint storage.
    #[getter]
    fn token(&self) -> String {
        self.inner.to_string()
    }

    /// Parse a token previously returned by ``token``.
    #[staticmethod]
    fn from_token(token: &str) -> PyResult<Self> {
        token
            .parse::<RustSupervisorProcessId>()
            .map(|inner| Self { inner })
            .map_err(PyValueError::new_err)
    }

    #[getter]
    fn value(&self) -> u64 {
        self.inner.get()
    }

    fn __int__(&self) -> u64 {
        self.inner.get()
    }

    fn __repr__(&self) -> String {
        format!("SupervisorProcessId('{}')", self.inner)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        op.matches(self.inner.cmp(&other.inner))
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut hasher);
        hasher.finish()
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

/// One entry from a supervisor's event log.
#[pyclass(module = "agent_supervisor._native", frozen)]
struct SupervisorEvent {
    seq: u64,
    kind: &'static str,
    run_id: Option<u64>,
    process_id: Option<RustSupervisorProcessId>,
    data: Vec<u8>,
    payload: Option<Py<PyAny>>,
    result: Option<Py<RunResult>>,
    dropped: u64,
}

#[pymethods]
impl SupervisorEvent {
    #[getter]
    fn seq(&self) -> u64 {
        self.seq
    }

    #[getter]
    fn kind(&self) -> &str {
        self.kind
    }

    #[getter]
    fn run_id(&self) -> Option<u64> {
        self.run_id
    }

    #[getter]
    fn process_id(&self) -> Option<SupervisorProcessId> {
        self.process_id.map(|inner| SupervisorProcessId { inner })
    }

    /// Bytes for ``stdout`` and ``stderr`` events.
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    /// The framework's own event object for ``agent`` events.
    #[getter]
    fn payload(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.payload.as_ref().map(|value| value.clone_ref(py))
    }

    /// The final result for ``exited`` events, when the exit was observed.
    /// Output is not included; read it from the ``stdout`` and ``stderr``
    /// events or from the process's own result.
    #[getter]
    fn result(&self, py: Python<'_>) -> Option<Py<RunResult>> {
        self.result.as_ref().map(|value| value.clone_ref(py))
    }

    /// How many log entries were lost, for ``lagged`` events.
    #[getter]
    fn dropped(&self) -> u64 {
        self.dropped
    }
}

fn event_to_python(py: Python<'_>, event: CoreSupervisorEvent) -> PyResult<Py<SupervisorEvent>> {
    let mut value = SupervisorEvent {
        seq: event.seq,
        kind: "",
        run_id: event.run_id,
        process_id: event.process_id,
        data: Vec::new(),
        payload: None,
        result: None,
        dropped: 0,
    };
    match event.kind {
        SupervisorEventKind::RunStarted => value.kind = "run_started",
        SupervisorEventKind::RunEnded => value.kind = "run_ended",
        SupervisorEventKind::ProcessStarted { .. } => value.kind = "started",
        SupervisorEventKind::Stdout(bytes) => {
            value.kind = "stdout";
            value.data = bytes;
        }
        SupervisorEventKind::Stderr(bytes) => {
            value.kind = "stderr";
            value.data = bytes;
        }
        SupervisorEventKind::ProcessExited(receipt) => {
            value.kind = "exited";
            value.result = receipt
                .map(|receipt| {
                    Py::new(py, RunResult::from_receipt(receipt, Vec::new(), Vec::new()))
                })
                .transpose()?;
        }
        SupervisorEventKind::Agent(payload) => {
            value.kind = "agent";
            value.payload = payload
                .downcast_ref::<Py<PyAny>>()
                .map(|object| object.clone_ref(py));
        }
        SupervisorEventKind::Lagged { dropped } => {
            value.kind = "lagged";
            value.dropped = dropped;
        }
    }
    Py::new(py, value)
}

/// A watcher from ``Supervisor.stream()``. Stopping it stops nothing.
#[pyclass(module = "agent_supervisor._native")]
struct EventStream {
    inner: Arc<Mutex<CoreEventStream>>,
}

#[pymethods]
impl EventStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let event = inner.lock().await.next().await;
            let Some(event) = event else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            Python::attach(|py| event_to_python(py, event))
        })
    }
}

#[pyclass(module = "agent_supervisor._native")]
struct Process {
    events: Arc<Mutex<ProcessEventStream>>,
    completed: Arc<Mutex<Option<RunResult>>>,
    output: Arc<Mutex<(Vec<u8>, Vec<u8>)>>,
    _supervisor: Arc<RustSupervisor>,
}

#[pymethods]
impl Process {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn events(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = slf.borrow(py).events.clone();
        let completed = slf.borrow(py).completed.clone();
        let output = slf.borrow(py).output.clone();
        future_into_py(py, async move {
            let event = events.lock().await.next().await.map_err(to_py_error)?;
            let Some(event) = event else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            let (kind, data, result) = match event {
                ProcessEvent::Started { .. } => ("started", Vec::new(), None),
                ProcessEvent::Stdout(data) => {
                    output.lock().await.0.extend_from_slice(&data);
                    ("stdout", data, None)
                }
                ProcessEvent::Stderr(data) => {
                    output.lock().await.1.extend_from_slice(&data);
                    ("stderr", data, None)
                }
                ProcessEvent::Exited(receipt) => {
                    let (stdout, stderr) = {
                        let output = output.lock().await;
                        (output.0.clone(), output.1.clone())
                    };
                    let result = RunResult::from_receipt(receipt, stdout, stderr);
                    *completed.lock().await = Some(result.clone());
                    ("exited", Vec::new(), Some(result))
                }
            };
            Python::attach(|py| {
                Py::new(
                    py,
                    OutputEvent {
                        kind: kind.into(),
                        data,
                        result: result.map(|r| Py::new(py, r)).transpose()?,
                    },
                )
            })
        })
    }

    fn write<'py>(
        &self,
        py: Python<'py>,
        data: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let events = self.events.clone();
        let bytes = data.as_bytes().to_vec();
        future_into_py(py, async move {
            events.lock().await.write(&bytes).await.map_err(to_py_error)
        })
    }

    fn close_stdin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = self.events.clone();
        future_into_py(py, async move {
            events.lock().await.close_stdin().await.map_err(to_py_error)
        })
    }

    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let events = self.events.clone();
        let completed = self.completed.clone();
        let output = self.output.clone();
        future_into_py(py, async move {
            if let Some(result) = completed.lock().await.clone() {
                return Python::attach(|py| Py::new(py, result));
            }
            loop {
                match events.lock().await.next().await.map_err(to_py_error)? {
                    Some(ProcessEvent::Stdout(bytes)) => {
                        output.lock().await.0.extend_from_slice(&bytes);
                    }
                    Some(ProcessEvent::Stderr(bytes)) => {
                        output.lock().await.1.extend_from_slice(&bytes);
                    }
                    Some(ProcessEvent::Exited(receipt)) => {
                        let (stdout, stderr) = {
                            let output = output.lock().await;
                            (output.0.clone(), output.1.clone())
                        };
                        let result = RunResult::from_receipt(receipt, stdout, stderr);
                        *completed.lock().await = Some(result.clone());
                        return Python::attach(|py| Py::new(py, result));
                    }
                    Some(_) => continue,
                    None => return Err(PyRuntimeError::new_err("process event stream ended")),
                }
            }
        })
    }

    fn shutdown<'py>(&self, py: Python<'py>, grace: f64) -> PyResult<Bound<'py, PyAny>> {
        if !grace.is_finite() || !(0.0..=60.0).contains(&grace) {
            return Err(PyValueError::new_err(
                "grace must be between 0 and 60 seconds",
            ));
        }
        let events = self.events.clone();
        let completed = self.completed.clone();
        let output = self.output.clone();
        future_into_py(py, async move {
            let receipt = events
                .lock()
                .await
                .shutdown(std::time::Duration::from_secs_f64(grace))
                .await
                .map_err(to_py_error)?;
            let (stdout, stderr) = {
                let output = output.lock().await;
                (output.0.clone(), output.1.clone())
            };
            let result = RunResult::from_receipt(receipt, stdout, stderr);
            *completed.lock().await = Some(result.clone());
            Python::attach(|py| Py::new(py, result))
        })
    }
}

#[pyclass(module = "agent_supervisor._native")]
struct SupervisorHandle {
    inner: Arc<RustSupervisor>,
}

#[pyclass(module = "agent_supervisor._native")]
struct AgentRunHandle {
    inner: agent_supervisor::AgentRun,
    supervisor: Arc<RustSupervisor>,
}

#[pymethods]
impl AgentRunHandle {
    fn active_processes(&self) -> usize {
        self.inner.active_processes()
    }

    fn cancellation_token(&self, py: Python<'_>) -> PyResult<Py<CancellationToken>> {
        Py::new(
            py,
            CancellationToken {
                token: self.inner.cancellation_token(),
            },
        )
    }

    #[getter]
    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn publish_agent_event(&self, event: Py<PyAny>) {
        self.inner.publish_agent_event(Arc::new(event));
    }

    fn wait_cancelled<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let token = self.inner.cancellation_token();
        future_into_py(py, async move {
            token.cancelled().await;
            Ok(())
        })
    }

    fn cancel(&self) {
        self.inner.request_cancel();
    }

    fn cancel_now(&self) {
        self.inner.cancel_now();
    }

    fn wait_cleanup<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.cancel().await;
            Ok(())
        })
    }

    #[pyo3(signature = (command, *, policy=None, input=None))]
    fn run<'py>(
        &self,
        py: Python<'py>,
        command: PyRef<'py, Command>,
        policy: Option<PyRef<'py, Policy>>,
        input: Option<&Bound<'py, PyBytes>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = request_from(&command.clone(), policy.as_deref());
        let command = agent_supervisor::Command {
            argv: std::iter::once(request.executable.display().to_string())
                .chain(request.args.clone())
                .collect(),
            cwd: request.working_directory.clone(),
        };
        let policy = request.policy.clone();
        let input = input.map(|v| v.as_bytes().to_vec()).unwrap_or_default();
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let output = inner
                .run_command(&command, &policy, &input)
                .await
                .map_err(to_py_error)?;
            Python::attach(|py| Py::new(py, RunResult::from_output(output)))
        })
    }

    #[pyo3(signature = (command, *, policy=None))]
    fn start<'py>(
        &self,
        py: Python<'py>,
        command: PyRef<'py, Command>,
        policy: Option<PyRef<'py, Policy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = request_from(&command.clone(), policy.as_deref());
        let supervisor = self.supervisor.clone();
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let command = agent_supervisor::Command {
                argv: std::iter::once(request.executable.display().to_string())
                    .chain(request.args.clone())
                    .collect(),
                cwd: request.working_directory.clone(),
            };
            let child = inner
                .start(&command, &request.policy)
                .map_err(to_py_error)?;
            let events = child.into_event_stream().map_err(to_py_error)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    Process {
                        events: Arc::new(Mutex::new(events)),
                        completed: Arc::new(Mutex::new(None)),
                        output: Arc::new(Mutex::new((Vec::new(), Vec::new()))),
                        _supervisor: supervisor,
                    },
                )
            })
        })
    }
}

#[pymethods]
impl SupervisorHandle {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(RustSupervisor::new()),
        }
    }

    fn terminate_all(&self) {
        self.inner.terminate_all();
    }

    fn active_processes(&self) -> usize {
        self.inner.active_processes()
    }

    fn active_agent_runs(&self) -> usize {
        self.inner.active_agent_runs()
    }

    #[pyo3(signature = (timeout=300.0, shutdown_grace=5.0))]
    fn start_agent_run(
        &self,
        py: Python<'_>,
        timeout: f64,
        shutdown_grace: f64,
    ) -> PyResult<Py<AgentRunHandle>> {
        if !timeout.is_finite()
            || timeout <= 0.0
            || !shutdown_grace.is_finite()
            || !(0.0..=60.0).contains(&shutdown_grace)
        {
            return Err(PyValueError::new_err(
                "invalid run timeout or shutdown grace",
            ));
        }
        let inner = self
            .inner
            .start_agent_run(agent_supervisor::AgentRunPolicy {
                timeout: std::time::Duration::from_secs_f64(timeout),
                shutdown_grace: std::time::Duration::from_secs_f64(shutdown_grace),
            });
        Py::new(
            py,
            AgentRunHandle {
                supervisor: self.inner.clone(),
                inner,
            },
        )
    }

    #[pyo3(signature = (*, run=None, process=None))]
    fn stream(
        &self,
        run: Option<PyRef<'_, AgentRunHandle>>,
        process: Option<PyRef<'_, SupervisorProcessId>>,
    ) -> PyResult<EventStream> {
        let scope = match (&run, &process) {
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err("pass run or process, not both"));
            }
            (Some(run), None) => StreamScope::Run(&run.inner),
            (None, Some(process)) => StreamScope::Process(process.inner),
            (None, None) => StreamScope::All,
        };
        Ok(EventStream {
            inner: Arc::new(Mutex::new(self.inner.stream(scope))),
        })
    }

    #[pyo3(signature = (command, *, policy=None))]
    fn start_background<'py>(
        &self,
        py: Python<'py>,
        command: PyRef<'py, Command>,
        policy: Option<PyRef<'py, Policy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = request_from(&command.clone(), policy.as_deref());
        let command = agent_supervisor::Command {
            argv: std::iter::once(request.executable.display().to_string())
                .chain(request.args.clone())
                .collect(),
            cwd: request.working_directory.clone(),
        };
        let policy = request.policy;
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            let id = supervisor
                .start_background(&command, &policy)
                .map_err(to_py_error)?;
            Python::attach(|py| Py::new(py, SupervisorProcessId { inner: id }))
        })
    }

    fn process_status(&self, id: PyRef<'_, SupervisorProcessId>) -> Option<String> {
        self.inner.process_status(id.inner).map(|status| {
            match status {
                ManagedProcessStatus::Running => "running",
                ManagedProcessStatus::Completed => "completed",
                ManagedProcessStatus::Failed => "failed",
            }
            .to_owned()
        })
    }

    fn process_result(
        &self,
        py: Python<'_>,
        id: PyRef<'_, SupervisorProcessId>,
    ) -> PyResult<Option<Py<RunResult>>> {
        self.inner
            .process_result(id.inner)
            .map(|result| Py::new(py, managed_result_to_python(result)))
            .transpose()
    }

    fn wait_process<'py>(
        &self,
        py: Python<'py>,
        id: PyRef<'py, SupervisorProcessId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = id.inner;
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            let result = supervisor.wait_process(id).await.map_err(to_py_error)?;
            Python::attach(|py| {
                result
                    .map(|result| Py::new(py, managed_result_to_python(result)))
                    .transpose()
            })
        })
    }

    fn take_process_result(
        &self,
        py: Python<'_>,
        id: PyRef<'_, SupervisorProcessId>,
    ) -> PyResult<Option<Py<RunResult>>> {
        self.inner
            .take_process_result(id.inner)
            .map(|result| Py::new(py, managed_result_to_python(result)))
            .transpose()
    }

    fn forget_process(&self, id: PyRef<'_, SupervisorProcessId>) -> bool {
        self.inner.forget_process(id.inner)
    }

    fn write_process_stdin<'py>(
        &self,
        py: Python<'py>,
        id: PyRef<'py, SupervisorProcessId>,
        data: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = id.inner;
        let bytes = data.as_bytes().to_vec();
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            supervisor
                .write_process_stdin(id, &bytes)
                .await
                .map_err(to_py_error)
        })
    }

    fn close_process_stdin<'py>(
        &self,
        py: Python<'py>,
        id: PyRef<'py, SupervisorProcessId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = id.inner;
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            supervisor
                .close_process_stdin(id)
                .await
                .map_err(to_py_error)
        })
    }

    #[pyo3(signature = (id, *, grace=5.0))]
    fn shutdown_process<'py>(
        &self,
        py: Python<'py>,
        id: PyRef<'py, SupervisorProcessId>,
        grace: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let grace = checked_grace(grace)?;
        let id = id.inner;
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            let result = supervisor
                .shutdown_process(id, grace)
                .await
                .map_err(to_py_error)?;
            Ok(stop_result_name(result).to_owned())
        })
    }

    #[pyo3(signature = (*, grace=5.0))]
    fn shutdown_all<'py>(&self, py: Python<'py>, grace: f64) -> PyResult<Bound<'py, PyAny>> {
        let grace = checked_grace(grace)?;
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            let results = supervisor.shutdown_all(grace).await.map_err(to_py_error)?;
            Python::attach(|py| {
                let values = PyList::empty(py);
                for (id, result) in results {
                    values.append((
                        Py::new(py, SupervisorProcessId { inner: id })?,
                        Py::new(py, managed_result_to_python(result))?,
                    ))?;
                }
                Ok(values.unbind().into_any())
            })
        })
    }

    #[pyo3(signature = (command, *, policy=None))]
    fn start<'py>(
        &self,
        py: Python<'py>,
        command: PyRef<'py, Command>,
        policy: Option<PyRef<'py, Policy>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = request_from(&command.clone(), policy.as_deref());
        let supervisor = self.inner.clone();
        future_into_py(py, async move {
            let child = spawn_with_supervisor(&request, &supervisor).map_err(to_py_error)?;
            let events = child.into_event_stream().map_err(to_py_error)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    Process {
                        events: Arc::new(Mutex::new(events)),
                        completed: Arc::new(Mutex::new(None)),
                        output: Arc::new(Mutex::new((Vec::new(), Vec::new()))),
                        _supervisor: supervisor,
                    },
                )
            })
        })
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
            error: None,
            output_truncated: false,
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
            error: None,
            output_truncated: false,
        }
    }
}

fn managed_result_to_python(result: ManagedProcessResult) -> RunResult {
    let mut value = RunResult::from_receipt(result.receipt, result.stdout, result.stderr);
    value.error = result.error;
    value.output_truncated = result.output_truncated;
    value
}

fn checked_grace(grace: f64) -> PyResult<std::time::Duration> {
    if !grace.is_finite() || !(0.0..=60.0).contains(&grace) {
        return Err(PyValueError::new_err(
            "grace must be between 0 and 60 seconds",
        ));
    }
    Ok(std::time::Duration::from_secs_f64(grace))
}

fn stop_result_name(result: ProcessStopResult) -> &'static str {
    match result {
        ProcessStopResult::GracefulStopRequested => "graceful_stop_requested",
        ProcessStopResult::ForceStopRequested => "force_stop_requested",
        ProcessStopResult::AlreadyStopping => "already_stopping",
        ProcessStopResult::AlreadyExited => "already_exited",
        ProcessStopResult::NotFound => "not_found",
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
    module.add_class::<SupervisorProcessId>()?;
    module.add_class::<OutputEvent>()?;
    module.add_class::<SupervisorEvent>()?;
    module.add_class::<EventStream>()?;
    module.add_class::<Process>()?;
    module.add_class::<SupervisorHandle>()?;
    module.add_class::<AgentRunHandle>()?;
    module.add_class::<CancellationToken>()?;
    module.add_function(wrap_pyfunction!(mcp_stdio::relay_stdio, module)?)?;
    module.add_function(wrap_pyfunction!(mcp_stdio::mcp_defaults, module)?)?;
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(platform_capabilities, module)?)?;
    Ok(())
}
