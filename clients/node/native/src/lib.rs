use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use agent_supervisor::{
    AgentEventPayload, AgentRun as RustAgentRun, AgentRunPolicy, Command, EventStream,
    ExecutionOutput, ExecutionRequest, Policy, ProcessEvent, ProcessEventStream, ProcessReceipt,
    StreamScope, Supervisor as RustSupervisor, SupervisorEvent, SupervisorEventKind,
    SupervisorProcessId,
};
use napi::{bindgen_prelude::*, Error, Result};
use napi_derive::napi;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken as RustCancellationToken;

#[napi(object)]
pub struct CommandInput {
    pub argv: Vec<String>,
    pub cwd: Option<String>,
}

#[napi(object)]
pub struct ProcessEventValue {
    pub kind: String,
    pub process_id: Option<u32>,
    pub data: Option<Buffer>,
    pub receipt: Option<Value>,
    pub executable: Option<Value>,
}

#[napi(object)]
pub struct ExecutionOutputValue {
    pub success: bool,
    pub stdout: Buffer,
    pub stderr: Buffer,
    pub enforcement: Value,
    pub executable: Value,
    pub termination: Value,
}

#[napi(object)]
pub struct SupervisorEventValue {
    pub sequence: String,
    pub run_id: Option<String>,
    pub process_id: Option<String>,
    pub kind: String,
    pub data: Option<Buffer>,
    pub receipt: Option<Value>,
    pub executable: Option<Value>,
    pub payload: Option<Value>,
    pub dropped: Option<String>,
}

#[napi]
pub struct CancellationToken {
    inner: RustCancellationToken,
}

#[napi]
impl CancellationToken {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: RustCancellationToken::new(),
        }
    }

    #[napi]
    pub fn cancel(&self) {
        self.inner.cancel();
    }

    #[napi(getter)]
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }
}

fn error(error: impl std::fmt::Display) -> Error {
    Error::from_reason(error.to_string())
}

fn parse_policy(value: Option<Value>) -> Result<Policy> {
    match value {
        Some(value) => serde_json::from_value(value).map_err(error),
        None => Ok(Policy::default()),
    }
}

fn parse_command(command: CommandInput) -> Result<Command> {
    if command.argv.is_empty() || command.argv[0].is_empty() {
        return Err(Error::from_reason(
            "command.argv must include an executable",
        ));
    }
    Ok(Command {
        argv: command.argv,
        cwd: command.cwd.map(Into::into),
    })
}

fn execution_value(value: ExecutionOutput) -> Result<ExecutionOutputValue> {
    Ok(ExecutionOutputValue {
        success: value.success,
        stdout: Buffer::from(value.stdout),
        stderr: Buffer::from(value.stderr),
        enforcement: serde_json::to_value(value.enforcement).map_err(error)?,
        executable: serde_json::to_value(value.executable).map_err(error)?,
        termination: serde_json::to_value(value.termination).map_err(error)?,
    })
}

fn receipt_value(value: ProcessReceipt) -> Result<Value> {
    serde_json::to_value(value).map_err(error)
}

#[napi]
pub struct Supervisor {
    inner: Arc<RustSupervisor>,
}

#[napi]
impl Supervisor {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RustSupervisor::new()),
        }
    }

    #[napi(getter)]
    pub fn active_processes(&self) -> u32 {
        self.inner.active_processes() as u32
    }

    #[napi(getter)]
    pub fn active_agent_runs(&self) -> u32 {
        self.inner.active_agent_runs() as u32
    }

    #[napi]
    pub fn start_agent_run(
        &self,
        timeout_ms: Option<u32>,
        shutdown_grace_ms: Option<u32>,
    ) -> Result<AgentRun> {
        let timeout = timeout_ms.unwrap_or(300_000);
        let grace = shutdown_grace_ms.unwrap_or(5_000);
        if timeout == 0 || grace > 60_000 {
            return Err(Error::from_reason(
                "timeoutMs must be positive and shutdownGraceMs must be 0..60000",
            ));
        }
        let inner = self.inner.start_agent_run(AgentRunPolicy {
            timeout: Duration::from_millis(timeout as u64),
            shutdown_grace: Duration::from_millis(grace as u64),
        });
        Ok(AgentRun {
            inner,
            owner: Arc::clone(&self.inner),
        })
    }

    #[napi]
    pub async fn run_command(
        &self,
        command: CommandInput,
        policy: Option<Value>,
        input: Option<Buffer>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ExecutionOutputValue> {
        let command = parse_command(command)?;
        let policy = parse_policy(policy)?;
        let mut argv = command.argv.into_iter();
        let executable = PathBuf::from(argv.next().expect("parse_command checked argv"));
        let request = ExecutionRequest {
            executable,
            args: argv.collect(),
            working_directory: command.cwd,
            policy,
        };
        let token = cancellation
            .map(|token| token.inner.clone())
            .unwrap_or_default();
        let output = agent_supervisor::execute_with_cancellation(
            &request,
            input.as_deref().unwrap_or_default(),
            token,
        )
        .await
        .map_err(error)?;
        execution_value(output)
    }

    #[napi(async_runtime)]
    pub fn start(&self, command: CommandInput, policy: Option<Value>) -> Result<ProcessHandle> {
        start_process(
            self.inner
                .start(&parse_command(command)?, &parse_policy(policy)?)
                .map_err(error)?,
        )
    }

    #[napi(async_runtime)]
    pub fn start_background(&self, command: CommandInput, policy: Option<Value>) -> Result<String> {
        let id = self
            .inner
            .start_background(&parse_command(command)?, &parse_policy(policy)?)
            .map_err(error)?;
        Ok(id.to_string())
    }

    #[napi]
    pub async fn wait_process(&self, process_id: String) -> Result<Option<Value>> {
        let id = parse_process_id(&process_id)?;
        self.inner
            .wait_process(id)
            .await
            .map_err(error)?
            .map(|value| {
                managed_result_value(
                    value.stdout,
                    value.stderr,
                    value.output_truncated,
                    value.receipt,
                    value.error,
                )
            })
            .transpose()
    }

    #[napi]
    pub fn process_status(&self, process_id: String) -> Result<Option<String>> {
        Ok(self
            .inner
            .process_status(parse_process_id(&process_id)?)
            .map(|status| format!("{status:?}").to_lowercase()))
    }

    #[napi]
    pub fn process_info(&self, process_id: String) -> Result<Option<Value>> {
        let Some(info) = self.inner.process(parse_process_id(&process_id)?) else {
            return Ok(None);
        };
        Ok(Some(serde_json::json!({
            "supervisorProcessId": info.supervisor_id.to_string(),
            "processId": info.process_id,
            "alive": info.alive,
            "parentProcessId": info.parent_process_id,
            "startTimeTicks": info.start_time_ticks.map(|ticks| ticks.to_string()),
            "executable": info.executable,
            "framework": info.framework,
            "runLabel": info.run_label,
            "projectId": info.project_id,
            "flowRunId": info.flow_run_id,
            "taskId": info.task_id,
            "postId": info.post_id,
            "workerId": info.worker_id,
            "runtimeRunId": info.runtime_run_id,
        })))
    }

    #[napi]
    pub fn process_result(&self, process_id: String) -> Result<Option<Value>> {
        managed_result_option(self.inner.process_result(parse_process_id(&process_id)?))
    }

    #[napi]
    pub fn take_process_result(&self, process_id: String) -> Result<Option<Value>> {
        managed_result_option(
            self.inner
                .take_process_result(parse_process_id(&process_id)?),
        )
    }

    #[napi]
    pub fn forget_process(&self, process_id: String) -> Result<bool> {
        Ok(self.inner.forget_process(parse_process_id(&process_id)?))
    }

    #[napi]
    pub async fn write_process_stdin(&self, process_id: String, data: Buffer) -> Result<()> {
        self.inner
            .write_process_stdin(parse_process_id(&process_id)?, &data)
            .await
            .map_err(error)
    }

    #[napi]
    pub async fn close_process_stdin(&self, process_id: String) -> Result<()> {
        self.inner
            .close_process_stdin(parse_process_id(&process_id)?)
            .await
            .map_err(error)
    }

    #[napi]
    pub async fn shutdown_process(
        &self,
        process_id: String,
        grace_ms: Option<u32>,
    ) -> Result<String> {
        let result = self
            .inner
            .shutdown_process(
                parse_process_id(&process_id)?,
                Duration::from_millis(grace_ms.unwrap_or(5_000) as u64),
            )
            .await
            .map_err(error)?;
        Ok(format!("{result:?}").to_lowercase())
    }

    #[napi]
    pub async fn shutdown_all(&self, grace_ms: Option<u32>) -> Result<Vec<Value>> {
        let completed = self
            .inner
            .shutdown_all(Duration::from_millis(grace_ms.unwrap_or(5_000) as u64))
            .await
            .map_err(error)?;
        completed
            .into_iter()
            .map(|(id, result)| {
                Ok(serde_json::json!({
                    "processId": id.to_string(),
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "outputTruncated": result.output_truncated,
                    "receipt": serde_json::to_value(result.receipt).map_err(error)?,
                    "error": result.error,
                }))
            })
            .collect()
    }

    #[napi]
    pub async fn start_tui(&self, executable: String, args: Option<Vec<String>>) -> Result<()> {
        let args = args
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        self.inner
            .start_tui(PathBuf::from(executable).as_path(), &args)
            .map_err(error)
    }

    #[napi]
    pub fn stream(&self, process_id: Option<String>) -> Result<SupervisorEventStream> {
        let scope = match process_id {
            Some(id) => StreamScope::Process(parse_process_id(&id)?),
            None => StreamScope::All,
        };
        Ok(SupervisorEventStream {
            inner: Arc::new(Mutex::new(self.inner.stream(scope))),
        })
    }
}

#[napi]
pub struct AgentRun {
    inner: RustAgentRun,
    owner: Arc<RustSupervisor>,
}

#[napi]
impl AgentRun {
    #[napi(getter)]
    pub fn id(&self) -> String {
        self.inner.id().to_string()
    }

    #[napi(getter)]
    pub fn active_processes(&self) -> u32 {
        self.inner.active_processes() as u32
    }

    #[napi]
    pub async fn run_command(
        &self,
        command: CommandInput,
        policy: Option<Value>,
        input: Option<Buffer>,
    ) -> Result<ExecutionOutputValue> {
        let output = self
            .inner
            .run_command(
                &parse_command(command)?,
                &parse_policy(policy)?,
                input.as_deref().unwrap_or_default(),
            )
            .await
            .map_err(error)?;
        execution_value(output)
    }

    #[napi(async_runtime)]
    pub fn start(&self, command: CommandInput, policy: Option<Value>) -> Result<ProcessHandle> {
        start_process(
            self.inner
                .start(&parse_command(command)?, &parse_policy(policy)?)
                .map_err(error)?,
        )
    }

    #[napi]
    pub fn cancel(&self) {
        self.inner.request_cancel();
    }

    #[napi]
    pub fn cancel_now(&self) {
        self.inner.cancel_now();
    }

    #[napi]
    pub async fn wait_cleanup(&self) {
        self.inner.cancel().await;
    }

    #[napi]
    pub fn publish_agent_event(&self, payload: Value) {
        let payload: AgentEventPayload = Arc::new(payload);
        self.inner.publish_agent_event(payload);
    }

    #[napi]
    pub fn stream(&self) -> SupervisorEventStream {
        SupervisorEventStream {
            inner: Arc::new(Mutex::new(self.owner.stream(StreamScope::Run(&self.inner)))),
        }
    }
}

fn start_process(child: agent_supervisor::SupervisedChild) -> Result<ProcessHandle> {
    let id = child.supervisor_id().map(|id| id.to_string());
    let events = child.into_event_stream().map_err(error)?;
    Ok(ProcessHandle {
        id,
        events: Arc::new(Mutex::new(events)),
    })
}

#[napi]
pub struct ProcessHandle {
    id: Option<String>,
    events: Arc<Mutex<ProcessEventStream>>,
}

#[napi]
impl ProcessHandle {
    #[napi(getter)]
    pub fn supervisor_process_id(&self) -> Option<String> {
        self.id.clone()
    }

    #[napi]
    pub async fn next_event(&self) -> Result<Option<ProcessEventValue>> {
        let event = self.events.lock().await.next().await.map_err(error)?;
        Ok(event.map(process_event_value))
    }

    #[napi]
    pub async fn write(&self, data: Buffer) -> Result<()> {
        self.events.lock().await.write(&data).await.map_err(error)
    }

    #[napi]
    pub async fn close_stdin(&self) -> Result<()> {
        self.events.lock().await.close_stdin().await.map_err(error)
    }

    #[napi]
    pub async fn wait(&self) -> Result<Value> {
        receipt_value(self.events.lock().await.wait().await.map_err(error)?)
    }

    #[napi]
    pub async fn shutdown(&self, grace_ms: Option<u32>) -> Result<Value> {
        receipt_value(
            self.events
                .lock()
                .await
                .shutdown(Duration::from_millis(grace_ms.unwrap_or(5_000) as u64))
                .await
                .map_err(error)?,
        )
    }
}

fn process_event_value(event: ProcessEvent) -> ProcessEventValue {
    match event {
        ProcessEvent::Started {
            process_id,
            executable,
        } => ProcessEventValue {
            kind: "started".into(),
            process_id,
            data: None,
            receipt: None,
            executable: serde_json::to_value(executable).ok(),
        },
        ProcessEvent::Stdout(data) => ProcessEventValue {
            kind: "stdout".into(),
            process_id: None,
            data: Some(Buffer::from(data)),
            receipt: None,
            executable: None,
        },
        ProcessEvent::Stderr(data) => ProcessEventValue {
            kind: "stderr".into(),
            process_id: None,
            data: Some(Buffer::from(data)),
            receipt: None,
            executable: None,
        },
        ProcessEvent::Exited(receipt) => ProcessEventValue {
            kind: "exited".into(),
            process_id: None,
            data: None,
            receipt: serde_json::to_value(receipt).ok(),
            executable: None,
        },
    }
}

#[napi]
pub struct SupervisorEventStream {
    inner: Arc<Mutex<EventStream>>,
}

#[napi]
impl SupervisorEventStream {
    #[napi]
    pub async fn next_event(&self) -> Option<SupervisorEventValue> {
        self.inner
            .lock()
            .await
            .next()
            .await
            .map(supervisor_event_value)
    }
}

fn supervisor_event_value(event: SupervisorEvent) -> SupervisorEventValue {
    let mut value = SupervisorEventValue {
        sequence: event.seq.to_string(),
        run_id: event.run_id.map(|id| id.to_string()),
        process_id: event.process_id.map(|id| id.to_string()),
        kind: String::new(),
        data: None,
        receipt: None,
        executable: None,
        payload: None,
        dropped: None,
    };
    match event.kind {
        SupervisorEventKind::RunStarted => value.kind = "runStarted".into(),
        SupervisorEventKind::RunEnded => value.kind = "runEnded".into(),
        SupervisorEventKind::ProcessStarted { executable } => {
            value.kind = "started".into();
            value.executable = serde_json::to_value(executable).ok();
        }
        SupervisorEventKind::Stdout(data) => {
            value.kind = "stdout".into();
            value.data = Some(Buffer::from(data));
        }
        SupervisorEventKind::Stderr(data) => {
            value.kind = "stderr".into();
            value.data = Some(Buffer::from(data));
        }
        SupervisorEventKind::ProcessExited(receipt) => {
            value.kind = "exited".into();
            value.receipt = receipt.and_then(|receipt| serde_json::to_value(receipt).ok());
        }
        SupervisorEventKind::Agent(payload) => {
            value.kind = "agent".into();
            value.payload = payload.downcast_ref::<Value>().cloned();
        }
        SupervisorEventKind::Lagged { dropped } => {
            value.kind = "lagged".into();
            value.dropped = Some(dropped.to_string());
        }
    }
    value
}

fn parse_process_id(value: &str) -> Result<SupervisorProcessId> {
    SupervisorProcessId::from_str(value).map_err(error)
}

#[napi]
pub fn platform_capabilities() -> Result<Value> {
    serde_json::to_value(agent_supervisor::platform_capabilities()).map_err(error)
}

fn managed_result_value(
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_truncated: bool,
    receipt: ProcessReceipt,
    error_text: Option<String>,
) -> Result<Value> {
    Ok(serde_json::json!({
        "stdout": stdout,
        "stderr": stderr,
        "outputTruncated": output_truncated,
        "receipt": serde_json::to_value(receipt).map_err(error)?,
        "error": error_text,
    }))
}

fn managed_result_option(
    value: Option<agent_supervisor::ManagedProcessResult>,
) -> Result<Option<Value>> {
    value
        .map(|result| {
            managed_result_value(
                result.stdout,
                result.stderr,
                result.output_truncated,
                result.receipt,
                result.error,
            )
        })
        .transpose()
}
