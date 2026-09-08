use crate::{
    ExecutionRequest, ProcessGroup, ProcessReceipt, SandboxError, SupervisedChild,
    TerminationReason,
};
use futures::{stream::FuturesUnordered, StreamExt};
use std::future::Future;
use std::io;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;

/// A supervised, connected sequence of processes.
///
/// All stages share one process container. Dropping the pipeline therefore
/// cleans up every stage, while dropping an individual stage handle does not
/// tear down its siblings.
pub struct Pipeline {
    group: ProcessGroup,
    stages: Vec<SupervisedChild>,
    pumps: Vec<JoinHandle<io::Result<u64>>>,
}

#[derive(Debug)]
pub struct PipelineError {
    pub stage: Option<usize>,
    pub kind: PipelineFailureKind,
    pub error: SandboxError,
    /// Receipts for stages that were successfully reaped before the failure
    /// was reported. The pipeline always attempts to reap every stage.
    pub receipts: Vec<ProcessReceipt>,
}

/// Identifies whether a pipeline failed while reaping a stage, observing a
/// stage's exit status, or transporting bytes between stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineFailureKind {
    StageExit,
    StageWait,
    Pipe,
}

enum PipelineTaskResult {
    Stage(usize, Result<ProcessReceipt, SandboxError>),
    Pump(usize, io::Result<u64>),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.stage {
            Some(stage) => write!(formatter, "pipeline stage {stage} failed: {}", self.error),
            None => self.error.fmt(formatter),
        }
    }
}

impl std::error::Error for PipelineError {}

impl Pipeline {
    pub fn start(requests: &[ExecutionRequest]) -> Result<Self, SandboxError> {
        if requests.is_empty() {
            return Err(SandboxError::spawn(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a pipeline requires at least one stage",
            )));
        }
        let group = ProcessGroup::new()?;
        let mut stages = Vec::with_capacity(requests.len());
        for request in requests {
            stages.push(group.start(request)?);
        }

        let mut pumps = Vec::with_capacity(stages.len().saturating_sub(1));
        for index in 0..stages.len().saturating_sub(1) {
            let stdout = stages[index].take_stdout().ok_or_else(|| {
                SandboxError::spawn(io::Error::other("pipeline stage stdout unavailable"))
            })?;
            let stdin = stages[index + 1].take_stdin().ok_or_else(|| {
                SandboxError::spawn(io::Error::other("pipeline stage stdin unavailable"))
            })?;
            pumps.push(tokio::spawn(async move { copy_pipe(stdout, stdin).await }));
        }

        Ok(Self {
            group,
            stages,
            pumps,
        })
    }

    pub fn enforcement(&self) -> crate::Enforcement {
        self.group.enforcement()
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn is_alive(&self) -> bool {
        self.group.is_alive()
    }

    /// Writes input to the first stage and closes its stdin so the pipeline
    /// can observe end-of-input.
    pub async fn write_input(&mut self, input: &[u8]) -> Result<(), SandboxError> {
        let stdin = self.stages[0]
            .take_stdin()
            .ok_or_else(|| SandboxError::stdin(io::Error::other("pipeline stdin unavailable")))?;
        let mut stdin = stdin;
        stdin.write_all(input).await.map_err(SandboxError::stdin)?;
        stdin.shutdown().await.map_err(SandboxError::stdin)
    }

    pub fn terminate(&self) {
        self.group.terminate();
    }

    pub async fn wait(self) -> Result<Vec<ProcessReceipt>, SandboxError> {
        self.wait_detailed().await.map_err(|failure| failure.error)
    }

    /// Wait for all stages and report the stage responsible for a transport or
    /// non-zero-exit failure. All stages are still reaped before returning.
    pub async fn wait_detailed(self) -> Result<Vec<ProcessReceipt>, PipelineError> {
        let Pipeline {
            group,
            stages,
            pumps,
        } = self;
        let stage_count = stages.len();
        let mut tasks: FuturesUnordered<Pin<Box<dyn Future<Output = PipelineTaskResult>>>> =
            FuturesUnordered::new();
        for (index, stage) in stages.into_iter().enumerate() {
            tasks.push(Box::pin(async move {
                PipelineTaskResult::Stage(index, stage.wait().await)
            }));
        }
        for (index, pump) in pumps.into_iter().enumerate() {
            tasks.push(Box::pin(async move {
                PipelineTaskResult::Pump(
                    index,
                    pump.await
                        .map_err(io::Error::other)
                        .and_then(|result| result),
                )
            }));
        }
        let mut receipts = Vec::with_capacity(stage_count);
        let mut failure = None;
        let mut termination_started = false;
        let mut stage_failure_has_exit_code = false;
        while let Some(result) = tasks.next().await {
            match result {
                PipelineTaskResult::Stage(index, Ok(receipt)) => {
                    let stage_failed = !matches!(
                        receipt.termination,
                        TerminationReason::Exited { code: Some(0), .. }
                    );
                    let coded_failure = matches!(
                        receipt.termination,
                        TerminationReason::Exited {
                            code: Some(code),
                            ..
                        } if code != 0
                    );
                    if coded_failure && !stage_failure_has_exit_code {
                        // A non-zero exit is more useful than a signal caused
                        // by terminating the rest of the pipeline after an
                        // earlier stage failed.
                        failure = Some((
                            index,
                            PipelineFailureKind::StageExit,
                            SandboxError::execution(io::Error::other(
                                "pipeline stage exited unsuccessfully",
                            )),
                        ));
                        stage_failure_has_exit_code = true;
                    } else if stage_failed && failure.is_none() && !termination_started {
                        failure = Some((
                            index,
                            PipelineFailureKind::StageExit,
                            SandboxError::execution(io::Error::other(
                                "pipeline stage exited unsuccessfully",
                            )),
                        ));
                    }
                    if stage_failed && !termination_started {
                        termination_started = true;
                        group.terminate();
                    }
                    receipts.push(receipt);
                }
                PipelineTaskResult::Stage(index, Err(error)) if failure.is_none() => {
                    failure = Some((index, PipelineFailureKind::StageWait, error));
                    if !termination_started {
                        termination_started = true;
                        group.terminate();
                    }
                }
                PipelineTaskResult::Stage(_, Err(_)) => {}
                PipelineTaskResult::Pump(_, Ok(_)) => {}
                PipelineTaskResult::Pump(index, Err(error)) if failure.is_none() => {
                    failure = Some((
                        index,
                        PipelineFailureKind::Pipe,
                        SandboxError::execution(error),
                    ));
                    if !termination_started {
                        termination_started = true;
                        group.terminate();
                    }
                }
                PipelineTaskResult::Pump(_, Err(_)) => {}
            }
        }
        if let Some((stage, kind, error)) = failure {
            return Err(PipelineError {
                stage: Some(stage),
                kind,
                error,
                receipts,
            });
        }
        Ok(receipts)
    }
}

async fn copy_pipe<R, W>(mut reader: R, mut writer: W) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let copied = tokio::io::copy(&mut reader, &mut writer).await?;
    writer.shutdown().await?;
    Ok(copied)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        EnforcementRequirement, EnvironmentPolicy, PipelineFailureKind, ResourceLimits,
        TerminationReason,
    };
    use std::path::PathBuf;

    fn request(executable: &str, args: &[&str]) -> ExecutionRequest {
        ExecutionRequest {
            executable: PathBuf::from(executable),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            environment: EnvironmentPolicy::default(),
            working_directory: None,
            filesystem: None,
            network: crate::NetworkMode::Host,
            limits: ResourceLimits::default(),
            enforcement: EnforcementRequirement::BestEffort,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connects_stages_and_cleans_up_as_one_unit() {
        let requests = [request("/bin/cat", &[]), request("/usr/bin/wc", &["-c"])].to_vec();
        let mut pipeline = Pipeline::start(&requests).unwrap();
        assert_eq!(pipeline.len(), 2);
        pipeline.write_input(b"hello\n").await.unwrap();
        let receipts = pipeline.wait().await.unwrap();
        assert_eq!(receipts.len(), 2);
        assert!(receipts.iter().all(|receipt| matches!(
            receipt.termination,
            TerminationReason::Exited { code: Some(0), .. }
        )));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reports_failed_stage_after_reaping_all_stages() {
        let requests = [
            request("/bin/echo", &["hello"]),
            request("/usr/bin/false", &[]),
        ];
        let error = Pipeline::start(&requests)
            .unwrap()
            .wait_detailed()
            .await
            .unwrap_err();
        assert_eq!(error.stage, Some(1));
        assert_eq!(error.kind, PipelineFailureKind::StageExit);
        assert_eq!(error.receipts.len(), 2);
        assert!(error.receipts.iter().any(|receipt| matches!(
            receipt.termination,
            TerminationReason::Exited { code: Some(1), .. }
        )));
    }
}
