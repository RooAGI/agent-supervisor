use crate::{SandboxError, SupervisedChild};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ProcessEvent {
    Started {
        process_id: Option<u32>,
        executable: crate::ExecutableIdentity,
    },
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exited(crate::ProcessReceipt),
}

enum EventMessage {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Limit(bool),
    ReadFailed(bool, std::io::Error),
    ReaderFinished,
}

pub struct ProcessEventStream {
    child: Option<SupervisedChild>,
    messages: mpsc::Receiver<EventMessage>,
    exit: Option<crate::ProcessReceipt>,
    process_id: Option<u32>,
    executable: crate::ExecutableIdentity,
    started: bool,
    readers_finished: usize,
}

impl ProcessEventStream {
    pub(crate) fn new(mut child: SupervisedChild) -> Result<Self, SandboxError> {
        let stdout = child
            .take_stdout()
            .ok_or_else(|| SandboxError::execution(std::io::Error::other("stdout unavailable")))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| SandboxError::execution(std::io::Error::other("stderr unavailable")))?;
        let process_id = child.id();
        let executable = child.executable().clone();
        let (sender, messages) = mpsc::channel(32);
        let (output_limit, stderr_limit) = child.output_limits();
        spawn_reader(stdout, sender.clone(), true, output_limit);
        spawn_reader(stderr, sender, false, stderr_limit);
        Ok(Self {
            child: Some(child),
            messages,
            exit: None,
            process_id,
            executable,
            started: false,
            readers_finished: 0,
        })
    }

    pub async fn next(&mut self) -> Result<Option<ProcessEvent>, SandboxError> {
        if !self.started {
            self.started = true;
            return Ok(Some(ProcessEvent::Started {
                process_id: self.process_id,
                executable: self.executable.clone(),
            }));
        }
        loop {
            if let Some(child) = self.child.as_mut() {
                if let Some(status) = child.try_wait().await? {
                    self.exit = Some(status);
                    self.child = None;
                }
            }

            match self.messages.try_recv() {
                Ok(EventMessage::Stdout(bytes)) => return Ok(Some(ProcessEvent::Stdout(bytes))),
                Ok(EventMessage::Stderr(bytes)) => return Ok(Some(ProcessEvent::Stderr(bytes))),
                Ok(EventMessage::Limit(stdout)) => {
                    if let Some(child) = self.child.take() {
                        let _ = child.shutdown(Duration::from_millis(100)).await;
                    }
                    return Err(if stdout {
                        SandboxError::output_too_large()
                    } else {
                        SandboxError::stderr_too_large()
                    });
                }
                Ok(EventMessage::ReadFailed(stdout, error)) => {
                    if let Some(child) = self.child.take() {
                        let _ = child.shutdown(Duration::from_millis(100)).await;
                    }
                    return Err(if stdout {
                        SandboxError::stdout_read(error)
                    } else {
                        SandboxError::stderr_read(error)
                    });
                }
                Ok(EventMessage::ReaderFinished) => {
                    self.readers_finished += 1;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    if self.child.is_none() && self.exit.is_some() {
                        return Ok(self.exit.take().map(ProcessEvent::Exited));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }

            if self.child.is_none() && self.readers_finished == 2 {
                if let Some(exit) = self.exit.take() {
                    return Ok(Some(ProcessEvent::Exited(exit)));
                }
                return Ok(None);
            }

            match self.messages.recv().await {
                Some(EventMessage::Stdout(bytes)) => return Ok(Some(ProcessEvent::Stdout(bytes))),
                Some(EventMessage::Stderr(bytes)) => return Ok(Some(ProcessEvent::Stderr(bytes))),
                Some(EventMessage::Limit(stdout)) => {
                    if let Some(child) = self.child.take() {
                        let _ = child.shutdown(Duration::from_millis(100)).await;
                    }
                    return Err(if stdout {
                        SandboxError::output_too_large()
                    } else {
                        SandboxError::stderr_too_large()
                    });
                }
                Some(EventMessage::ReadFailed(stdout, error)) => {
                    if let Some(child) = self.child.take() {
                        let _ = child.shutdown(Duration::from_millis(100)).await;
                    }
                    return Err(if stdout {
                        SandboxError::stdout_read(error)
                    } else {
                        SandboxError::stderr_read(error)
                    });
                }
                Some(EventMessage::ReaderFinished) => {
                    self.readers_finished += 1;
                }
                None => continue,
            }
        }
    }

    /// Receive the next event, terminating the process group if cancellation
    /// wins the race. The cancellation result is represented as the normal
    /// terminal event, keeping consumers on one event protocol.
    pub async fn next_with_cancellation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProcessEvent>, SandboxError> {
        if !self.started {
            return self.next().await;
        }
        tokio::select! {
            result = self.next() => result,
            _ = cancellation.cancelled() => {
                let Some(child) = self.child.take() else {
                    return Ok(self.exit.take().map(ProcessEvent::Exited));
                };
                let receipt = child
                    .wait_with_cancellation(cancellation.clone(), Duration::from_millis(250))
                    .await?;
                self.exit = Some(receipt.clone());
                Ok(Some(ProcessEvent::Exited(receipt)))
            }
        }
    }

    pub async fn shutdown(
        mut self,
        grace: Duration,
    ) -> Result<crate::ProcessReceipt, SandboxError> {
        match self.child.take() {
            Some(child) => child.shutdown(grace).await,
            None => self.exit.ok_or_else(|| {
                SandboxError::execution(std::io::Error::other("session already completed"))
            }),
        }
    }
}

fn spawn_reader(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    sender: mpsc::Sender<EventMessage>,
    stdout: bool,
    limit: usize,
) {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 8 * 1024];
        let mut total = 0usize;
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => {
                    let _ = sender.send(EventMessage::ReaderFinished).await;
                    return;
                }
                Ok(size) => {
                    let remaining = limit.saturating_sub(total);
                    if size > remaining {
                        if remaining > 0 {
                            let bytes = if stdout {
                                EventMessage::Stdout(buffer[..remaining].to_vec())
                            } else {
                                EventMessage::Stderr(buffer[..remaining].to_vec())
                            };
                            let _ = sender.send(bytes).await;
                        }
                        let _ = sender.send(EventMessage::Limit(stdout)).await;
                        let _ = sender.send(EventMessage::ReaderFinished).await;
                        return;
                    }
                    total += size;
                    let message = if stdout {
                        EventMessage::Stdout(buffer[..size].to_vec())
                    } else {
                        EventMessage::Stderr(buffer[..size].to_vec())
                    };
                    if sender.send(message).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(EventMessage::ReadFailed(stdout, error)).await;
                    let _ = sender.send(EventMessage::ReaderFinished).await;
                    return;
                }
            }
        }
    });
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        spawn, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest, ResourceLimits,
    };
    use std::path::PathBuf;

    #[cfg(unix)]
    #[tokio::test]
    async fn streams_output_before_exit() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/usr/bin/printf"),
            args: vec!["hello".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let mut stream = spawn(&request).unwrap().into_event_stream().unwrap();
        let mut output = Vec::new();
        while let Some(event) = stream.next().await.unwrap() {
            match event {
                ProcessEvent::Started { .. } => {}
                ProcessEvent::Stdout(bytes) => output.extend(bytes),
                ProcessEvent::Exited(_) => break,
                ProcessEvent::Stderr(_) => {}
            }
        }
        assert_eq!(output, b"hello");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_is_reported_as_terminal_event() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/bin/sleep"),
            args: vec!["30".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let mut stream = spawn(&request).unwrap().into_event_stream().unwrap();
        assert!(matches!(
            stream.next().await.unwrap(),
            Some(ProcessEvent::Started { .. })
        ));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let event = stream
            .next_with_cancellation(&cancellation)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            ProcessEvent::Exited(crate::ProcessReceipt {
                termination: crate::TerminationReason::Cancelled,
                ..
            })
        ));
    }
}
