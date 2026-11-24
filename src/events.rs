use crate::{SandboxError, SupervisedChild};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
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

/// What `ProcessEventStream::next` woke up for.
enum Waited {
    Message(Option<EventMessage>),
    Exit(Result<crate::ProcessReceipt, SandboxError>),
    DrainDeadline,
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
    completed: Option<crate::ProcessReceipt>,
    process_id: Option<u32>,
    executable: crate::ExecutableIdentity,
    started: bool,
    readers_finished: usize,
    stdin: Option<Box<dyn tokio::io::AsyncWrite + Unpin + Send>>,
    tap: Option<crate::event_log::ProcessTap>,
    /// When the process itself exited; output may still be draining.
    exited_at: Option<Instant>,
    /// How long output may keep draining after the exit before `Exited` is
    /// reported anyway (a process left running can hold the pipes open).
    drain_grace: Duration,
    /// Stop what the process left running in its process group when it exits.
    stop_group_on_exit: bool,
}

/// Default time output may keep draining after the process exits.
pub const DEFAULT_OUTPUT_DRAIN_GRACE: Duration = Duration::from_secs(2);
/// Time processes left running get to stop on SIGTERM before SIGKILL.
const LEFTOVER_STOP_GRACE: Duration = Duration::from_millis(500);

impl ProcessEventStream {
    pub(crate) fn new(mut child: SupervisedChild) -> Result<Self, SandboxError> {
        let stdin = child.take_stdin();
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
        let tap = child.take_tap();
        spawn_reader(stdout, sender.clone(), true, output_limit, tap.clone());
        spawn_reader(stderr, sender, false, stderr_limit, tap.clone());
        Ok(Self {
            child: Some(child),
            messages,
            exit: None,
            completed: None,
            process_id,
            executable,
            started: false,
            readers_finished: 0,
            stdin,
            tap,
            exited_at: None,
            drain_grace: DEFAULT_OUTPUT_DRAIN_GRACE,
            stop_group_on_exit: false,
        })
    }

    /// Stop what the process leaves running in its process group (servers
    /// started in the background) as soon as the process itself exits: SIGTERM,
    /// then SIGKILL after a short grace. Use it for one-shot commands. Off by
    /// default, so a process may hand work to children that outlive it. On macOS
    /// a process that moves itself to another process group escapes this; Linux
    /// cgroups and Windows job objects also cover those.
    pub fn stop_process_group_on_exit(&mut self, enabled: bool) {
        self.stop_group_on_exit = enabled;
    }

    /// How long output may keep draining after the process exits before
    /// `Exited` is reported with `output_incomplete` set.
    pub fn set_output_drain_grace(&mut self, grace: Duration) {
        self.drain_grace = grace;
    }

    /// The process itself has exited: record it, release the child handle,
    /// and start stopping what it left running when asked to. The stop runs in
    /// its own task, so a caller that drops `next()` midway cannot skip it.
    fn main_exited(&mut self, receipt: crate::ProcessReceipt) {
        if let Some(child) = self.child.take() {
            if self.stop_group_on_exit {
                let container = child.container();
                tokio::spawn(async move {
                    let _ = container.request_graceful_stop();
                    tokio::time::sleep(LEFTOVER_STOP_GRACE).await;
                    let _ = container.kill_remaining_group();
                });
            }
        }
        self.exit = Some(receipt);
        self.exited_at = Some(Instant::now());
    }

    /// `Exited` once the output has drained, or the drain grace has passed.
    fn finish_if_drained(&mut self) -> Option<ProcessEvent> {
        let exited_at = self.exited_at?;
        if self.readers_finished >= 2 {
            return self.take_exit_event();
        }
        if exited_at.elapsed() >= self.drain_grace {
            if let Some(receipt) = self.exit.as_mut() {
                receipt.output_incomplete = true;
            }
            return self.take_exit_event();
        }
        None
    }

    /// Write bytes to the running process stdin.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<(), SandboxError> {
        let stdin = self.stdin.as_mut().ok_or_else(|| {
            SandboxError::execution(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "stdin is closed",
            ))
        })?;
        stdin.write_all(bytes).await.map_err(SandboxError::stdin)
    }

    pub(crate) fn take_stdin_writer(&mut self) -> Option<Box<dyn AsyncWrite + Unpin + Send>> {
        self.stdin.take()
    }

    /// Close stdin and notify the process that no more input will arrive.
    pub async fn close_stdin(&mut self) -> Result<(), SandboxError> {
        // Dropping Tokio's child stdin handle closes the pipe and sends EOF.
        drop(self.stdin.take());
        Ok(())
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
                    self.main_exited(status);
                }
            }

            match self.messages.try_recv() {
                Ok(EventMessage::Stdout(bytes)) => return Ok(Some(ProcessEvent::Stdout(bytes))),
                Ok(EventMessage::Stderr(bytes)) => return Ok(Some(ProcessEvent::Stderr(bytes))),
                Ok(EventMessage::Limit(stdout)) => {
                    let termination = if stdout {
                        crate::TerminationReason::StdoutLimitExceeded
                    } else {
                        crate::TerminationReason::StderrLimitExceeded
                    };
                    self.stop_after_reader_error(Some(termination)).await?;
                    return Err(if stdout {
                        SandboxError::output_too_large()
                    } else {
                        SandboxError::stderr_too_large()
                    });
                }
                Ok(EventMessage::ReadFailed(stdout, error)) => {
                    self.stop_after_reader_error(None).await?;
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
                    if self.child.is_none() {
                        return Ok(self.take_exit_event());
                    }
                    if let Some(child) = self.child.as_mut() {
                        let receipt = child.wait_exit().await?;
                        self.main_exited(receipt);
                    }
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }

            if self.child.is_none() {
                if let Some(event) = self.finish_if_drained() {
                    return Ok(Some(event));
                }
            }

            // Wait for output, the process's own exit, or the end of the drain
            // grace, whichever comes first. Output held open by a process left
            // running must not hide the exit.
            let drain_deadline = self
                .exited_at
                .map(|exited_at| tokio::time::Instant::from_std(exited_at + self.drain_grace));
            let waited = {
                let messages = &mut self.messages;
                let child = self.child.as_mut();
                tokio::select! {
                    message = messages.recv() => Waited::Message(message),
                    receipt = async {
                        match child {
                            Some(child) => child.wait_exit().await,
                            None => std::future::pending().await,
                        }
                    } => Waited::Exit(receipt),
                    _ = async {
                        match drain_deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    } => Waited::DrainDeadline,
                }
            };
            let message = match waited {
                Waited::Message(message) => message,
                Waited::Exit(receipt) => {
                    self.main_exited(receipt?);
                    continue;
                }
                Waited::DrainDeadline => continue,
            };
            match message {
                Some(EventMessage::Stdout(bytes)) => return Ok(Some(ProcessEvent::Stdout(bytes))),
                Some(EventMessage::Stderr(bytes)) => return Ok(Some(ProcessEvent::Stderr(bytes))),
                Some(EventMessage::Limit(stdout)) => {
                    let termination = if stdout {
                        crate::TerminationReason::StdoutLimitExceeded
                    } else {
                        crate::TerminationReason::StderrLimitExceeded
                    };
                    self.stop_after_reader_error(Some(termination)).await?;
                    return Err(if stdout {
                        SandboxError::output_too_large()
                    } else {
                        SandboxError::stderr_too_large()
                    });
                }
                Some(EventMessage::ReadFailed(stdout, error)) => {
                    self.stop_after_reader_error(None).await?;
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
                    return Ok(self.take_exit_event());
                };
                let receipt = child
                    .wait_with_cancellation(cancellation.clone(), Duration::from_millis(250))
                    .await?;
                self.exit = Some(receipt.clone());
                self.completed = Some(receipt.clone());
                self.publish_exit(&receipt);
                Ok(Some(ProcessEvent::Exited(receipt)))
            }
        }
    }

    /// Wait for process completion, discarding any remaining output events.
    pub async fn wait(&mut self) -> Result<crate::ProcessReceipt, SandboxError> {
        if let Some(receipt) = &self.completed {
            return Ok(receipt.clone());
        }
        while let Some(event) = self.next().await? {
            if let ProcessEvent::Exited(receipt) = event {
                return Ok(receipt);
            }
        }
        self.completed.clone().ok_or_else(|| {
            SandboxError::execution(std::io::Error::other("process event stream ended"))
        })
    }

    pub async fn shutdown(
        &mut self,
        grace: Duration,
    ) -> Result<crate::ProcessReceipt, SandboxError> {
        let receipt = if let Some(child) = self.child.take() {
            child.shutdown(grace).await?
        } else if let Some(receipt) = self.completed.clone().or_else(|| self.exit.take()) {
            receipt
        } else {
            return Err(SandboxError::execution(std::io::Error::other(
                "session already completed",
            )));
        };
        self.completed = Some(receipt.clone());
        self.publish_exit(&receipt);
        Ok(receipt)
    }

    fn publish_exit(&mut self, receipt: &crate::ProcessReceipt) {
        if let Some(tap) = self.tap.take() {
            tap.finish(Some(receipt.clone()));
        }
    }

    async fn stop_after_reader_error(
        &mut self,
        termination: Option<crate::TerminationReason>,
    ) -> Result<(), SandboxError> {
        if let Some(child) = self.child.take() {
            let mut receipt = child.shutdown(Duration::from_millis(100)).await?;
            if let Some(termination) = termination {
                receipt.termination = termination;
            }
            self.exit = Some(receipt.clone());
            self.completed = Some(receipt.clone());
            self.publish_exit(&receipt);
        }
        Ok(())
    }

    fn take_exit_event(&mut self) -> Option<ProcessEvent> {
        let receipt = self.exit.take()?;
        self.completed = Some(receipt.clone());
        self.publish_exit(&receipt);
        Some(ProcessEvent::Exited(receipt))
    }
}

fn spawn_reader(
    mut reader: impl AsyncRead + Unpin + Send + 'static,
    sender: mpsc::Sender<EventMessage>,
    stdout: bool,
    limit: usize,
    tap: Option<crate::event_log::ProcessTap>,
) {
    let publish = move |bytes: &[u8]| match &tap {
        Some(tap) if stdout => tap.stdout(bytes),
        Some(tap) => tap.stderr(bytes),
        None => {}
    };
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
                            publish(&buffer[..remaining]);
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
                    total = total.saturating_add(size);
                    publish(&buffer[..size]);
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
    use crate::{spawn, ExecutionRequest};
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
        let supervisor = crate::Supervisor::new();
        let command = crate::Command {
            argv: vec!["/usr/bin/printf".into(), "hello".into()],
            cwd: None,
        };
        let mut stream = supervisor
            .start(&command, &request.policy)
            .unwrap()
            .into_event_stream()
            .unwrap();
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

    #[tokio::test]
    async fn writes_stdin_and_closes_it_before_observing_exit() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/bin/cat"),
            args: Vec::new(),
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let mut stream = spawn(&request).unwrap().into_event_stream().unwrap();
        stream.write(b"input bytes").await.unwrap();
        stream.close_stdin().await.unwrap();

        let mut output = Vec::new();
        let mut exited = false;
        while let Some(event) = stream.next().await.unwrap() {
            match event {
                ProcessEvent::Stdout(bytes) => output.extend(bytes),
                ProcessEvent::Exited(_) => exited = true,
                ProcessEvent::Started { .. } | ProcessEvent::Stderr(_) => {}
            }
        }
        assert_eq!(output, b"input bytes");
        assert!(exited);
    }

    #[tokio::test]
    async fn run_is_the_one_shot_command_entry_point() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/usr/bin/printf"),
            args: vec!["one shot".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };

        let command = crate::Command {
            argv: vec!["/usr/bin/printf".into(), "one shot".into()],
            cwd: None,
        };
        let output = crate::run(&command, &request.policy).await.unwrap();

        assert!(output.success);
        assert_eq!(output.stdout, b"one shot");
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

    /// Runs `script` with /bin/sh; it writes the PID of a background
    /// `sleep 30` to `pid_file` and keeps the sleep holding stdout open.
    fn background_holder(pid_file: &std::path::Path) -> ExecutionRequest {
        ExecutionRequest {
            executable: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".into(),
                format!(
                    "sleep 30 & echo $! > '{}'; echo started",
                    pid_file.display()
                ),
            ],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        }
    }

    async fn run_to_exit(stream: &mut ProcessEventStream) -> (String, crate::ProcessReceipt) {
        let mut stdout = String::new();
        loop {
            match stream.next().await.unwrap() {
                Some(ProcessEvent::Stdout(bytes)) => {
                    stdout.push_str(&String::from_utf8_lossy(&bytes))
                }
                Some(ProcessEvent::Exited(receipt)) => return (stdout, receipt),
                Some(_) => {}
                None => panic!("the stream ended without an exit"),
            }
        }
    }

    fn alive(pid: i32) -> bool {
        unsafe { libc::kill(pid, 0) == 0 }
    }

    // Found live 2026-11-22: a command that left a server running in the
    // background was never reported as exited, because the server held its
    // output open.
    #[tokio::test]
    async fn the_exit_is_reported_even_when_a_background_process_holds_the_output_open() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("pid");
        let mut stream = spawn(&background_holder(&pid_file))
            .unwrap()
            .into_event_stream()
            .unwrap();
        stream.set_output_drain_grace(Duration::from_millis(300));
        let started = Instant::now();
        let (stdout, receipt) =
            tokio::time::timeout(Duration::from_secs(10), run_to_exit(&mut stream))
                .await
                .expect("the exit is reported without waiting for the background process");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(stdout, "started\n");
        assert!(receipt.output_incomplete, "the output was still held open");
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            alive(pid),
            "without the option, the background process keeps running"
        );
        unsafe { libc::kill(pid, libc::SIGKILL) };
    }

    #[tokio::test]
    async fn a_one_shot_command_has_what_it_left_running_stopped_when_it_exits() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("pid");
        let mut stream = spawn(&background_holder(&pid_file))
            .unwrap()
            .into_event_stream()
            .unwrap();
        stream.stop_process_group_on_exit(true);
        let (stdout, receipt) =
            tokio::time::timeout(Duration::from_secs(10), run_to_exit(&mut stream))
                .await
                .expect("the exit is reported");
        assert_eq!(stdout, "started\n");
        assert!(
            !receipt.output_incomplete,
            "stopping the group closes its output"
        );
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while alive(pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!alive(pid), "the background process was stopped");
    }

    #[tokio::test]
    async fn a_command_without_background_processes_reports_all_its_output() {
        let request = ExecutionRequest {
            executable: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), "for i in 1 2 3; do echo line $i; done".into()],
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let mut stream = spawn(&request).unwrap().into_event_stream().unwrap();
        stream.stop_process_group_on_exit(true);
        let (stdout, receipt) = run_to_exit(&mut stream).await;
        assert_eq!(stdout, "line 1\nline 2\nline 3\n");
        assert!(!receipt.output_incomplete);
    }
}
