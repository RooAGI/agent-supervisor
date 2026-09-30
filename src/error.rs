use crate::{ExecutableIdentity, TerminationReason};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxErrorCode {
    InputTooLarge,
    OutputTooLarge,
    StderrTooLarge,
    RelativeExecutable,
    RelativeWorkingDirectory,
    InvalidWorkingDirectory,
    WorkingDirectoryNotGranted,
    InvalidExecutable,
    RelativeSearchPath,
    InvalidSearchPath,
    InvalidEnvironmentName,
    ForbiddenEnvironmentVariable,
    EnforcementUnavailable,
    SpawnFailed,
    StdinFailed,
    ExecutionFailed,
    StdoutReadFailed,
    StderrReadFailed,
    TimedOut,
    Unsupported,
}

impl SandboxErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InputTooLarge => "input_too_large",
            Self::OutputTooLarge => "output_too_large",
            Self::StderrTooLarge => "stderr_too_large",
            Self::RelativeExecutable => "relative_executable",
            Self::RelativeWorkingDirectory => "relative_working_directory",
            Self::InvalidWorkingDirectory => "invalid_working_directory",
            Self::WorkingDirectoryNotGranted => "working_directory_not_granted",
            Self::InvalidExecutable => "invalid_executable",
            Self::RelativeSearchPath => "relative_search_path",
            Self::InvalidSearchPath => "invalid_search_path",
            Self::InvalidEnvironmentName => "invalid_environment_name",
            Self::ForbiddenEnvironmentVariable => "forbidden_environment_variable",
            Self::EnforcementUnavailable => "sandbox_enforcement_unavailable",
            Self::SpawnFailed => "spawn_failed",
            Self::StdinFailed => "stdin_failed",
            Self::ExecutionFailed => "execution_failed",
            Self::StdoutReadFailed => "stdout_read_failed",
            Self::StderrReadFailed => "stderr_read_failed",
            Self::TimedOut => "timed_out",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPhase {
    Validate,
    Spawn,
    Stdin,
    Stdout,
    Stderr,
    Wait,
    Shutdown,
}

#[derive(Debug)]
pub struct SandboxError {
    code: SandboxErrorCode,
    phase: SandboxPhase,
    message: String,
    executable: Option<Box<ExecutableIdentity>>,
    termination: Option<Box<TerminationReason>>,
    stderr_tail: Option<Box<str>>,
    retryable: bool,
    source: Option<std::io::Error>,
}

impl SandboxError {
    pub fn new(code: SandboxErrorCode, phase: SandboxPhase, message: impl Into<String>) -> Self {
        Self {
            code,
            phase,
            message: message.into(),
            executable: None,
            termination: None,
            stderr_tail: None,
            retryable: false,
            source: None,
        }
    }

    pub fn with_source(
        code: SandboxErrorCode,
        phase: SandboxPhase,
        message: impl Into<String>,
        source: std::io::Error,
    ) -> Self {
        let mut error = Self::new(code, phase, message);
        error.source = Some(source);
        error
    }

    pub fn code(&self) -> &'static str {
        self.code.as_str()
    }
    pub fn code_kind(&self) -> SandboxErrorCode {
        self.code
    }
    pub fn phase(&self) -> SandboxPhase {
        self.phase
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn executable(&self) -> Option<&ExecutableIdentity> {
        self.executable.as_deref()
    }
    pub fn termination(&self) -> Option<&TerminationReason> {
        self.termination.as_deref()
    }
    pub fn stderr_tail(&self) -> Option<&str> {
        self.stderr_tail.as_deref()
    }
    pub fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn with_executable(mut self, executable: ExecutableIdentity) -> Self {
        self.executable = Some(Box::new(executable));
        self
    }
    pub fn with_termination(mut self, termination: TerminationReason) -> Self {
        self.termination = Some(Box::new(termination));
        self
    }
    pub fn with_stderr_tail(mut self, stderr: impl Into<String>) -> Self {
        self.stderr_tail = Some(stderr.into().into_boxed_str());
        self
    }

    pub fn input_too_large() -> Self {
        Self::new(
            SandboxErrorCode::InputTooLarge,
            SandboxPhase::Validate,
            "execution input exceeds the configured limit",
        )
    }
    pub fn output_too_large() -> Self {
        Self::new(
            SandboxErrorCode::OutputTooLarge,
            SandboxPhase::Stdout,
            "execution output exceeds the configured limit",
        )
    }
    pub fn stderr_too_large() -> Self {
        Self::new(
            SandboxErrorCode::StderrTooLarge,
            SandboxPhase::Stderr,
            "execution stderr exceeds the configured limit",
        )
    }
    pub fn relative_executable() -> Self {
        Self::new(
            SandboxErrorCode::RelativeExecutable,
            SandboxPhase::Validate,
            "the executable path must be absolute",
        )
    }
    pub fn invalid_executable(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::InvalidExecutable,
            SandboxPhase::Validate,
            "the executable could not be resolved",
            source,
        )
    }
    pub fn relative_search_path() -> Self {
        Self::new(
            SandboxErrorCode::RelativeSearchPath,
            SandboxPhase::Validate,
            "an executable search path must be absolute",
        )
    }

    pub fn relative_working_directory() -> Self {
        Self::new(
            SandboxErrorCode::RelativeWorkingDirectory,
            SandboxPhase::Validate,
            "the working directory path must be absolute",
        )
    }

    pub fn invalid_working_directory(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::InvalidWorkingDirectory,
            SandboxPhase::Validate,
            "the working directory could not be resolved",
            source,
        )
    }

    pub fn working_directory_not_granted() -> Self {
        Self::new(
            SandboxErrorCode::WorkingDirectoryNotGranted,
            SandboxPhase::Validate,
            "the working directory is outside the filesystem grants",
        )
    }
    pub fn invalid_search_path() -> Self {
        Self::new(
            SandboxErrorCode::InvalidSearchPath,
            SandboxPhase::Validate,
            "the executable search path cannot be represented",
        )
    }
    pub fn invalid_environment_name() -> Self {
        Self::new(
            SandboxErrorCode::InvalidEnvironmentName,
            SandboxPhase::Validate,
            "an environment variable name is invalid",
        )
    }

    pub fn forbidden_environment_variable() -> Self {
        Self::new(
            SandboxErrorCode::ForbiddenEnvironmentVariable,
            SandboxPhase::Validate,
            "the environment variable is forbidden in a sandboxed process",
        )
    }
    pub fn enforcement_unavailable() -> Self {
        Self::new(
            SandboxErrorCode::EnforcementUnavailable,
            SandboxPhase::Validate,
            "the requested OS sandbox enforcement is unavailable",
        )
    }
    pub fn spawn(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::SpawnFailed,
            SandboxPhase::Spawn,
            "failed to spawn the sandboxed process",
            source,
        )
    }
    pub fn stdin(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::StdinFailed,
            SandboxPhase::Stdin,
            "failed to write sandboxed process input",
            source,
        )
    }
    pub fn execution(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::ExecutionFailed,
            SandboxPhase::Wait,
            "sandboxed process execution failed",
            source,
        )
    }

    pub fn stdout_read(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::StdoutReadFailed,
            SandboxPhase::Stdout,
            "failed to read sandboxed process stdout",
            source,
        )
    }

    pub fn stderr_read(source: std::io::Error) -> Self {
        Self::with_source(
            SandboxErrorCode::StderrReadFailed,
            SandboxPhase::Stderr,
            "failed to read sandboxed process stderr",
            source,
        )
    }
    pub fn timed_out() -> Self {
        Self::new(
            SandboxErrorCode::TimedOut,
            SandboxPhase::Wait,
            "sandboxed process exceeded its deadline",
        )
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(
            SandboxErrorCode::Unsupported,
            SandboxPhase::Shutdown,
            message,
        )
    }
}

impl fmt::Display for SandboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for SandboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
