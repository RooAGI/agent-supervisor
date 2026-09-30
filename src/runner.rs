use crate::{ExecutionOutput, ExecutionRequest, SandboxError};
use std::future::Future;
use std::pin::Pin;

/// Injection seam for runtime code that needs to test process orchestration
/// without starting an operating-system process.
pub trait ProcessRunner: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: &'a ExecutionRequest,
        input: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionOutput, SandboxError>> + 'a>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NativeRunner;

impl ProcessRunner for NativeRunner {
    fn execute<'a>(
        &'a self,
        request: &'a ExecutionRequest,
        input: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<ExecutionOutput, SandboxError>> + 'a>> {
        Box::pin(crate::execute(request, input))
    }
}

/// Execute through an injected runner. Production callers normally pass
/// [`NativeRunner`]; tests and embedders can provide a deterministic runner
/// without creating a child process.
pub async fn execute_with_runner(
    runner: &dyn ProcessRunner,
    request: &ExecutionRequest,
    input: &[u8],
) -> Result<ExecutionOutput, SandboxError> {
    runner.execute(request, input).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Enforcement, ExecutableIdentity, TerminationReason};
    use std::path::PathBuf;

    struct MockRunner;

    impl ProcessRunner for MockRunner {
        fn execute<'a>(
            &'a self,
            request: &'a ExecutionRequest,
            input: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<ExecutionOutput, SandboxError>> + 'a>> {
            let executable = request.executable.clone();
            Box::pin(async move {
                Ok(ExecutionOutput {
                    success: true,
                    stdout: input.to_vec(),
                    stderr: Vec::new(),
                    enforcement: Enforcement::Trusted,
                    executable: ExecutableIdentity {
                        requested_path: executable.clone(),
                        canonical_path: executable,
                    },
                    termination: TerminationReason::Exited {
                        code: Some(0),
                        signal: None,
                    },
                })
            })
        }
    }

    #[tokio::test]
    async fn dispatches_through_injected_runner() {
        let request = ExecutionRequest {
            executable: PathBuf::from("mock"),
            args: Vec::new(),
            working_directory: None,
            policy: crate::SandboxPolicy::default(),
        };
        let output = execute_with_runner(&MockRunner, &request, b"input")
            .await
            .unwrap();
        assert_eq!(output.stdout, b"input");
        assert!(output.success);
    }
}
