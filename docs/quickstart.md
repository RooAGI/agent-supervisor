# Quickstart

## Add the dependency

```toml
[dependencies]
rooagi-sandbox = "0.1.0"
```

## Build an execution request

The caller chooses the executable, arguments, environment, filesystem policy,
network mode, resource limits, and enforcement requirement. The sandbox does
not infer missing authority.

```rust
use rooagi_sandbox::{
    execute, EnforcementRequirement, EnvironmentPolicy, ExecutionRequest,
    NetworkMode, ResourceLimits,
};
use std::path::PathBuf;

let request = ExecutionRequest {
    executable: PathBuf::from("/usr/bin/printf"),
    args: vec!["hello\\n".into()],
    environment: EnvironmentPolicy::default(),
    working_directory: None,
    filesystem: None,
    network: NetworkMode::Host,
    limits: ResourceLimits::default(),
    enforcement: EnforcementRequirement::BestEffort,
};

let output = execute(&request, b"").await?;
assert!(output.success);
```

For a new integration, inspect `platform_capabilities()` before requiring
optional behavior such as native filesystem isolation, process adoption, PTY,
or kernel resource limits.

## Run the tests

```bash
cargo fmt --all -- --check
cargo test --all-targets -- --test-threads=1
cargo clippy --all-targets -- -D warnings
```

Platform-specific filesystem and network tests must execute on their native
operating system. Cross-compilation checks the API bindings but does not
replace native integration testing.
