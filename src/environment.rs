use crate::SandboxError;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use tokio::process::Command;

/// Exact environment released to a child process. Ambient inheritance is
/// disabled unless individual names are explicitly allowlisted by the host.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentPolicy {
    pub inherit: BTreeSet<String>,
    pub variables: BTreeMap<String, String>,
    pub executable_search_paths: Vec<PathBuf>,
}

pub fn apply_environment_policy(
    command: &mut Command,
    policy: &EnvironmentPolicy,
) -> Result<(), SandboxError> {
    validate_environment_policy(policy)?;
    command.env_clear();

    for name in &policy.inherit {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    for (name, value) in &policy.variables {
        command.env(name, value);
    }
    if !policy.executable_search_paths.is_empty() {
        if policy
            .executable_search_paths
            .iter()
            .any(|path| !path.is_absolute())
        {
            return Err(SandboxError::relative_search_path());
        }
        let path = std::env::join_paths(&policy.executable_search_paths)
            .map_err(|_| SandboxError::invalid_search_path())?;
        command.env("PATH", path);
    }
    Ok(())
}

/// Validates environment variable names and executable search paths without
/// starting a process.
pub fn validate_environment_policy(policy: &EnvironmentPolicy) -> Result<(), SandboxError> {
    for name in policy.inherit.iter().chain(policy.variables.keys()) {
        validate_environment_name(name)?;
    }
    if policy
        .executable_search_paths
        .iter()
        .any(|path| !path.is_absolute())
    {
        return Err(SandboxError::relative_search_path());
    }
    Ok(())
}

fn validate_environment_name(name: &str) -> Result<(), SandboxError> {
    if name.is_empty() || name.contains('=') || name.contains('\0') {
        return Err(SandboxError::invalid_environment_name());
    }
    if is_forbidden_environment_variable(name) {
        return Err(SandboxError::forbidden_environment_variable());
    }
    Ok(())
}

fn is_forbidden_environment_variable(name: &str) -> bool {
    matches!(
        name,
        "LD_PRELOAD" | "LD_LIBRARY_PATH" | "LD_AUDIT" | "LD_DEBUG"
    ) || name.starts_with("DYLD_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn rejects_dynamic_loader_environment_variables() {
        let mut command = Command::new("/usr/bin/true");
        let policy = EnvironmentPolicy {
            variables: BTreeMap::from([(
                if cfg!(target_os = "macos") {
                    "DYLD_INSERT_LIBRARIES".to_owned()
                } else {
                    "LD_PRELOAD".to_owned()
                },
                OsString::from("/tmp/attacker.dylib")
                    .to_string_lossy()
                    .into_owned(),
            )]),
            ..EnvironmentPolicy::default()
        };

        let error = apply_environment_policy(&mut command, &policy).unwrap_err();
        assert_eq!(error.code(), "forbidden_environment_variable");
    }
}
