//! Provider-neutral filesystem authority for a sandboxed child.

use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use tokio::process::Command;

/// Filesystem operations that can be granted to a child process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemAccess {
    Read,
    Write,
    /// Append-only access. The process backend rejects an append-only grant
    /// unless `Write` is also explicitly granted, because Landlock cannot
    /// enforce `O_APPEND` for an arbitrary child process.
    Append,
}

/// Access to a directory tree rooted at `root`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FilesystemGrant {
    pub root: PathBuf,
    pub access: Vec<FilesystemAccess>,
}

/// An immutable, default-deny filesystem policy for one child process.
/// `None` on [`crate::ExecutionRequest`] means no filesystem restriction;
/// `Some` with no grants means the child has no filesystem grants.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FilesystemPolicy {
    #[serde(default)]
    pub grants: Vec<FilesystemGrant>,
}

impl FilesystemPolicy {
    /// Creates an explicit default-deny policy.
    ///
    /// This is intentionally different from `None` on [`crate::ExecutionRequest`],
    /// which means unrestricted access for compatibility with existing callers.
    pub fn deny_all() -> Self {
        Self::default()
    }

    pub fn new(grants: Vec<FilesystemGrant>) -> Self {
        Self { grants }
    }

    pub fn is_restricted(&self) -> bool {
        true
    }

    /// Validate this policy against the current host before spawning a child.
    /// The process APIs perform the same check automatically; this method is
    /// useful to hosts that want to reject an invalid configuration earlier.
    pub fn validate(&self) -> io::Result<()> {
        validate_policy(self)
    }
}

/// Validates policy inputs in the parent before a child is spawned.
pub(crate) fn validate_policy(policy: &FilesystemPolicy) -> io::Result<()> {
    for grant in &policy.grants {
        if !grant.root.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "filesystem grant must be absolute: {}",
                    grant.root.display()
                ),
            ));
        }
        if grant.access.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "filesystem grant has no access rights: {}",
                    grant.root.display()
                ),
            ));
        }
        if grant.access.contains(&FilesystemAccess::Append)
            && !grant.access.contains(&FilesystemAccess::Write)
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "append-only filesystem access is not enforceable for a child process; grant write explicitly: {}",
                    grant.root.display()
                ),
            ));
        }
    }

    validate_platform_support()
}

#[cfg(target_os = "linux")]
fn validate_platform_support() -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn validate_platform_support() -> io::Result<()> {
    let launcher = PathBuf::from("/usr/bin/sandbox-exec");
    if launcher.is_file() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "macOS filesystem isolation requires /usr/bin/sandbox-exec",
        ))
    }
}

#[cfg(windows)]
fn validate_platform_support() -> io::Result<()> {
    Ok(())
}

#[cfg(all(not(target_os = "linux"), not(target_os = "macos"), not(windows)))]
fn validate_platform_support() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem isolation is not implemented on this platform",
    ))
}

/// Wrap a macOS command in a generated default-deny legacy Seatbelt profile before
/// the caller adds stdio, environment, and lifecycle configuration. This is
/// kept separate from `prepare_command` because Tokio does not expose a
/// program setter after a command has been configured.
#[cfg(target_os = "macos")]
pub(crate) fn wrap_command(
    command: &mut Command,
    policy: &FilesystemPolicy,
    network: crate::NetworkMode,
) -> io::Result<()> {
    use std::mem;
    validate_policy(policy)?;
    let program = command.as_std().get_program().to_owned();
    let args = command
        .as_std()
        .get_args()
        .map(std::ffi::OsStr::to_owned)
        .collect::<Vec<_>>();
    let _original = mem::replace(command, Command::new("/usr/bin/sandbox-exec"));
    let profile = macos_profile(policy, network)?;
    command.arg("-p").arg(profile).arg(program).args(args);
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn wrap_command(
    _command: &mut Command,
    _policy: &FilesystemPolicy,
    _network: crate::NetworkMode,
) -> io::Result<()> {
    Ok(())
}

/// Apply the Seatbelt boundary for a network-restricted command that did not
/// request explicit filesystem grants. Seatbelt is also the macOS mechanism
/// that removes IP networking; the default-deny filesystem is intentional for
/// this restrictive mode.
#[cfg(target_os = "macos")]
pub(crate) fn wrap_network_command(command: &mut Command) -> io::Result<()> {
    wrap_command(
        command,
        &FilesystemPolicy::deny_all(),
        crate::NetworkMode::Disabled,
    )
}

#[cfg(not(target_os = "macos"))]
pub(crate) fn wrap_network_command(_command: &mut Command) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_profile(policy: &FilesystemPolicy, network: crate::NetworkMode) -> io::Result<String> {
    let mut profile = String::from(
        "(version 1)\n\
         (import \"system.sb\")\n\
         (deny default)\n\
         (allow process-fork)\n\
         (allow process-exec)\n\
         (allow signal (target self))\n\
         (allow sysctl-read)\n\
         (allow file-read-metadata)\n\
         (allow file-read* file-test-existence file-map-executable (subpath \"/System\"))\n\
         (allow file-read* file-test-existence file-map-executable (subpath \"/usr\"))\n\
         (allow file-read* file-test-existence file-map-executable (subpath \"/bin\"))\n\
         (allow file-read* file-test-existence file-map-executable (subpath \"/sbin\"))\n\
         (allow file-read* file-test-existence (subpath \"/dev\"))\n\
         (allow file-read* file-test-existence (subpath \"/private/var/db\"))\n\
         ",
    );
    if network == crate::NetworkMode::Host {
        profile.push_str("(allow network-outbound)\n(allow network-inbound)\n");
    } else {
        profile.push_str(
            "(allow network-outbound (remote unix))\n(allow network-inbound (local unix))\n",
        );
    }

    for grant in &policy.grants {
        let root = macos_profile_path(&grant.root)?;
        let readable = grant.access.iter().any(|access| {
            matches!(
                access,
                FilesystemAccess::Read | FilesystemAccess::Write | FilesystemAccess::Append
            )
        });
        if readable {
            profile
                .push_str("(allow file-read* file-test-existence file-map-executable (subpath \"");
            profile.push_str(&root);
            profile.push_str("\"))\n");
        }
        if grant.access.contains(&FilesystemAccess::Write) {
            profile.push_str("(allow file-write* (subpath \"");
            profile.push_str(&root);
            profile.push_str("\"))\n");
        }
    }
    Ok(profile)
}

#[cfg(target_os = "macos")]
fn macos_profile_path(path: &std::path::Path) -> io::Result<String> {
    let path = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "macOS filesystem grants must contain valid UTF-8 paths",
        )
    })?;
    let mut escaped = String::with_capacity(path.len());
    for character in path.chars() {
        match character {
            '\\' | '"' => {
                escaped.push('\\');
                escaped.push(character);
            }
            '\n' | '\r' | '\0' => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "macOS filesystem grants cannot contain control characters",
                ));
            }
            _ => escaped.push(character),
        }
    }
    Ok(escaped)
}

/// Installs this policy in the child immediately before it executes.
///
/// The policy is deliberately applied in the child pre-exec hook: the parent
/// runtime remains unrestricted, while every descendant of the child inherits
/// the restriction. Unsupported platforms fail closed rather than silently
/// treating a requested policy as advisory.
#[cfg(target_os = "linux")]
pub(crate) fn prepare_command(command: &mut Command, policy: &FilesystemPolicy) {
    let policy = policy.clone();
    unsafe {
        command.pre_exec(move || install_landlock(&policy));
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn prepare_command(_command: &mut tokio::process::Command, _policy: &FilesystemPolicy) {}

#[cfg(target_os = "linux")]
fn install_landlock(policy: &FilesystemPolicy) -> io::Result<()> {
    use landlock::{
        AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
        RulesetCreatedAttr, ABI,
    };

    const ABI_VERSION: ABI = ABI::V1;
    let read = AccessFs::from_read(ABI_VERSION);
    let write = AccessFs::from_write(ABI_VERSION);
    let handled = read | write;
    let mut ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(handled)
        .map_err(landlock_error)?
        .create()
        .map_err(landlock_error)?;

    for grant in &policy.grants {
        let mut access = landlock::BitFlags::EMPTY;
        for permission in &grant.access {
            access |= match permission {
                FilesystemAccess::Read => read,
                // Validation requires Write alongside Append, so this branch
                // intentionally grants the explicitly requested write right.
                FilesystemAccess::Write | FilesystemAccess::Append => write,
            };
        }
        if access.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "filesystem grant has no access rights: {}",
                    grant.root.display()
                ),
            ));
        }
        let path = PathFd::new(&grant.root).map_err(landlock_error)?;
        ruleset = ruleset
            .add_rule(PathBeneath::new(path, access))
            .map_err(landlock_error)?;
    }

    ruleset.restrict_self().map(|_| ()).map_err(landlock_error)
}

#[cfg(target_os = "linux")]
fn landlock_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_must_be_absolute_and_non_empty() {
        let relative = FilesystemPolicy::new(vec![FilesystemGrant {
            root: PathBuf::from("workspace"),
            access: vec![FilesystemAccess::Read],
        }]);
        assert_eq!(
            validate_policy(&relative).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        let empty = FilesystemPolicy::new(vec![FilesystemGrant {
            root: PathBuf::from("/tmp/workspace"),
            access: Vec::new(),
        }]);
        assert_eq!(
            validate_policy(&empty).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn empty_policy_is_explicit_default_deny() {
        assert_eq!(FilesystemPolicy::deny_all(), FilesystemPolicy::default());
        assert!(FilesystemPolicy::deny_all().is_restricted());
    }

    #[test]
    fn append_only_grants_fail_closed() {
        let policy = FilesystemPolicy::new(vec![FilesystemGrant {
            root: std::env::temp_dir().join("workspace"),
            access: vec![FilesystemAccess::Append],
        }]);
        assert_eq!(
            policy.validate().unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[test]
    fn policy_uses_stable_wire_names() {
        let policy = FilesystemPolicy::new(vec![FilesystemGrant {
            root: PathBuf::from("/workspace"),
            access: vec![FilesystemAccess::Read, FilesystemAccess::Append],
        }]);
        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(value["grants"][0]["root"], "/workspace");
        assert_eq!(
            value["grants"][0]["access"],
            serde_json::json!(["read", "append"])
        );
        let decoded: FilesystemPolicy = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, policy);
    }
}
