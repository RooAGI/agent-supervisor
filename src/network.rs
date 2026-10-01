//! Native network restriction hooks.

#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
#[cfg(target_os = "linux")]
use tokio::process::Command;

#[cfg(target_os = "linux")]
pub(crate) fn prepare_command(command: &mut Command, mode: crate::NetworkMode) -> io::Result<()> {
    if mode == crate::NetworkMode::Host {
        return Ok(());
    }

    let filter = build_filter()?;
    unsafe {
        command.as_std_mut().pre_exec(move || {
            // Only the standard streams are intentionally inherited by the
            // supervisor. Closing the rest prevents an already-open IP socket
            // from bypassing the socket(2) filter.
            for fd in 3..1024 {
                // SAFETY: closing an unowned descriptor in the child is exactly
                // the purpose of this pre-exec hook; EBADF is harmless.
                libc::close(fd);
            }
            // Seccomp filters require no_new_privs for an unprivileged process.
            // SAFETY: this only makes privilege escalation harder for the child.
            let result = libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
            seccompiler::apply_filter(&filter)
                .map_err(|error| io::Error::other(format!("install network filter: {error}")))
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn prepare_command(
    _command: &mut tokio::process::Command,
    _mode: crate::NetworkMode,
) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn build_filter() -> io::Result<seccompiler::BpfProgram> {
    use seccompiler::{
        SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter,
        SeccompRule, TargetArch,
    };

    // AF_UNIX is the only family needed for local IPC. Deny every other
    // family so newly-added Linux families cannot silently bypass the policy.
    let non_local_family = SeccompRule::new(vec![SeccompCondition::new(
        0,
        SeccompCmpArgLen::Dword,
        SeccompCmpOp::Ne,
        libc::AF_UNIX as u64,
    )
    .map_err(|error| io::Error::other(error.to_string()))?])
    .map_err(|error| io::Error::other(error.to_string()))?;
    #[allow(clippy::unnecessary_cast)]
    let socket_syscall = libc::SYS_socket as i64;
    #[allow(clippy::unnecessary_cast)]
    let socketpair_syscall = libc::SYS_socketpair as i64;
    let rules = BTreeMap::from([
        (socket_syscall, vec![non_local_family.clone()]),
        (socketpair_syscall, vec![non_local_family]),
    ]);
    let arch: TargetArch = std::env::consts::ARCH
        .try_into()
        .map_err(|error| io::Error::other(format!("{error:?}")))?;
    SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EPERM as u32),
        arch,
    )
    .map_err(|error| io::Error::other(error.to_string()))?
    .try_into()
    .map_err(|error: seccompiler::BackendError| io::Error::other(error.to_string()))
}
