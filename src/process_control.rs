//! Owned process-tree containment primitives.
//!
//! This module intentionally exposes only the lifecycle operations needed by
//! the sandbox. Policy, reporting, and supervisor identity remain in the
//! parent module. Each spawned child owns an `Arc<ProcessContainer>` and the
//! runtime supervisor keeps another reference while the child is registered.

use crate::Enforcement;
use crate::{FilesystemPolicy, ProcessMember, ProcessSignal, ResourceLimits, ResourceStats};
use std::io;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(unix, windows))]
use std::sync::Mutex;
use tokio::process::{Child, Command};

#[cfg(target_os = "linux")]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
#[cfg(any(unix, windows))]
use std::path::Path;

#[derive(Debug)]
pub(crate) struct ProcessContainer {
    #[cfg(windows)]
    job: windows_job::JobObject,
    #[cfg(any(unix, windows))]
    process_id: Mutex<u32>,
    #[cfg(target_os = "linux")]
    process_start_time: Mutex<Option<u64>>,
    #[cfg(target_os = "linux")]
    cgroup: Option<linux_cgroup::Cgroup>,
}

// Windows kernel handles are process-wide and may be used concurrently. The
// surrounding container serializes mutable Rust state with Mutex; the job
// object itself is safe to reference from supervisor and worker threads.
#[cfg(windows)]
unsafe impl Send for ProcessContainer {}
#[cfg(windows)]
unsafe impl Sync for ProcessContainer {}

impl ProcessContainer {
    pub(crate) fn new() -> io::Result<Self> {
        #[cfg(windows)]
        {
            Ok(Self {
                job: windows_job::JobObject::new()?,
                process_id: Mutex::new(0),
            })
        }

        #[cfg(unix)]
        {
            Ok(Self {
                process_id: Mutex::new(0),
                #[cfg(target_os = "linux")]
                process_start_time: Mutex::new(None),
                #[cfg(target_os = "linux")]
                cgroup: linux_cgroup::Cgroup::try_create(),
            })
        }
    }

    pub(crate) fn prepare_command_in_group(
        &self,
        command: &mut Command,
        _group_id: Option<u32>,
        limits: &ResourceLimits,
        filesystem: Option<&FilesystemPolicy>,
    ) {
        let _ = limits;

        #[cfg(unix)]
        command.process_group(_group_id.map(|id| id as i32).unwrap_or(0));

        #[cfg(target_os = "linux")]
        install_parent_death_signal(command);

        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            cgroup.prepare_command(command);
        }

        #[cfg(windows)]
        self.job.prepare_command(command);

        if let Some(policy) = filesystem {
            crate::filesystem::prepare_command(command, policy);
        }
    }

    #[allow(unreachable_code)]
    pub(crate) fn apply_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        if !limits.has_kernel_limits() {
            return Ok(());
        }

        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            return cgroup.apply_limits(limits);
        }

        #[cfg(windows)]
        return self.job.apply_limits(limits);

        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kernel resource limits are unavailable for this containment backend",
        ))
    }

    pub(crate) fn enforcement(&self) -> Enforcement {
        #[cfg(windows)]
        {
            Enforcement::Enforced
        }

        #[cfg(target_os = "linux")]
        {
            if self.cgroup.is_some() {
                Enforcement::Enforced
            } else {
                Enforcement::Degraded
            }
        }

        #[cfg(target_os = "macos")]
        {
            Enforcement::Degraded
        }

        #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
        {
            Enforcement::Degraded
        }

        #[cfg(not(any(unix, windows)))]
        {
            Enforcement::Unavailable
        }
    }

    pub(crate) fn enforcement_for_filesystem(
        &self,
        #[cfg(target_os = "macos")] _filesystem_requested: bool,
        #[cfg(not(target_os = "macos"))] _filesystem_requested: bool,
    ) -> Enforcement {
        self.enforcement()
    }

    pub(crate) fn attach(&self, child: &mut Child) -> io::Result<()> {
        #[cfg(windows)]
        self.job.attach(child)?;

        #[cfg(unix)]
        {
            let _ = child;
        }
        #[cfg(target_os = "macos")]
        {
            let _ = child;
        }
        Ok(())
    }

    #[cfg(windows)]
    pub(crate) fn attach_process_id(&self, process_id: u32) -> io::Result<()> {
        self.job.attach_process_id(process_id)
    }

    #[cfg(windows)]
    pub(crate) fn attach_native_process(
        &self,
        process_handle: usize,
        process_id: u32,
    ) -> io::Result<()> {
        self.job.attach_native_process(process_handle, process_id)
    }

    pub(crate) fn set_process_id(&self, process_id: Option<u32>) -> io::Result<()> {
        let process_id = process_id
            .filter(|process_id| *process_id > 0)
            .ok_or_else(|| io::Error::other("child process identifier unavailable"))?;

        #[cfg(unix)]
        {
            let mut current = self
                .process_id
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *current == 0 {
                *current = process_id;
                #[cfg(target_os = "linux")]
                {
                    *self
                        .process_start_time
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        linux_process_start_time(process_id);
                }
            }
        }

        #[cfg(windows)]
        {
            let mut current = self
                .process_id
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if *current == 0 {
                *current = process_id;
            }
        }
        Ok(())
    }

    #[cfg(any(unix, windows))]
    #[allow(unreachable_code)]
    pub(crate) fn adopt_process(
        &self,
        process_id: u32,
        expected_executable: Option<&Path>,
    ) -> io::Result<()> {
        if process_id == 0 || process_id == std::process::id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid process identifier for adoption",
            ));
        }
        #[cfg(windows)]
        {
            self.job.adopt_process(process_id, expected_executable)?;
            self.set_process_id(Some(process_id))?;
            return Ok(());
        }

        #[cfg(target_os = "macos")]
        {
            let process_group = unsafe { libc::getpgid(process_id as libc::pid_t) };
            if process_group < 0 || process_group as u32 != process_id {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to adopt a process that does not lead its own process group",
                ));
            }
            if let Some(expected) = expected_executable {
                let actual = macos_member(process_id as libc::pid_t)
                    .and_then(|member| member.executable)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "cannot verify adopted executable identity",
                        )
                    })?;
                let expected = std::fs::canonicalize(expected)?;
                let actual = std::fs::canonicalize(actual)?;
                if actual.as_path() != expected.as_path() {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "adopted executable does not match the expected identity (expected {}, actual {})",
                            expected.display(),
                            actual.display()
                        ),
                    ));
                }
            }
            self.set_process_id(Some(process_id))?;
            return Ok(());
        }

        #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
        {
            if expected_executable.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "executable identity checks for adoption are unavailable on this Unix platform",
                ));
            }
            let process_group = unsafe { libc::getpgid(process_id as libc::pid_t) };
            if process_group < 0 || process_group as u32 != process_id {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "refusing to adopt a process that does not lead its own process group",
                ));
            }
            self.set_process_id(Some(process_id))?;
            return Ok(());
        }

        #[cfg(target_os = "linux")]
        let proc_dir = Path::new("/proc").join(process_id.to_string());
        #[cfg(target_os = "linux")]
        let stat = std::fs::read_to_string(proc_dir.join("stat"))?;
        #[cfg(target_os = "linux")]
        let (_, fields) = stat
            .rsplit_once(") ")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid process stat"))?;
        #[cfg(target_os = "linux")]
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        #[cfg(target_os = "linux")]
        let process_group = fields
            .get(2)
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process group"))?;
        #[cfg(target_os = "linux")]
        if process_group != process_id {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "refusing to adopt a process that does not lead its own process group",
            ));
        }
        #[cfg(target_os = "linux")]
        if let Some(expected) = expected_executable {
            let expected = std::fs::canonicalize(expected)?;
            let actual = std::fs::canonicalize(proc_dir.join("exe"))?;
            if actual.as_path() != expected.as_path() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "adopted executable does not match the expected identity for pid {} (caller pid {}): expected {}, actual {}",
                        process_id,
                        std::process::id(),
                        expected.display(),
                        actual.display()
                    ),
                ));
            }
        }
        #[cfg(target_os = "linux")]
        self.set_process_id(Some(process_id))?;
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            cgroup.adopt(process_id)?;
        }
        Ok(())
    }

    #[allow(unreachable_code)]
    pub(crate) fn terminate(&self) -> io::Result<()> {
        #[cfg(windows)]
        return self.job.terminate();

        #[cfg(unix)]
        let process_id = *self
            .process_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        #[cfg(unix)]
        if process_id > 0 {
            if let Ok(process_id) = i32::try_from(process_id) {
                #[cfg(target_os = "linux")]
                if let Some(expected) = *self
                    .process_start_time
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                {
                    match linux_process_start_time(process_id as u32) {
                        Some(actual) if actual == expected => {}
                        Some(_) => {
                            return Err(io::Error::other(
                                "refusing to terminate a reused process identifier",
                            ));
                        }
                        None => {
                            return Err(io::Error::other(
                                "refusing to terminate an unverified process identifier",
                            ));
                        }
                    }
                }
                // The process is created as the leader of a fresh process group.
                // This is best-effort during Drop; explicit operations report at
                // the higher-level API where that becomes observable.
                unsafe {
                    let result = libc::kill(-process_id, libc::SIGKILL);
                    if result != 0 {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::ESRCH) {
                            return Err(error);
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            cgroup.kill()?;
        }

        Ok(())
    }

    pub(crate) fn request_graceful_stop(&self) -> io::Result<()> {
        #[cfg(unix)]
        let process_id = *self
            .process_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        #[cfg(unix)]
        if process_id > 0 {
            if let Ok(process_id) = i32::try_from(process_id) {
                unsafe {
                    let result = libc::kill(-process_id, libc::SIGTERM);
                    if result != 0 {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::ESRCH) {
                            return Err(error);
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            cgroup.signal_members(libc::SIGTERM)?;
        }

        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn signal(&self, signal: ProcessSignal) -> io::Result<()> {
        match signal {
            ProcessSignal::Terminate => self.request_graceful_stop(),
            ProcessSignal::Kill => self.terminate(),
            ProcessSignal::Hangup => self.signal_unix(libc::SIGHUP),
            ProcessSignal::Interrupt => self.signal_unix(libc::SIGINT),
        }
    }

    #[cfg(windows)]
    pub(crate) fn signal(&self, signal: ProcessSignal) -> io::Result<()> {
        match signal {
            ProcessSignal::Kill => self.terminate(),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "soft signals are unavailable on Windows",
            )),
        }
    }

    #[cfg(unix)]
    fn signal_unix(&self, signal: i32) -> io::Result<()> {
        let process_id = *self.process_id.lock().unwrap_or_else(|p| p.into_inner());
        if process_id == 0 {
            return Ok(());
        }
        let result = unsafe { libc::kill(-(process_id as i32), signal) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error);
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            cgroup.signal_members(signal)?;
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            return cgroup.freeze(true);
        }
        self.signal_unix(libc::SIGSTOP)
    }

    #[cfg(unix)]
    pub(crate) fn resume(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            return cgroup.freeze(false);
        }
        self.signal_unix(libc::SIGCONT)
    }

    #[cfg(windows)]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "suspend is unavailable on Windows",
        ))
    }

    #[cfg(windows)]
    pub(crate) fn resume(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "resume is unavailable on Windows",
        ))
    }

    #[allow(clippy::needless_return)]
    pub(crate) fn is_alive(&self) -> bool {
        #[cfg(windows)]
        {
            return self.job.is_alive();
        }

        #[cfg(unix)]
        let process_id = *self
            .process_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        #[cfg(unix)]
        if process_id == 0 {
            return false;
        } else {
            #[cfg(target_os = "linux")]
            if let Some(expected) = *self
                .process_start_time
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
            {
                return linux_process_is_alive(process_id)
                    && linux_process_start_time(process_id)
                        .map(|actual| actual == expected)
                        .unwrap_or(false);
            }

            unsafe {
                return libc::kill(process_id as i32, 0) == 0
                    || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
            }
        }

        #[cfg(not(any(unix, windows)))]
        false
    }

    #[allow(clippy::needless_return)]
    pub(crate) fn members(&self) -> io::Result<Vec<ProcessMember>> {
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            return cgroup.members();
        }

        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let process_id = *self.process_id.lock().unwrap_or_else(|p| p.into_inner());
            if process_id == 0 {
                return Ok(Vec::new());
            }
            return Ok(vec![unix_member(process_id)]);
        }

        #[cfg(windows)]
        return self.job.members();

        #[cfg(target_os = "macos")]
        {
            let process_id = *self.process_id.lock().unwrap_or_else(|p| p.into_inner());
            return macos_members(process_id);
        }

        #[cfg(not(any(unix, windows)))]
        Ok(Vec::new())
    }

    pub(crate) fn stats(&self) -> io::Result<ResourceStats> {
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            return cgroup.stats();
        }

        #[cfg(windows)]
        {
            self.job.stats()
        }

        #[cfg(target_os = "macos")]
        {
            let process_id = *self.process_id.lock().unwrap_or_else(|p| p.into_inner());
            macos_stats(process_id)
        }

        #[cfg(all(unix, not(target_os = "macos")))]
        {
            Ok(ResourceStats::default())
        }
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn unix_member(process_id: u32) -> ProcessMember {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat")).ok();
        let (parent_process_id, start_time_ticks) = stat
            .as_deref()
            .and_then(|value| value.rsplit_once(") "))
            .map(|(_, fields)| {
                let fields = fields.split_whitespace().collect::<Vec<_>>();
                (
                    fields.get(1).and_then(|value| value.parse().ok()),
                    fields.get(19).and_then(|value| value.parse().ok()),
                )
            })
            .unwrap_or((None, None));
        let executable = std::fs::canonicalize(format!("/proc/{process_id}/exe")).ok();
        return ProcessMember {
            process_id,
            parent_process_id,
            start_time_ticks,
            executable,
            alive: linux_process_is_alive(process_id),
        };
    }
    #[allow(unreachable_code)]
    ProcessMember {
        process_id,
        parent_process_id: None,
        start_time_ticks: None,
        executable: None,
        alive: unsafe { libc::kill(process_id as i32, 0) == 0 },
    }
}

#[cfg(target_os = "macos")]
fn macos_members(process_id: u32) -> io::Result<Vec<ProcessMember>> {
    if process_id == 0 {
        return Ok(Vec::new());
    }
    let mut capacity = 32_usize;
    let process_ids = loop {
        let mut ids = vec![0 as libc::pid_t; capacity];
        let count = unsafe {
            libc::proc_listpgrppids(
                process_id as libc::pid_t,
                ids.as_mut_ptr().cast::<libc::c_void>(),
                (capacity * std::mem::size_of::<libc::pid_t>()) as libc::c_int,
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = count as usize;
        if count < capacity || capacity >= 65_536 {
            ids.truncate(count.min(capacity));
            break ids;
        }
        capacity = capacity.saturating_mul(2).min(65_536);
    };

    Ok(process_ids
        .into_iter()
        .filter(|process_id| *process_id > 0)
        .filter_map(macos_member)
        .collect())
}

#[cfg(target_os = "macos")]
fn macos_member(process_id: libc::pid_t) -> Option<ProcessMember> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let result = unsafe {
        libc::proc_pidinfo(
            process_id,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast::<libc::c_void>(),
            std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int,
        )
    };
    if result < std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int {
        return None;
    }
    let info = unsafe { info.assume_init() };
    let mut path = vec![0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let path_length = unsafe {
        libc::proc_pidpath(
            process_id,
            path.as_mut_ptr().cast::<libc::c_void>(),
            path.len() as u32,
        )
    };
    let executable = (path_length > 0).then(|| {
        std::path::PathBuf::from(
            String::from_utf8_lossy(&path[..path_length as usize]).into_owned(),
        )
    });
    let start_time_ticks = info
        .pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec));
    Some(ProcessMember {
        process_id: process_id as u32,
        parent_process_id: (info.pbi_ppid > 0).then_some(info.pbi_ppid),
        start_time_ticks,
        executable,
        alive: info.pbi_status != 0,
    })
}

#[cfg(target_os = "macos")]
fn macos_stats(process_id: u32) -> io::Result<ResourceStats> {
    let members = macos_members(process_id)?;
    let mut memory_current_bytes = 0_u64;
    let mut cpu_usage_micros = 0_u64;
    let mut observed = 0_u64;
    for member in &members {
        let mut info = std::mem::MaybeUninit::<libc::proc_taskallinfo>::zeroed();
        let result = unsafe {
            libc::proc_pidinfo(
                member.process_id as libc::pid_t,
                libc::PROC_PIDTASKALLINFO,
                0,
                info.as_mut_ptr().cast::<libc::c_void>(),
                std::mem::size_of::<libc::proc_taskallinfo>() as libc::c_int,
            )
        };
        if result < std::mem::size_of::<libc::proc_taskallinfo>() as libc::c_int {
            continue;
        }
        let info = unsafe { info.assume_init() };
        memory_current_bytes = memory_current_bytes.saturating_add(info.ptinfo.pti_resident_size);
        cpu_usage_micros = cpu_usage_micros.saturating_add(
            info.ptinfo
                .pti_total_user
                .saturating_add(info.ptinfo.pti_total_system)
                / 1_000,
        );
        observed += 1;
    }
    Ok(ResourceStats {
        active_processes: Some(members.iter().filter(|member| member.alive).count() as u64),
        memory_current_bytes: (observed > 0).then_some(memory_current_bytes),
        memory_peak_bytes: None,
        cpu_usage_micros: (observed > 0).then_some(cpu_usage_micros),
    })
}

#[cfg(target_os = "linux")]
fn linux_process_start_time(process_id: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat")).ok()?;
    let (_, fields) = stat.rsplit_once(") ")?;
    fields.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(target_os = "linux")]
fn linux_process_is_alive(process_id: u32) -> bool {
    let stat = match std::fs::read_to_string(format!("/proc/{process_id}/stat")) {
        Ok(stat) => stat,
        Err(_) => return false,
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return false;
    };
    fields.split_whitespace().next() != Some("Z")
}

#[cfg(target_os = "linux")]
fn install_parent_death_signal(command: &mut Command) {
    let parent_process_id = unsafe { libc::getpid() };
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() != parent_process_id {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "sandbox parent exited before child setup completed",
                ));
            }
            Ok(())
        });
    }
}

#[cfg(target_os = "linux")]
mod linux_cgroup {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    pub(super) struct Cgroup {
        path: PathBuf,
    }

    impl Cgroup {
        pub(super) fn try_create() -> Option<Self> {
            let contents = std::fs::read_to_string("/proc/self/cgroup").ok()?;
            let relative = contents
                .lines()
                .find_map(|line| line.strip_prefix("0::"))?
                .trim();
            if relative.is_empty() || !relative.starts_with('/') || relative.contains("..") {
                return None;
            }
            let parent = PathBuf::from("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
            if !parent.join("cgroup.controllers").is_file() {
                return None;
            }
            let name = format!(
                "rooagi-{}-{}",
                std::process::id(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            );
            let path = parent.join(name);
            match std::fs::create_dir(&path) {
                Ok(()) => Some(Self { path }),
                Err(_) => None,
            }
        }

        pub(super) fn prepare_command(&self, command: &mut Command) {
            let procs = self.path.join("cgroup.procs");
            let Ok(procs) = CString::new(procs.as_os_str().as_bytes()) else {
                return;
            };
            unsafe {
                command.as_std_mut().pre_exec(move || {
                    let fd = libc::open(procs.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
                    if fd < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    let pid = std::process::id().to_string();
                    let bytes = pid.as_bytes();
                    let mut written = 0;
                    while written < bytes.len() {
                        let result = libc::write(
                            fd,
                            bytes[written..].as_ptr().cast(),
                            bytes.len() - written,
                        );
                        if result < 0 {
                            let error = io::Error::last_os_error();
                            if error.raw_os_error() == Some(libc::EINTR) {
                                continue;
                            }
                            libc::close(fd);
                            return Err(error);
                        }
                        if result == 0 {
                            libc::close(fd);
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "cgroup.procs write made no progress",
                            ));
                        }
                        written += result as usize;
                    }
                    if libc::close(fd) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }

        pub(super) fn adopt(&self, process_id: u32) -> io::Result<()> {
            write_limit(&self.path.join("cgroup.procs"), process_id.to_string())
        }

        pub(super) fn kill(&self) -> io::Result<()> {
            let kill_path = self.path.join("cgroup.kill");
            if let Err(error) = std::fs::write(&kill_path, b"1") {
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(error);
                }
                self.kill_members()?;
            }
            self.remove_or_retry();
            Ok(())
        }

        fn kill_members(&self) -> io::Result<()> {
            let members = std::fs::read_to_string(self.path.join("cgroup.procs"))?;
            for member in members
                .lines()
                .filter_map(|line| line.trim().parse::<i32>().ok())
            {
                let result = unsafe { libc::kill(member, libc::SIGKILL) };
                if result != 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error);
                    }
                }
            }
            Ok(())
        }

        pub(super) fn signal_members(&self, signal: i32) -> io::Result<()> {
            let members = std::fs::read_to_string(self.path.join("cgroup.procs"))?;
            for member in members
                .lines()
                .filter_map(|line| line.trim().parse::<i32>().ok())
            {
                let result = unsafe { libc::kill(member, signal) };
                if result != 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error);
                    }
                }
            }
            Ok(())
        }

        pub(super) fn freeze(&self, frozen: bool) -> io::Result<()> {
            write_limit(
                &self.path.join("cgroup.freeze"),
                if frozen { "1".into() } else { "0".into() },
            )
        }

        pub(super) fn members(&self) -> io::Result<Vec<ProcessMember>> {
            let members = std::fs::read_to_string(self.path.join("cgroup.procs"))?;
            Ok(members
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .map(super::unix_member)
                .collect())
        }

        pub(super) fn stats(&self) -> io::Result<ResourceStats> {
            let active_processes = std::fs::read_to_string(self.path.join("cgroup.procs"))
                .ok()
                .map(|value| value.lines().filter(|line| !line.trim().is_empty()).count() as u64);
            let memory_current_bytes = std::fs::read_to_string(self.path.join("memory.current"))
                .ok()
                .and_then(|value| value.trim().parse().ok());
            let memory_peak_bytes = std::fs::read_to_string(self.path.join("memory.peak"))
                .ok()
                .and_then(|value| value.trim().parse().ok());
            let cpu_usage_micros = std::fs::read_to_string(self.path.join("cpu.stat"))
                .ok()
                .and_then(|value| {
                    value
                        .lines()
                        .find_map(|line| line.strip_prefix("usage_usec ").map(str::to_owned))
                })
                .and_then(|value| value.trim().parse().ok());
            Ok(ResourceStats {
                active_processes,
                memory_current_bytes,
                memory_peak_bytes,
                cpu_usage_micros,
            })
        }

        pub(super) fn apply_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
            if let Some(memory_bytes) = limits.memory_bytes {
                write_limit(&self.path.join("memory.max"), memory_bytes.to_string())?;
            }
            if let Some(max_processes) = limits.max_processes {
                write_limit(&self.path.join("pids.max"), max_processes.to_string())?;
            }
            if let Some(cpu_quota_micros) = limits.cpu_quota_micros {
                write_limit(
                    &self.path.join("cpu.max"),
                    format!("{cpu_quota_micros} 100000"),
                )?;
            }
            Ok(())
        }

        fn remove_or_retry(&self) {
            if std::fs::remove_dir(&self.path).is_ok() {
                return;
            }
            let path = self.path.clone();
            std::thread::Builder::new()
                .name("rooagi-cgroup-cleanup".into())
                .spawn(move || {
                    let mut delay = Duration::from_millis(10);
                    for _ in 0..100 {
                        std::thread::sleep(delay);
                        match std::fs::remove_dir(&path) {
                            Ok(()) => return,
                            Err(error) if error.kind() == io::ErrorKind::NotFound => return,
                            Err(_) => delay = (delay * 2).min(Duration::from_secs(1)),
                        }
                    }
                })
                .ok();
        }
    }

    fn write_limit(path: &std::path::Path, value: String) -> io::Result<()> {
        std::fs::write(path, value).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("unable to apply cgroup limit {}: {error}", path.display()),
            )
        })
    }

    impl Drop for Cgroup {
        fn drop(&mut self) {
            if std::fs::remove_dir(&self.path).is_err() {
                let path = self.path.clone();
                std::thread::Builder::new()
                    .name("rooagi-cgroup-cleanup".into())
                    .spawn(move || {
                        let mut delay = Duration::from_millis(10);
                        for _ in 0..100 {
                            std::thread::sleep(delay);
                            match std::fs::remove_dir(&path) {
                                Ok(()) => return,
                                Err(error) if error.kind() == io::ErrorKind::NotFound => return,
                                Err(_) => delay = (delay * 2).min(Duration::from_secs(1)),
                            }
                        }
                    })
                    .ok();
            }
        }
    }
}

#[cfg(windows)]
mod windows_job {
    use super::*;
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32FirstW, Process32NextW, Thread32First, Thread32Next,
        PROCESSENTRY32W, TH32CS_SNAPPROCESS, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
        JobObjectBasicProcessIdList, JobObjectCpuRateControlInformation,
        JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
        TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
        JOBOBJECT_CPU_RATE_CONTROL_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_CPU_RATE_CONTROL_ENABLE, JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
        JOB_OBJECT_LIMIT_ACTIVE_PROCESS, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOB_OBJECT_LIMIT_PROCESS_MEMORY,
    };
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, OpenThread, QueryFullProcessImageNameW, ResumeThread,
        CREATE_SUSPENDED, PROCESS_NAME_WIN32, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SET_QUOTA,
        PROCESS_TERMINATE, THREAD_SUSPEND_RESUME,
    };

    #[derive(Debug)]
    pub(super) struct JobObject {
        handle: HANDLE,
    }

    impl JobObject {
        pub(super) fn new() -> io::Result<Self> {
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }

            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let ok = unsafe {
                SetInformationJobObject(
                    handle,
                    JobObjectExtendedLimitInformation,
                    (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION)
                        .cast::<std::ffi::c_void>(),
                    std::mem::size_of_val(&limits) as u32,
                )
            };
            if ok == 0 {
                let error = io::Error::last_os_error();
                unsafe { CloseHandle(handle) };
                return Err(error);
            }
            Ok(Self { handle })
        }

        pub(super) fn prepare_command(&self, command: &mut Command) {
            command.creation_flags(CREATE_SUSPENDED);
        }

        pub(super) fn apply_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
            if let Some(cpu_quota_micros) = limits.cpu_quota_micros {
                if !(1..=100_000).contains(&cpu_quota_micros) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows CPU quota must be between 1 and 100000 microseconds per 100ms",
                    ));
                }
                let mut cpu = JOBOBJECT_CPU_RATE_CONTROL_INFORMATION {
                    ControlFlags: JOB_OBJECT_CPU_RATE_CONTROL_ENABLE
                        | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
                    ..Default::default()
                };
                // Job Object CpuRate is hundredths of a percent, while the
                // public request uses the equivalent microsecond budget per
                // 100ms period.
                let rate =
                    ((u128::from(cpu_quota_micros) * 10_000) / 100_000).clamp(1, 10_000) as u32;
                cpu.Anonymous.CpuRate = rate;
                let ok = unsafe {
                    SetInformationJobObject(
                        self.handle,
                        JobObjectCpuRateControlInformation,
                        (&cpu as *const JOBOBJECT_CPU_RATE_CONTROL_INFORMATION)
                            .cast::<std::ffi::c_void>(),
                        std::mem::size_of_val(&cpu) as u32,
                    )
                };
                if ok == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            if let Some(memory_bytes) = limits.memory_bytes {
                info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_PROCESS_MEMORY;
                info.ProcessMemoryLimit = memory_bytes as usize;
            }
            if let Some(max_processes) = limits.max_processes {
                info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
                info.BasicLimitInformation.ActiveProcessLimit = max_processes;
            }
            if info.BasicLimitInformation.LimitFlags == 0 {
                return Ok(());
            }
            let ok = unsafe {
                SetInformationJobObject(
                    self.handle,
                    JobObjectExtendedLimitInformation,
                    (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION)
                        .cast::<std::ffi::c_void>(),
                    std::mem::size_of_val(&info) as u32,
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub(super) fn attach(&self, child: &mut Child) -> io::Result<()> {
            let raw_handle = child
                .raw_handle()
                .ok_or_else(|| io::Error::other("child handle unavailable"))?;
            let process = raw_handle as HANDLE;
            let ok = unsafe { AssignProcessToJobObject(self.handle, process) };
            if ok == 0 {
                let error = io::Error::last_os_error();
                let _ = child.start_kill();
                return Err(error);
            }
            if let Err(error) = resume_process_threads(
                child
                    .id()
                    .ok_or_else(|| io::Error::other("child exited before containment completed"))?,
            ) {
                let _ = unsafe { TerminateJobObject(self.handle, 1) };
                return Err(error);
            }
            Ok(())
        }

        pub(super) fn attach_native_process(
            &self,
            process_handle: usize,
            process_id: u32,
        ) -> io::Result<()> {
            let process = process_handle as HANDLE;
            let ok = unsafe { AssignProcessToJobObject(self.handle, process) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            resume_process_threads(process_id)
        }

        pub(super) fn attach_process_id(&self, process_id: u32) -> io::Result<()> {
            let process =
                unsafe { OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, process_id) };
            if process.is_null() {
                return Err(io::Error::last_os_error());
            }
            let result = unsafe { AssignProcessToJobObject(self.handle, process) };
            let error = if result == 0 {
                Some(io::Error::last_os_error())
            } else {
                None
            };
            unsafe { CloseHandle(process) };
            match error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        pub(super) fn adopt_process(
            &self,
            process_id: u32,
            expected_executable: Option<&Path>,
        ) -> io::Result<()> {
            let (actual_executable, start_time, alive) = process_identity(process_id);
            if !alive {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "cannot adopt an exited or inaccessible process",
                ));
            }
            let start_time = start_time.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "cannot verify adopted process start identity",
                )
            })?;
            if let Some(expected) = expected_executable {
                let expected = std::fs::canonicalize(expected)?;
                let actual = actual_executable.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "cannot verify adopted executable identity",
                    )
                })?;
                let actual = std::fs::canonicalize(&actual)?;
                if actual.as_path() != expected.as_path() {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                        "adopted executable does not match the expected identity (expected {}, actual {})",
                        expected.display(),
                        actual.display()
                    ),
                    ));
                }
            }
            self.attach_process_id(process_id)?;

            let (_, current_start_time, current_alive) = process_identity(process_id);
            if !current_alive || current_start_time != Some(start_time) {
                let _ = self.terminate();
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "adopted process identity changed during attachment",
                ));
            }
            Ok(())
        }

        pub(super) fn terminate(&self) -> io::Result<()> {
            let ok = unsafe { TerminateJobObject(self.handle, 1) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub(super) fn is_alive(&self) -> bool {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            let ok = unsafe {
                QueryInformationJobObject(
                    self.handle,
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION)
                        .cast::<std::ffi::c_void>(),
                    std::mem::size_of_val(&accounting) as u32,
                    std::ptr::null_mut(),
                )
            };
            ok != 0 && accounting.ActiveProcesses > 0
        }

        pub(super) fn members(&self) -> io::Result<Vec<ProcessMember>> {
            // QueryInformationJobObject returns a variable-sized structure.
            // Use an aligned usize buffer so the trailing PID array is safe to
            // read on both 32-bit and 64-bit Windows.
            let mut capacity = 16_usize;
            let process_ids = loop {
                let mut buffer = vec![0_usize; 2 + capacity];
                let ok = unsafe {
                    QueryInformationJobObject(
                        self.handle,
                        JobObjectBasicProcessIdList,
                        buffer.as_mut_ptr().cast::<std::ffi::c_void>(),
                        (8 + capacity * std::mem::size_of::<usize>()) as u32,
                        std::ptr::null_mut(),
                    )
                };
                let assigned = unsafe { *(buffer.as_ptr().cast::<u32>()) } as usize;
                let listed = unsafe { *(buffer.as_ptr().cast::<u32>().add(1)) } as usize;
                if ok != 0 {
                    let count = listed.min(capacity);
                    break (0..count)
                        .map(|index| unsafe { *buffer.as_ptr().add(2 + index) as u32 })
                        .collect::<Vec<_>>();
                }
                if assigned <= capacity || capacity >= 65_536 {
                    return Err(io::Error::last_os_error());
                }
                capacity = capacity.saturating_mul(2).min(65_536);
            };

            let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
            if snapshot == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            let mut entries = std::collections::HashMap::new();
            let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
            entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
            let mut first = true;
            loop {
                let found = unsafe {
                    if first {
                        first = false;
                        Process32FirstW(snapshot, &mut entry)
                    } else {
                        Process32NextW(snapshot, &mut entry)
                    }
                };
                if found == 0 {
                    break;
                }
                let name_end = entry
                    .szExeFile
                    .iter()
                    .position(|character| *character == 0)
                    .unwrap_or(entry.szExeFile.len());
                entries.insert(
                    entry.th32ProcessID,
                    (
                        entry.th32ParentProcessID,
                        String::from_utf16_lossy(&entry.szExeFile[..name_end]),
                    ),
                );
            }
            unsafe { CloseHandle(snapshot) };

            Ok(process_ids
                .into_iter()
                .map(|process_id| {
                    let (parent_process_id, executable) =
                        entries.remove(&process_id).unwrap_or((0, String::new()));
                    let (full_executable, start_time_ticks, alive) = process_identity(process_id);
                    ProcessMember {
                        process_id,
                        parent_process_id: (parent_process_id != 0).then_some(parent_process_id),
                        start_time_ticks,
                        executable: full_executable.or_else(|| {
                            (!executable.is_empty()).then(|| std::path::PathBuf::from(executable))
                        }),
                        alive,
                    }
                })
                .collect())
        }

        pub(super) fn stats(&self) -> io::Result<ResourceStats> {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            let ok = unsafe {
                QueryInformationJobObject(
                    self.handle,
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION)
                        .cast::<std::ffi::c_void>(),
                    std::mem::size_of_val(&accounting) as u32,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(ResourceStats {
                active_processes: Some(accounting.ActiveProcesses as u64),
                ..ResourceStats::default()
            })
        }
    }

    fn process_identity(process_id: u32) -> (Option<std::path::PathBuf>, Option<u64>, bool) {
        let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
        if process.is_null() {
            return (None, None, true);
        }

        let mut path = vec![0_u16; 32_768];
        let mut path_length = path.len() as u32;
        let executable = unsafe {
            (QueryFullProcessImageNameW(
                process,
                PROCESS_NAME_WIN32,
                path.as_mut_ptr(),
                &mut path_length,
            ) != 0)
                .then(|| {
                    std::path::PathBuf::from(String::from_utf16_lossy(
                        &path[..path_length as usize],
                    ))
                })
        };

        let mut creation = FILETIME::default();
        let mut exit = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        let start_time_ticks = unsafe {
            (GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) != 0).then(
                || (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime),
            )
        };
        unsafe { CloseHandle(process) };
        (executable, start_time_ticks, true)
    }

    impl Drop for JobObject {
        fn drop(&mut self) {
            unsafe {
                let _ = CloseHandle(self.handle);
            }
        }
    }

    fn resume_process_threads(process_id: u32) -> io::Result<()> {
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
        let mut result = Ok(());
        let mut resumed_any = false;
        let mut first = true;
        loop {
            let found = unsafe {
                if first {
                    first = false;
                    Thread32First(snapshot, &mut entry)
                } else {
                    Thread32Next(snapshot, &mut entry)
                }
            };
            if found == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error()
                    != Some(windows_sys::Win32::Foundation::ERROR_NO_MORE_FILES as i32)
                {
                    result = Err(error);
                }
                break;
            }
            if entry.th32OwnerProcessID != process_id {
                continue;
            }
            let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if thread.is_null() {
                result = Err(io::Error::last_os_error());
                break;
            }
            let resume_result = unsafe { ResumeThread(thread) };
            unsafe { CloseHandle(thread) };
            if resume_result == u32::MAX {
                result = Err(io::Error::last_os_error());
                break;
            }
            resumed_any = true;
        }
        unsafe { CloseHandle(snapshot) };
        if result.is_ok() && !resumed_any {
            return Err(io::Error::other("child primary thread was not found"));
        }
        result
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::ProcessContainer;

    #[cfg(unix)]
    #[test]
    fn missing_process_id_is_rejected_before_container_sharing() {
        let container = ProcessContainer::new().expect("local container construction");
        assert!(container.set_process_id(None).is_err());
    }
}
