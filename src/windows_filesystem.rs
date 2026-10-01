//! Windows filesystem sandboxing.
//!
//! Windows Job Objects contain a process tree but do not restrict file access.
//! A filesystem request therefore uses a native AppContainer token.  The
//! requested roots receive temporary DACL entries for the per-launch
//! AppContainer SID; the entries are restored when the child is dropped.

#![cfg(windows)]

use crate::environment::EnvironmentPolicy;
use crate::filesystem::{FilesystemAccess, FilesystemPolicy};
use crate::policy::NetworkMode;
use crate::process_control::ProcessContainer;
use crate::supervisor::ChildStatus;
use std::ffi::{c_void, OsStr, OsString};
use std::io;
use std::mem::{size_of, zeroed};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::io::FromRawHandle;
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncWrite};

use windows_sys::core::PCWSTR;
use windows_sys::Win32::Foundation::{
    CloseHandle, SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT, WAIT_FAILED, WAIT_OBJECT_0,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::NetworkManagement::WindowsFirewall::{
    NetworkIsolationGetAppContainerConfig, NetworkIsolationSetAppContainerConfig,
};
use windows_sys::Win32::Security::Authorization::{
    GetNamedSecurityInfoW, SetEntriesInAclW, SetNamedSecurityInfoW, TreeResetNamedSecurityInfoW,
    EXPLICIT_ACCESS_W, GRANT_ACCESS, SE_FILE_OBJECT, TRUSTEE_IS_SID, TRUSTEE_IS_WELL_KNOWN_GROUP,
};
use windows_sys::Win32::Security::{
    DeriveCapabilitySidsFromName, FreeSid, GetLengthSid, GetSecurityDescriptorDacl,
    GetSecurityDescriptorLength, DACL_SECURITY_INFORMATION, PSID, SECURITY_ATTRIBUTES,
    SECURITY_CAPABILITIES, SID_AND_ATTRIBUTES,
};
use windows_sys::Win32::Storage::FileSystem::{
    GetFullPathNameW, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_TRAVERSE,
};
use windows_sys::Win32::System::Memory::{GetProcessHeap, HeapFree};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::Threading::{
    CreateMutexW, CreateProcessW, DeleteProcThreadAttributeList, GetExitCodeProcess,
    InitializeProcThreadAttributeList, ReleaseMutex, TerminateProcess, UpdateProcThreadAttribute,
    WaitForSingleObject, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT,
    EXTENDED_STARTUPINFO_PRESENT, INFINITE, LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION,
    PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES, STARTUPINFOEXW,
};

#[link(name = "kernel32")]
unsafe extern "system" {
    fn LocalFree(memory: isize) -> isize;
}

static NEXT_PROFILE: AtomicU64 = AtomicU64::new(1);

/// A child launched with native AppContainer capabilities and asynchronous
/// parent-side pipe handles.
pub(crate) struct WindowsChild {
    process: HANDLE,
    pub(crate) pid: u32,
    stdin: Option<tokio::fs::File>,
    stdout: Option<tokio::fs::File>,
    stderr: Option<tokio::fs::File>,
    _sandbox: WindowsSandbox,
}

// HANDLEs are kernel references and are safe to move between worker threads.
unsafe impl Send for WindowsChild {}

impl WindowsChild {
    pub(crate) fn spawn(
        executable: &Path,
        args: &[String],
        cwd: Option<&Path>,
        environment: &EnvironmentPolicy,
        policy: &FilesystemPolicy,
        network: NetworkMode,
        container: &ProcessContainer,
    ) -> io::Result<Self> {
        let sandbox = WindowsSandbox::prepare(policy)?;
        let (stdin_parent, stdin_child) = pipe()?;
        let (stdout_child, stdout_parent) = pipe()?;
        let (stderr_child, stderr_parent) = pipe()?;
        for handle in [stdin_parent, stdout_parent, stderr_parent] {
            if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
                close_handle(stdin_parent);
                close_handle(stdin_child);
                close_handle(stdout_child);
                close_handle(stdout_parent);
                close_handle(stderr_child);
                close_handle(stderr_parent);
                return Err(io::Error::last_os_error());
            }
        }

        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = windows_sys::Win32::System::Threading::STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = stdin_child;
        startup.StartupInfo.hStdOutput = stdout_child;
        startup.StartupInfo.hStdError = stderr_child;

        let mut attributes = AttributeList::new()?;
        let network_capabilities = CapabilitySids::for_network(network)?;
        let mut capabilities = SECURITY_CAPABILITIES {
            AppContainerSid: sandbox.sid,
            Capabilities: network_capabilities.entries.as_ptr() as *mut SID_AND_ATTRIBUTES,
            CapabilityCount: network_capabilities.entries.len() as u32,
            Reserved: 0,
        };
        attributes.set_security_capabilities(&mut capabilities)?;
        startup.lpAttributeList = attributes.list;

        let application = wide_path(executable)?;
        let mut command_line = command_line(executable, args)?;
        // Pass an explicit current directory whenever a custom environment
        // block is supplied. This avoids making CreateProcess infer the
        // process drive from its caller while also relying on the `=C:`
        // pseudo-variable preserved in that block.
        let inherited_directory;
        let current_directory_path = match cwd {
            Some(directory) => directory,
            None => {
                inherited_directory = std::env::current_dir()?;
                &inherited_directory
            }
        };
        let current_directory = Some(wide_path(current_directory_path)?);
        let mut environment_block = environment_block(environment, cwd)?;
        let mut process_info: PROCESS_INFORMATION = unsafe { zeroed() };
        let flags = CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT;
        let created = unsafe {
            CreateProcessW(
                application.as_ptr(),
                command_line.as_mut_ptr(),
                null(),
                null(),
                1,
                flags,
                environment_block.as_mut_ptr().cast::<c_void>(),
                current_directory
                    .as_ref()
                    .map_or(null(), |value| value.as_ptr()),
                (&startup as *const STARTUPINFOEXW)
                    .cast::<windows_sys::Win32::System::Threading::STARTUPINFOW>(),
                &mut process_info,
            )
        };
        unsafe {
            CloseHandle(stdin_child);
            CloseHandle(stdout_child);
            CloseHandle(stderr_child);
        }
        if created == 0 {
            close_handle(process_info.hProcess);
            close_handle(process_info.hThread);
            return Err(io::Error::last_os_error());
        }
        if let Err(error) = container.attach_native_process(
            process_info.hProcess as usize,
            process_info.hThread as usize,
        ) {
            unsafe { TerminateProcess(process_info.hProcess, 1) };
            unsafe { CloseHandle(process_info.hProcess) };
            unsafe { CloseHandle(process_info.hThread) };
            return Err(error);
        }
        unsafe { CloseHandle(process_info.hThread) };

        Ok(Self {
            process: process_info.hProcess,
            pid: process_info.dwProcessId,
            stdin: Some(file_from_handle(stdin_parent)),
            stdout: Some(file_from_handle(stdout_parent)),
            stderr: Some(file_from_handle(stderr_parent)),
            _sandbox: sandbox,
        })
    }

    pub(crate) fn take_stdin(&mut self) -> Option<Box<dyn AsyncWrite + Unpin + Send>> {
        self.stdin
            .take()
            .map(|value| Box::new(value) as Box<dyn AsyncWrite + Unpin + Send>)
    }

    pub(crate) fn take_stdout(&mut self) -> Option<Box<dyn AsyncRead + Unpin + Send>> {
        self.stdout
            .take()
            .map(|value| Box::new(value) as Box<dyn AsyncRead + Unpin + Send>)
    }

    pub(crate) fn take_stderr(&mut self) -> Option<Box<dyn AsyncRead + Unpin + Send>> {
        self.stderr
            .take()
            .map(|value| Box::new(value) as Box<dyn AsyncRead + Unpin + Send>)
    }

    pub(crate) async fn wait(&mut self) -> io::Result<ChildStatus> {
        let handle = self.process as usize;
        let result = tokio::task::spawn_blocking(move || wait_process(handle as HANDLE, INFINITE))
            .await
            .map_err(io::Error::other)??;
        Ok(result)
    }

    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ChildStatus>> {
        match wait_process(self.process, 0) {
            Ok(status) => Ok(Some(status)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn kill(&mut self) -> io::Result<()> {
        if unsafe { TerminateProcess(self.process, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for WindowsChild {
    fn drop(&mut self) {
        if !self.process.is_null() {
            unsafe { CloseHandle(self.process) };
            self.process = null_mut();
        }
    }
}

fn wait_process(handle: HANDLE, timeout: u32) -> io::Result<ChildStatus> {
    let result =
        unsafe { windows_sys::Win32::System::Threading::WaitForSingleObject(handle, timeout) };
    if result == WAIT_TIMEOUT {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "process is running",
        ));
    }
    if result == WAIT_FAILED || result != WAIT_OBJECT_0 {
        return Err(with_context(
            "WaitForSingleObject failed",
            io::Error::last_os_error(),
        ));
    }
    let mut code = 0u32;
    if unsafe { GetExitCodeProcess(handle, &mut code) } == 0 {
        return Err(with_context(
            "GetExitCodeProcess failed",
            io::Error::last_os_error(),
        ));
    }
    Ok(ChildStatus {
        code: Some(code as i32),
        signal: None,
    })
}

fn with_context(context: &str, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{context} (OS error {:?}): {error}", error.raw_os_error()),
    )
}

fn pipe() -> io::Result<(HANDLE, HANDLE)> {
    let mut read = null_mut();
    let mut write = null_mut();
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };
    if unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((read, write))
}

fn file_from_handle(handle: HANDLE) -> tokio::fs::File {
    let file = unsafe { std::fs::File::from_raw_handle(handle) };
    tokio::fs::File::from_std(file)
}

fn close_handle(handle: HANDLE) {
    if !handle.is_null() {
        unsafe { CloseHandle(handle) };
    }
}

fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    if path.as_os_str().encode_wide().any(|value| value == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains NUL",
        ));
    }
    Ok(path.as_os_str().encode_wide().chain(Some(0)).collect())
}

fn command_line(executable: &Path, args: &[String]) -> io::Result<Vec<u16>> {
    let mut line = quote_argument(executable.as_os_str());
    for arg in args {
        if arg.encode_utf16().any(|value| value == 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "argument contains NUL",
            ));
        }
        line.push(' ');
        line.push_str(&quote_argument(OsStr::new(arg)));
    }
    Ok(line.encode_utf16().chain(Some(0)).collect())
}

fn quote_argument(value: &OsStr) -> String {
    let text = value.to_string_lossy();
    if !text.is_empty()
        && !text
            .chars()
            .any(|value| value.is_whitespace() || value == '"')
    {
        return text.into_owned();
    }
    let mut output = String::from("\"");
    let mut slashes = 0;
    for value in text.chars() {
        if value == '\\' {
            slashes += 1;
        } else if value == '"' {
            output.push_str(&"\\".repeat(slashes * 2 + 1));
            output.push(value);
            slashes = 0;
        } else {
            output.push_str(&"\\".repeat(slashes));
            output.push(value);
            slashes = 0;
        }
    }
    output.push_str(&"\\".repeat(slashes * 2));
    output.push('"');
    output
}

fn environment_block(
    policy: &EnvironmentPolicy,
    working_directory: Option<&Path>,
) -> io::Result<Vec<u16>> {
    let mut entries = Vec::new();
    for name in &policy.inherit {
        if let Some(value) = std::env::var_os(name) {
            entries.push((OsString::from(name), value));
        }
    }
    entries.extend(
        policy
            .variables
            .iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value))),
    );
    if !policy.executable_search_paths.is_empty() {
        entries.push((
            OsString::from("PATH"),
            std::env::join_paths(&policy.executable_search_paths)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid PATH"))?,
        ));
    }
    let directory = match working_directory {
        Some(directory) => directory.to_path_buf(),
        None => std::env::current_dir()?,
    };
    let directory = normalize_drive_path(&directory);
    add_drive_directory_entry(&mut entries, &directory, Some(&directory))?;
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        let system_root = normalize_drive_path(Path::new(&system_root));
        add_drive_directory_entry(&mut entries, &system_root, None)?;
    }
    // Windows requires Unicode environment blocks to be sorted by variable
    // name (case-insensitively). CreateProcess may reject unsorted blocks with
    // ERROR_ENVVAR_NOT_FOUND (203).
    entries.sort_by(|(left, _), (right, _)| {
        left.to_string_lossy()
            .to_lowercase()
            .cmp(&right.to_string_lossy().to_lowercase())
    });
    let mut block = Vec::new();
    for (name, value) in entries {
        block.extend(name.encode_wide());
        block.push('=' as u16);
        block.extend(value.encode_wide());
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

fn normalize_drive_path(path: &Path) -> PathBuf {
    path.to_str()
        .and_then(|path| path.strip_prefix(r"\\?\"))
        .map(PathBuf::from)
        .unwrap_or_else(|| path.to_path_buf())
}

fn add_drive_directory_entry(
    entries: &mut Vec<(OsString, OsString)>,
    path: &Path,
    value: Option<&Path>,
) -> io::Result<()> {
    let Some(std::path::Component::Prefix(prefix)) = path.components().next() else {
        return Ok(());
    };
    let std::path::Prefix::Disk(drive) = prefix.kind() else {
        return Ok(());
    };
    let name = OsString::from(format!("={}:", drive as char));
    let directory = match value {
        Some(value) => value.to_path_buf(),
        None => {
            let input = format!("{}:.", drive as char);
            let input = input.encode_utf16().chain(Some(0)).collect::<Vec<_>>();
            let mut output = vec![0_u16; 32_768];
            let length = unsafe {
                GetFullPathNameW(
                    input.as_ptr(),
                    output.len() as u32,
                    output.as_mut_ptr(),
                    null_mut(),
                )
            };
            if length == 0 || length as usize >= output.len() {
                return Err(io::Error::last_os_error());
            }
            PathBuf::from(OsString::from_wide(&output[..length as usize]))
        }
    };
    entries.retain(|(existing, _)| !existing.eq_ignore_ascii_case(&name));
    entries.push((name, directory.into_os_string()));
    Ok(())
}

#[cfg(test)]
mod environment_block_tests {
    use super::*;

    #[test]
    fn environment_block_entries_are_case_insensitively_sorted() {
        let policy = EnvironmentPolicy {
            inherit: ["SystemRoot", "ComSpec", "PATH", "WINDIR"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ..EnvironmentPolicy::default()
        };
        let block = environment_block(&policy, None).unwrap();
        let entries = String::from_utf16(&block)
            .unwrap()
            .split('\0')
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let names = entries
            .iter()
            .map(|entry| entry.split('=').next().unwrap().to_lowercase())
            .collect::<Vec<_>>();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        assert!(entries.iter().any(|entry| {
            entry.starts_with('=') && entry.get(1..3).is_some_and(|drive| drive.ends_with(':'))
        }));
    }

    #[test]
    fn custom_environment_block_starts_an_unconfined_process() {
        let system_root = std::env::var_os("SystemRoot").expect("SystemRoot");
        let executable = PathBuf::from(system_root).join("System32").join("cmd.exe");
        let policy = EnvironmentPolicy {
            inherit: ["SystemRoot", "WINDIR", "ComSpec", "PATH"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ..EnvironmentPolicy::default()
        };
        let mut environment = environment_block(&policy, None).expect("environment block");
        let application = wide_path(&executable).expect("application path");
        let mut command =
            command_line(&executable, &["/C".into(), "exit 0".into()]).expect("command line");
        let directory = wide_path(&std::env::current_dir().expect("current directory"))
            .expect("working directory");
        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        let mut process: PROCESS_INFORMATION = unsafe { zeroed() };

        let created = unsafe {
            CreateProcessW(
                application.as_ptr(),
                command.as_mut_ptr(),
                null(),
                null(),
                0,
                CREATE_UNICODE_ENVIRONMENT,
                environment.as_mut_ptr().cast::<c_void>(),
                directory.as_ptr(),
                (&startup as *const STARTUPINFOEXW)
                    .cast::<windows_sys::Win32::System::Threading::STARTUPINFOW>(),
                &mut process,
            )
        };
        assert_ne!(created, 0, "CreateProcessW: {}", io::Error::last_os_error());
        unsafe {
            CloseHandle(process.hThread);
        }
        let status = wait_process(process.hProcess, 10_000).expect("wait for cmd.exe");
        unsafe { CloseHandle(process.hProcess) };
        assert_eq!(status.code, Some(0));
    }
}

struct AttributeList {
    storage: Vec<u8>,
    list: LPPROC_THREAD_ATTRIBUTE_LIST,
}

struct CapabilitySids {
    entries: Vec<SID_AND_ATTRIBUTES>,
    sids: Vec<PSID>,
    arrays: Vec<*mut PSID>,
}

impl CapabilitySids {
    fn for_network(network: NetworkMode) -> io::Result<Self> {
        if matches!(network, NetworkMode::Disabled) {
            return Ok(Self {
                entries: Vec::new(),
                sids: Vec::new(),
                arrays: Vec::new(),
            });
        }
        let names = [
            "internetClient",
            "internetClientServer",
            "privateNetworkClientServer",
        ];
        let mut entries = Vec::with_capacity(names.len());
        let mut sids = Vec::with_capacity(names.len());
        let mut arrays = Vec::with_capacity(names.len());
        for name in names {
            let name = wide_string(name)?;
            let mut groups = null_mut();
            let mut group_count = 0;
            let mut capabilities = null_mut();
            let mut capability_count = 0;
            let ok = unsafe {
                DeriveCapabilitySidsFromName(
                    name.as_ptr(),
                    &mut groups,
                    &mut group_count,
                    &mut capabilities,
                    &mut capability_count,
                )
            };
            if ok == 0 || capability_count == 0 || capabilities.is_null() {
                if !groups.is_null() {
                    unsafe { free_sid_array(groups, group_count) };
                }
                if !capabilities.is_null() {
                    unsafe { free_sid_array(capabilities, capability_count) };
                }
                unsafe { Self::free_allocations(&mut sids, &mut arrays) };
                return Err(io::Error::last_os_error());
            }
            if !groups.is_null() {
                unsafe { free_sid_array(groups, group_count) };
            }
            let sid = unsafe { *capabilities };
            if sid.is_null() {
                unsafe { free_sid_array(capabilities, capability_count) };
                unsafe { Self::free_allocations(&mut sids, &mut arrays) };
                return Err(io::Error::other("network capability SID unavailable"));
            }
            sids.push(sid);
            arrays.push(capabilities);
            entries.push(SID_AND_ATTRIBUTES {
                Sid: sid,
                Attributes: 0x0000_0004,
            });
        }
        Ok(Self {
            entries,
            sids,
            arrays,
        })
    }

    unsafe fn free_allocations(sids: &mut Vec<PSID>, arrays: &mut Vec<*mut PSID>) {
        for sid in sids.drain(..) {
            unsafe { LocalFree(sid as isize) };
        }
        for array in arrays.drain(..) {
            unsafe { LocalFree(array as isize) };
        }
    }
}

impl Drop for CapabilitySids {
    fn drop(&mut self) {
        unsafe { Self::free_allocations(&mut self.sids, &mut self.arrays) };
    }
}

unsafe fn free_sid_array(array: *mut PSID, count: u32) {
    if array.is_null() {
        return;
    }
    for index in 0..count as usize {
        let sid = unsafe { *array.add(index) };
        if !sid.is_null() {
            unsafe { LocalFree(sid as isize) };
        }
    }
    unsafe { LocalFree(array as isize) };
}

impl AttributeList {
    fn new() -> io::Result<Self> {
        let mut length = 0usize;
        unsafe { InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut length) };
        if length == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut storage = vec![0u8; length];
        let list = storage.as_mut_ptr().cast::<c_void>();
        if unsafe { InitializeProcThreadAttributeList(list, 1, 0, &mut length) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { storage, list })
    }

    fn set_security_capabilities(
        &mut self,
        capabilities: &mut SECURITY_CAPABILITIES,
    ) -> io::Result<()> {
        if unsafe {
            UpdateProcThreadAttribute(
                self.list,
                0,
                PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                (capabilities as *mut SECURITY_CAPABILITIES).cast::<c_void>(),
                size_of::<SECURITY_CAPABILITIES>(),
                null_mut(),
                null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for AttributeList {
    fn drop(&mut self) {
        unsafe { DeleteProcThreadAttributeList(self.list) };
        let _ = &self.storage;
    }
}

struct WindowsSandbox {
    sid: PSID,
    profile_name: Vec<u16>,
    grants: Vec<DaclGrant>,
    _loopback: LoopbackExemption,
}

impl WindowsSandbox {
    fn prepare(policy: &FilesystemPolicy) -> io::Result<Self> {
        let id = NEXT_PROFILE.fetch_add(1, Ordering::Relaxed);
        let name = format!("agent-supervisor-{}-{}", std::process::id(), id);
        let profile_name = wide_string(&name)?;
        let sid = create_profile(&profile_name)?;
        let loopback = match LoopbackExemption::enable(sid) {
            Ok(value) => value,
            Err(error) => {
                unsafe {
                    FreeSid(sid);
                    delete_profile(profile_name.as_ptr());
                }
                return Err(error);
            }
        };
        let mut grants = Vec::new();
        for grant in &policy.grants {
            match DaclGrant::apply(&grant.root, grant.access.as_slice(), sid) {
                Ok(grant) => grants.push(grant),
                Err(error) => {
                    grants.clear();
                    drop(loopback);
                    unsafe {
                        FreeSid(sid);
                        delete_profile(profile_name.as_ptr());
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            sid,
            profile_name,
            grants,
            _loopback: loopback,
        })
    }
}

/// A scoped Windows AppContainer loopback exemption.
///
/// `NetworkIsolationSetAppContainerConfig` replaces the complete global
/// exemption list. The lock makes our read-modify-write operation atomic with
/// respect to other sandbox launches in this process, and Drop restores the
/// list while preserving entries created by other processes.
struct LoopbackExemption {
    sid: PSID,
}

struct OwnedLoopbackConfig {
    entries: Vec<SID_AND_ATTRIBUTES>,
    _sids: Vec<Vec<u8>>,
}

struct LoopbackConfigLock {
    handle: HANDLE,
}

impl LoopbackConfigLock {
    fn acquire() -> io::Result<Self> {
        let name = wide_string("Global\\RooAgiSandboxLoopbackConfig")?;
        let handle = unsafe { CreateMutexW(null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let wait = unsafe { WaitForSingleObject(handle, INFINITE) };
        if wait != windows_sys::Win32::Foundation::WAIT_OBJECT_0
            && wait != windows_sys::Win32::Foundation::WAIT_ABANDONED
        {
            unsafe { CloseHandle(handle) };
            return Err(io::Error::last_os_error());
        }
        Ok(Self { handle })
    }
}

impl Drop for LoopbackConfigLock {
    fn drop(&mut self) {
        unsafe {
            ReleaseMutex(self.handle);
            CloseHandle(self.handle);
        }
    }
}

impl LoopbackExemption {
    fn enable(sid: PSID) -> io::Result<Self> {
        let _lock = LoopbackConfigLock::acquire()?;
        let mut config = read_loopback_config()?;
        if !config
            .entries
            .iter()
            .any(|entry| unsafe { sid_equal(entry.Sid, sid) })
        {
            config.entries.push(SID_AND_ATTRIBUTES {
                Sid: sid,
                Attributes: 0,
            });
            write_loopback_config(&config.entries)?;
        }
        Ok(Self { sid })
    }
}

impl Drop for LoopbackExemption {
    fn drop(&mut self) {
        let Ok(_lock) = LoopbackConfigLock::acquire() else {
            return;
        };
        let Ok(mut config) = read_loopback_config() else {
            return;
        };
        config
            .entries
            .retain(|entry| unsafe { !sid_equal(entry.Sid, self.sid) });
        let _ = write_loopback_config(&config.entries);
    }
}

fn read_loopback_config() -> io::Result<OwnedLoopbackConfig> {
    let mut count = 0;
    let mut pointer = null_mut();
    let status = unsafe { NetworkIsolationGetAppContainerConfig(&mut count, &mut pointer) };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let source = if pointer.is_null() || count == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(pointer, count as usize) }
    };
    let mut sids = Vec::with_capacity(source.len());
    for entry in source {
        if entry.Sid.is_null() {
            unsafe { free_loopback_config(pointer, count) };
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows returned a null loopback exemption SID",
            ));
        }
        let length = unsafe { GetLengthSid(entry.Sid) } as usize;
        if length == 0 {
            unsafe { free_loopback_config(pointer, count) };
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows returned an invalid loopback exemption SID",
            ));
        }
        let mut sid = vec![0u8; length];
        unsafe {
            std::ptr::copy_nonoverlapping(entry.Sid.cast::<u8>(), sid.as_mut_ptr(), length);
        }
        sids.push(sid);
    }
    let entries = source
        .iter()
        .zip(sids.iter())
        .map(|(entry, sid)| SID_AND_ATTRIBUTES {
            Sid: sid.as_ptr().cast_mut().cast(),
            Attributes: entry.Attributes,
        })
        .collect();
    unsafe { free_loopback_config(pointer, count) };
    Ok(OwnedLoopbackConfig {
        entries,
        _sids: sids,
    })
}

fn write_loopback_config(entries: &[SID_AND_ATTRIBUTES]) -> io::Result<()> {
    let status =
        unsafe { NetworkIsolationSetAppContainerConfig(entries.len() as u32, entries.as_ptr()) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

unsafe fn free_loopback_config(entries: *mut SID_AND_ATTRIBUTES, count: u32) {
    if entries.is_null() {
        return;
    }
    for index in 0..count as usize {
        let sid = unsafe { (*entries.add(index)).Sid };
        if !sid.is_null() {
            unsafe { HeapFree(GetProcessHeap(), 0, sid.cast()) };
        }
    }
    unsafe { HeapFree(GetProcessHeap(), 0, entries.cast()) };
}

unsafe fn sid_equal(left: PSID, right: PSID) -> bool {
    if left.is_null() || right.is_null() {
        return left == right;
    }
    windows_sys::Win32::Security::EqualSid(left, right) != 0
}

impl Drop for WindowsSandbox {
    fn drop(&mut self) {
        // Restore DACLs before deleting the profile. Failure is intentionally
        // not propagated from Drop; the operation is still observable in the
        // process lifecycle logs by the caller that owns the execution.
        self.grants.clear();
        unsafe {
            FreeSid(self.sid);
            delete_profile(self.profile_name.as_ptr());
        }
    }
}

unsafe extern "system" {
    #[link_name = "CreateAppContainerProfile"]
    fn create_app_container_profile(
        name: PCWSTR,
        display: PCWSTR,
        description: PCWSTR,
        capabilities: *mut c_void,
        capability_count: u32,
        sid: *mut PSID,
    ) -> i32;
    #[link_name = "DeleteAppContainerProfile"]
    fn delete_app_container_profile(name: PCWSTR) -> i32;
}

fn create_profile(name: &[u16]) -> io::Result<PSID> {
    let mut sid = null_mut();
    let result = unsafe {
        create_app_container_profile(
            name.as_ptr(),
            name.as_ptr(),
            name.as_ptr(),
            null_mut(),
            0,
            &mut sid,
        )
    };
    if result != 0 && sid.is_null() {
        return Err(io::Error::from_raw_os_error(result));
    }
    if sid.is_null() {
        return Err(io::Error::other("AppContainer SID unavailable"));
    }
    Ok(sid)
}

unsafe fn delete_profile(name: PCWSTR) {
    let _ = delete_app_container_profile(name);
}

fn wide_string(value: &str) -> io::Result<Vec<u16>> {
    if value.encode_utf16().any(|value| value == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "string contains NUL",
        ));
    }
    Ok(value.encode_utf16().chain(Some(0)).collect())
}

struct DaclGrant {
    path: PathBuf,
    original_descriptor: Vec<u8>,
}

impl DaclGrant {
    fn apply(path: &Path, access: &[FilesystemAccess], sid: PSID) -> io::Result<Self> {
        let path_w = wide_path(path)?;
        let mask = access_mask(access)?;
        let mut descriptor = null_mut();
        let mut dacl = null_mut();
        let status = unsafe {
            GetNamedSecurityInfoW(
                path_w.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let length = unsafe { GetSecurityDescriptorLength(descriptor) } as usize;
        let original_descriptor =
            unsafe { std::slice::from_raw_parts(descriptor.cast::<u8>(), length) }.to_vec();

        let mut trustee: windows_sys::Win32::Security::Authorization::TRUSTEE_W =
            unsafe { zeroed() };
        trustee.TrusteeForm = TRUSTEE_IS_SID;
        trustee.TrusteeType = TRUSTEE_IS_WELL_KNOWN_GROUP;
        trustee.ptstrName = sid.cast::<u16>();
        let mut entry: EXPLICIT_ACCESS_W = unsafe { zeroed() };
        entry.grfAccessPermissions = mask;
        entry.grfAccessMode = GRANT_ACCESS;
        entry.grfInheritance = 0x3;
        entry.Trustee = trustee;
        let mut new_dacl = null_mut();
        let status = unsafe { SetEntriesInAclW(1, &entry, dacl, &mut new_dacl) };
        if status != 0 {
            unsafe { LocalFree(descriptor as isize) };
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        let status = unsafe {
            SetNamedSecurityInfoW(
                path_w.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                new_dacl,
                null_mut(),
            )
        };
        unsafe { LocalFree(new_dacl as isize) };
        unsafe { LocalFree(descriptor as isize) };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        Ok(Self {
            path: path.to_path_buf(),
            original_descriptor,
        })
    }
}

impl Drop for DaclGrant {
    fn drop(&mut self) {
        let Ok(path) = wide_path(&self.path) else {
            return;
        };
        let mut dacl = null_mut();
        let mut present = 0;
        let mut defaulted = 0;
        let descriptor = self.original_descriptor.as_mut_ptr().cast::<c_void>();
        if unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) }
            == 0
        {
            return;
        }
        unsafe {
            let status = TreeResetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                if present != 0 { dacl } else { null_mut() },
                null_mut(),
                0,
                None,
                0,
                null(),
            );
            if status != 0 {
                let _ = SetNamedSecurityInfoW(
                    path.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    if present != 0 { dacl } else { null_mut() },
                    null_mut(),
                );
            }
        }
    }
}

fn access_mask(access: &[FilesystemAccess]) -> io::Result<u32> {
    // Traversal is required to reach descendants of a granted directory in
    // an AppContainer. It does not grant read or write access by itself.
    let mut mask = FILE_TRAVERSE;
    for access in access {
        match access {
            FilesystemAccess::Read => mask |= FILE_GENERIC_READ,
            FilesystemAccess::Write | FilesystemAccess::Append => mask |= FILE_GENERIC_WRITE,
        }
    }
    if mask == 0 {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty filesystem grant",
        ))
    } else {
        Ok(mask)
    }
}
