# rooagi-sandbox

Policy-driven process isolation and supervision for RooAGI runtimes.

## Licensing

This project is licensed under the [Apache License, Version 2.0](LICENSE).
You may use, modify, and distribute it, including in commercial agent
products, subject to the terms of that license.

This crate accepts already-authorized execution requests. It does not read
graph configuration, resolve authentication, or grant capabilities. Ambient
environment inheritance is disabled by default, including `PATH`.

The native process supervisor supports:

- bounded input/output, cancellation, deadlines, and a TERM/grace/KILL
  shutdown ladder;
- Linux cgroup v2 and Windows Job Object containment, including memory/process
  limits and native CPU hard caps where supported, with explicit degraded
  reporting for Unix process-group fallback;
- shared `ProcessGroup` ownership, bounded member/resource snapshots and
  sampling, and external adoption guarded by process-group-leader, executable,
  and PID identity checks on the supported Linux and Windows backends;
- lifecycle event streams for supervised children, connected pipeline stages
  with stage-indexed failure receipts, and TCP/HTTP readiness probes; and
- `platform_capabilities()` so callers can fail closed on optional behavior
  such as adoption, PTY, and CPU limits; and
- `execute_with_runner()` plus the `ProcessRunner` trait for deterministic
  orchestration tests and alternate execution adapters; and
- parent-death cleanup on Linux and process-tree cleanup on supported
  containment backends.

The crate is intentionally a native process supervisor, not a container
runtime. Linux filesystem isolation is enforced with Landlock when a
`FilesystemPolicy` is present. On macOS, requested policies use a generated
default-deny Seatbelt profile through `/usr/bin/sandbox-exec`, with Apple’s
system baseline imported so dynamically linked tools can start. This is a
transitional backend: `sandbox-exec` is deprecated and is not equivalent to a
signed Apple App Sandbox helper. Windows filesystem isolation uses a
per-execution AppContainer and temporary DACL grants; the original DACL is
restored when the child is released. Host-network mode also supplies the
AppContainer capabilities required for outbound and private-network
connectivity and installs a scoped loopback exemption while the child runs.
If Windows refuses that exemption update, the child is not started. `None` means unrestricted
filesystem access; `Some` is default-deny and every required runtime path must
be granted explicitly. `FilesystemPolicy::deny_all()` creates an explicit
no-access policy. Append-only grants are rejected unless `Write` is also
explicitly granted, because neither current backend can enforce append-only
access for an arbitrary child process.
Network uses explicit `NetworkMode::Host` passthrough by default. The
`NetworkMode::Disabled` value is reserved and fails closed until native
network-denial enforcement is implemented. Windows Job Object member
records use Toolhelp plus process-query APIs and may omit fields when the OS
denies inspection access. PTY-backed execution is
  available through native Unix PTYs and Windows ConPTY, and is attached to
  the same process-group/container lifecycle. The capability report marks PTY
  support unavailable on platforms where that integration is not implemented.
