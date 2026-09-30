# Security model

`agent-sandbox` is a process boundary, not a complete container runtime. Its
security properties come from explicit policy plus native operating-system
enforcement.

## Fail-closed inputs

- Child environments are cleared unless variables are explicitly inherited or
  set.
- Dynamic-loader variables such as `LD_PRELOAD` and `DYLD_*` are rejected.
- Executables and search paths must be absolute.
- Filesystem grants must be absolute and must name their access rights.
- Append-only grants are rejected unless write access is also explicit.
- Unsupported required enforcement returns an error instead of silently
  degrading.

## Filesystem policy

`filesystem: None` preserves unrestricted filesystem behavior for compatibility.
`Some(FilesystemPolicy::deny_all())` is explicit default-deny access. Add
`FilesystemGrant` entries only for the paths the child needs.

The native backend varies by operating system. Linux uses Landlock for
filesystem policy, Windows uses AppContainer/DACL policy, and the current
macOS implementation uses the legacy Seatbelt launcher. macOS filesystem
enforcement is therefore reported separately from full process containment
and is not equivalent to Apple App Sandbox entitlements. See [filesystem and
network](network-host-mode.md) for the platform contract.

`platform_capabilities()` reports compile-time platform support. The
`enforcement` value on an execution receipt reports the backend selected for
that execution; callers requiring a security boundary must request
`EnforcementRequirement::Required` and handle rejection when the host cannot
provide it.

## Threat-model boundary

The caller must authenticate and authorize the request before invoking the
sandbox. The sandbox does not protect against a malicious caller that already
has unrestricted access to the host process, kernel, or granted filesystem
paths.
