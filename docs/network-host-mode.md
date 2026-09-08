# Host network mode

`NetworkMode::Host` is the compatibility networking mode for a child process.
It does not create a network namespace, proxy, or destination allow-list. The
child uses the host's normal resolver, interfaces, routes, firewall rules, and
proxy environment. The operating system may still apply its own firewall or
security policy.

## Platform behavior

- Linux: Landlock filesystem rules are installed without changing networking,
  so the child inherits the host network stack.
- macOS: the Seatbelt filesystem profile explicitly allows inbound and
  outbound networking. Filesystem-isolated children must have their working
  directory and required system configuration paths granted.
- Windows: a filesystem-isolated child runs in an AppContainer. The launcher
  grants the internet and private-network capabilities, then installs a
  scoped loopback exemption for the child AppContainer. The exemption list is
  updated under a named system mutex and restored when the child is released.

If Windows cannot update the loopback exemption list, the child is not
started. This prevents a request for host networking from silently degrading
to AppContainer-restricted loopback behavior. A stale exemption left by an
abrupt parent termination is harmless after the associated AppContainer
profile is deleted; later launches preserve all unrelated exemptions.

`NetworkMode::Disabled` is intentionally rejected with an unsupported error
until native network-denial enforcement is implemented. Callers can inspect
`platform_capabilities().host_network` before constructing a request.

## Verification

The integration suite verifies host loopback through filesystem containment on
macOS and includes equivalent Linux and Windows tests. The Windows test must
run on a Windows host; cross-compilation only verifies the native API bindings
and control flow.
