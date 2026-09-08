# Process groups and resources

`ProcessGroup` provides a shared lifecycle owner for related children,
pipelines, and adopted external processes.

Available observations include:

- process ID, parent ID, executable identity, start-time identity, and liveness;
- current members and a bounded point-in-time snapshot;
- optional memory, CPU, and process-count statistics; and
- stage-indexed pipeline failure receipts.

Resource counters are intentionally optional. `None` means the operating
system or backend cannot provide that counter; it does not mean zero.

Before using adoption or kernel resource limits, check
`platform_capabilities()` and select `EnforcementRequirement::Required` when
degraded behavior is unacceptable.
