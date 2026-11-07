# Rust supervisor background jobs

Run from this directory:

```bash
cargo run
```

The example starts several manager-owned jobs, stops one by its
`SupervisorProcessId`, inspects the final result, verifies a sibling remains
running, and awaits `shutdown_all()` before the Tokio runtime exits. It starts
its own executable for child jobs, so it does not require a shell or Python.

`Supervisor::start_background()` must run inside an active Tokio runtime.
Persistent jobs are stopped explicitly; the policy's stdout and stderr limits
still apply, while `timeout_ms` does not automatically end a background job.
Windows has no graceful process signal and uses an immediate force-stop.
Dropping `Supervisor` also requests process-tree cleanup if normal awaited
shutdown is skipped.
