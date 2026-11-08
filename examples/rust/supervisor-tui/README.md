# Supervisor-owned TUI example

This example starts two long-running jobs, labels them as OpenAI and LangGraph
work, and launches the separate terminal process manager. Stop one process from
the TUI; the example observes its completion and confirms its sibling is still
running before it cleans up.

From the repository root, build the TUI and example:

```bash
cargo build --manifest-path tui/Cargo.toml --release
cargo build --manifest-path examples/rust/supervisor-tui/Cargo.toml
```

Set the TUI binary path and run the example:

```bash
AGENT_SUPERVISOR_TUI="$PWD/tui/target/release/agent-supervisor-tui" \
  cargo run --manifest-path examples/rust/supervisor-tui/Cargo.toml
```

On Windows, use the `.exe` suffix and set the environment variable in the
shell's normal syntax. Keep an interactive terminal attached; the child TUI
uses its controlling terminal for keyboard and screen I/O while standard input
and output carry the private protocol.

In the dashboard, select the job you want to stop with `j` or `k`, press `s`,
then type `y`. The example reports the stopped job's result and confirms the
other job remains running. Press `q` to leave the dashboard; when the example
ends, the supervisor stops its TUI child and cleans up the remaining job.
