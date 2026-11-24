# Agent Supervisor TUI

This package builds the separate terminal client that a Rust application can
launch with `Supervisor::start_tui`. The supervisor gives the client private
protocol pipes; protocol stdin/stdout are not used for terminal rendering.

Build it with:

```sh
cargo build --release --manifest-path tui/Cargo.toml
```

The screen is drawn with ratatui and keys are read with crossterm. The
interactive UI needs an attached terminal: on Unix it uses `/dev/tty`, on
Windows the console. Keys act at once, without Return:

- `j` / `↓`, `k` / `↑`: select the next / previous process
- `i` / `Enter`: show or hide details for the selected process
- `s`: stop the selected process (press `y` to confirm)
- `a`: stop all managed processes (press `y` to confirm)
- `PgDn` / `PgUp`: scroll the runs and FlowRuns panel
- `r`: refresh the process list
- `?`: show help
- `Esc`: close details, or quit
- `q` / `Ctrl-C`: exit the TUI

Process changes appear as supervisor events arrive; the list is also refreshed
once per second to recover from missed events. Launching without a terminal
fails with a clear error.

`--snapshot` renders one frame to standard output without a terminal, as text
or SVG, from the live Supervisor (`--service-socket` and `--token-file`) or a
saved state (`--from FILE`). See [the TUI guide](../docs/tui.md#snapshot-without-a-terminal).
