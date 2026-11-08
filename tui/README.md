# Agent Supervisor TUI

This package builds the separate terminal client that a Rust application can
launch with `Supervisor::start_tui`. The supervisor gives the client private
protocol pipes; protocol stdin/stdout are not used for terminal rendering.

Build it with:

```sh
cargo build --release --manifest-path tui/Cargo.toml
```

The executable needs an attached interactive terminal. On Unix it opens
`/dev/tty`; on Windows it opens the current console as `CONIN$` and `CONOUT$`.
It uses ANSI screen clearing and line-based input, so input is entered with
Return. Process changes appear as supervisor events arrive; the list is also
refreshed once per second to recover from missed events.

Commands:

- `j` / `k`: select the next / previous process
- `i`: show or hide details for the selected process
- `s`: request a stop for the selected process (asks for confirmation)
- `a`: request shutdown of all managed processes (asks for confirmation)
- `r`: refresh the process list
- `?`: show help
- `q`: exit the TUI

On Unix, launching without a controlling terminal fails with a clear error.
On Windows, an interactive console is required; redirected protocol pipes stay
separate from the console handles.
