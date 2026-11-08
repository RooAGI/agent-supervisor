# Terminal process manager

Agent Supervisor includes a separate terminal UI for inspecting and controlling
processes managed by a `Supervisor`. The application starts the UI as a child;
the TUI communicates with that supervisor over a private control channel.

## Build the TUI

Build the standalone executable from the repository root:

```bash
cargo build --manifest-path tui/Cargo.toml --release
```

The binary is written to `tui/target/release/agent-supervisor-tui` (append
`.exe` on Windows). The TUI needs an attached terminal. On Unix it opens
`/dev/tty`; on Windows it opens `CONIN$` and `CONOUT$`. It uses standard input
and output for the supervisor protocol, so those streams must not be redirected
away from the parent supervisor.

## Start it from Rust

Build your application with the Rust crate and Tokio. Set
`AGENT_SUPERVISOR_TUI` to the path of the TUI binary, then pass that path to
`Supervisor::start_tui`:

```rust,no_run
use agent_supervisor::Supervisor;
use std::{error::Error, ffi::OsString, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let supervisor = Supervisor::new();
    let tui: PathBuf = std::env::var_os("AGENT_SUPERVISOR_TUI")
        .map(PathBuf::from)
        .expect("set AGENT_SUPERVISOR_TUI to the TUI executable");
    supervisor.start_tui(tui, &[] as &[OsString])?;
    Ok(())
}
```

The supervisor owns the TUI child. It stops and reaps that child when the
supervisor is dropped. The TUI itself is outside the managed-job list, so
`shutdown_all()` will not stop its own control client.

See the [Rust example](../examples/rust/supervisor-tui/README.md) for an
interactive walkthrough with two labeled jobs.

## Start it from Python

Install the Python package, build the TUI binary, and set
`AGENT_SUPERVISOR_TUI` to its path. Inside an async application, call
`await supervisor.start_tui(executable, args=())`. The supervisor launches and
owns the separate TUI child; standard input and output remain dedicated to the
private protocol.

See the [Python example](../examples/python/supervisor_tui.py) for an async
walkthrough that starts OpenAI- and LangGraph-labelled jobs, launches the
dashboard, and confirms that stopping one leaves the other running.

## Use the dashboard

The dashboard refreshes process information as jobs change. It displays the
supervisor process ID, OS PID, executable, alive state, and optional framework
and run labels supplied by the host application. Use these commands:

| Key | Action |
|---|---|
| `j` / `k` | Select the next or previous process |
| `i` | Show or hide details for the selected process |
| `s` | Request a stop for the selected process; type `y` to confirm |
| `a` | Request a stop for all managed processes; type `y` to confirm |
| `r` | Refresh the list |
| `?` | Show help |
| `q` | Quit the TUI |

To label work, use `ManagedProcessMetadata` with
`Supervisor::start_with_metadata` or
`Supervisor::start_background_with_metadata`. Existing start methods remain
available and leave the labels empty. Labels are descriptive; they do not
change process identity or grant control. The library limits each label to
128 UTF-8 bytes and rejects terminal control characters.

## Control-channel permissions

The supervisor starts its TUI with a per-child private anonymous-pipe channel.
The UI does not discover or connect to a public socket, and the supervisor
checks each full supervisor-scoped process ID before stopping a process. PID
values in the dashboard are display-only; they cannot be used as stop targets.

The UI is an operator client trusted by the application that launches it. This
channel prevents unrelated processes from connecting to an open endpoint; it
is not an isolation boundary against code already running as the same OS user.
Do not launch an untrusted replacement executable as the TUI.

For wire framing, request validation, and platform handle constraints, see the
[TUI control protocol](supervisor-tui-protocol.md).
