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
`.exe` on Windows). The screen is drawn with [ratatui](https://ratatui.rs) and
keys are read with [crossterm](https://github.com/crossterm-rs/crossterm). The
interactive UI needs an attached terminal: on Unix it draws to and reads keys
from `/dev/tty`; on Windows it uses the console. It uses standard input and
output for the supervisor protocol, so those streams must not be redirected
away from the parent supervisor. To render without a terminal, see
[Snapshot without a terminal](#snapshot-without-a-terminal).

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

See the [Rust example](https://github.com/RooAGI/agent-supervisor/blob/main/examples/rust/supervisor-tui/README.md) for an
interactive walkthrough with two labeled jobs.

## Start it from Python

Install the Python package, build the TUI binary, and set
`AGENT_SUPERVISOR_TUI` to its path. Inside an async application, call
`await supervisor.start_tui(executable, args=())`. The supervisor launches and
owns the separate TUI child; standard input and output remain dedicated to the
private protocol.

See the [Python example](https://github.com/RooAGI/agent-supervisor/blob/main/examples/python/supervisor_tui.py) for an async
walkthrough that starts OpenAI- and LangGraph-labelled jobs, launches the
dashboard, and confirms that stopping one leaves the other running.

## Use the dashboard

The dashboard refreshes process information as jobs change. The Processes
panel contains observed managed OS processes, keyed for control by their
Supervisor process ID; it displays OS PID, executable, alive state and
available framework/run labels. Rows form a tree only when the OS parent PID is
registered and known start times show that the parent predates the child.
Unknown ancestry stays at the root, and PID is display-only because it can be
reused.

The coordinator appears as a service root header with its observed service PID
and executable. If the service cannot read its executable path, the TUI shows
the exact `current_exe()` limitation; coordinator state is not a managed child
row. FlowRun, task, post, attempt and worker IDs are contextual labels and
records in the coordinated-work panel, never standalone OS process rows.
Validated worker tool requests appear under their attempt with request ID, tool
name and lifecycle state. A tool request gains an OS child row only when an
observed Supervisor process ID is explicitly linked to it. Keys act at once; there is
no need to press Return:

| Key | Action |
|---|---|
| `j` / `↓`, `k` / `↑` | Select the next or previous process |
| `i` / `Enter` | Show or hide details for the selected process |
| `s` | Stop the selected process; press `y` to confirm, any other key cancels |
| `a` | Stop all managed processes; press `y` to confirm, any other key cancels |
| `PgDn` / `PgUp` | Scroll the runs and FlowRuns panel |
| `r` | Refresh the list |
| `?` | Show help; any key closes it |
| `Esc` | Close details, or quit |
| `q` / `Ctrl-C` | Quit the TUI |

A `view` role connection shows the same screen without the stop keys.

To label work, use `ManagedProcessMetadata` with
`Supervisor::start_with_metadata` or
`Supervisor::start_background_with_metadata`. Existing start methods remain
available and leave the labels empty. Labels are descriptive; they do not
change process identity or grant control. The library limits each label to
128 UTF-8 bytes and rejects terminal control characters.

## Snapshot without a terminal

`--snapshot` draws one frame of the dashboard and prints it to standard
output, then exits. It needs no terminal, so a script, a CI job or an agent
can run it, for example to produce a screenshot for documentation:

```bash
# The live Supervisor, read over its service socket (Unix):
agent-supervisor-tui --snapshot \
  --service-socket /path/to/supervisor.sock --token-file /path/to/view.token \
  --size 120x30 --format svg > dashboard.svg

# A saved state: a JSON object with the processes, runs, activities,
# attempts and flowRuns lists of a TUI snapshot response:
agent-supervisor-tui --snapshot --from state.json --format text
```

| Option | Default | Meaning |
|---|---|---|
| `--size COLSxROWS` | `110x34` | Screen size to draw, at least `40x12` |
| `--format text\|svg` | `text` | Plain text, or an SVG image with the dashboard's colors |

The snapshot uses the same drawing code as the interactive dashboard.

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
