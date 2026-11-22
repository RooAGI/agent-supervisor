use agent_supervisor::{Command, ManagedProcessMetadata, ManagedProcessStatus, Policy, Supervisor};
use std::{error::Error, ffi::OsString, io, path::PathBuf, time::Duration};
use tokio::time::{sleep, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("--child") {
        tokio::time::sleep(Duration::from_secs(120)).await;
        return Ok(());
    }

    let tui = std::env::var_os("AGENT_SUPERVISOR_TUI")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("set AGENT_SUPERVISOR_TUI to the TUI executable"))?;
    let supervisor = Supervisor::new();
    let executable = std::env::current_exe()?;
    let policy = Policy::default();

    let openai = supervisor.start_background_with_metadata(
        &child_command(&executable),
        &policy,
        ManagedProcessMetadata::new(
            Some("OpenAI Agents SDK".into()),
            Some("support-agent".into()),
        )
        .map_err(io::Error::other)?,
    )?;
    let langgraph = supervisor.start_background_with_metadata(
        &child_command(&executable),
        &policy,
        ManagedProcessMetadata::new(Some("LangGraph".into()), Some("research-agent".into()))
            .map_err(io::Error::other)?,
    )?;

    supervisor.start_tui(tui, &[] as &[OsString])?;
    println!("Stop either labeled process in the TUI to continue the example.");

    let completed = timeout(Duration::from_secs(120), async {
        loop {
            let openai_status = supervisor.process_status(openai);
            let langgraph_status = supervisor.process_status(langgraph);
            if openai_status != Some(ManagedProcessStatus::Running) {
                break openai;
            }
            if langgraph_status != Some(ManagedProcessStatus::Running) {
                break langgraph;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no process was stopped in the TUI"))?;

    let sibling = if completed == openai {
        langgraph
    } else {
        openai
    };
    let result = supervisor.wait_process(completed).await?.ok_or_else(|| {
        io::Error::other("the stopped process result was not retained by the supervisor")
    })?;
    println!(
        "Stopped {} ({}); termination: {:?}",
        completed,
        if completed == openai {
            "OpenAI"
        } else {
            "LangGraph"
        },
        result.receipt.termination
    );
    assert_eq!(
        supervisor.process_status(sibling),
        Some(ManagedProcessStatus::Running),
        "stopping one managed process must leave its sibling running"
    );
    println!("Sibling {sibling} is still running.");

    for (id, result) in supervisor.shutdown_all(Duration::from_secs(1)).await? {
        println!("Cleaned up {id}: {:?}", result.receipt.termination);
    }
    Ok(())
}

fn child_command(executable: &std::path::Path) -> Command {
    Command {
        argv: vec![executable.display().to_string(), "--child".into()],
        cwd: None,
    }
}
