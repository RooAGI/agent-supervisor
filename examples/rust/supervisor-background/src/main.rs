use agent_supervisor::{Command, ManagedProcessStatus, Policy, Supervisor};
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("--child") {
        run_child(args.get(2).map(String::as_str).unwrap_or("report")).await;
        return Ok(());
    }

    // start_background requires an active Tokio runtime. It owns output
    // draining, exit observation, and reaping for each returned ID.
    let supervisor = Supervisor::new();
    let executable = std::env::current_exe()?;
    let mut policy = Policy::default();
    policy.limits.output_bytes = 4 * 1024;
    policy.limits.stderr_bytes = 4 * 1024;

    // Persistent jobs do not stop when timeout_ms elapses. Use explicit
    // shutdown calls to stop them; output remains bounded by the policy.
    let report_id = supervisor.start_background(&child_command(&executable, "report"), &policy)?;
    let stop_id = supervisor.start_background(&child_command(&executable, "long"), &policy)?;
    let sibling_id = supervisor.start_background(&child_command(&executable, "long"), &policy)?;

    let report = supervisor.wait_process(report_id).await?.unwrap();
    println!("report output: {}", String::from_utf8_lossy(&report.stdout));
    println!("report exit: {:?}", report.receipt.termination);

    let stop_outcome = supervisor
        .shutdown_process(stop_id, Duration::from_millis(500))
        .await?;
    println!("stop request: {stop_outcome:?}");
    let stopped = supervisor.wait_process(stop_id).await?.unwrap();
    println!("stopped result: {:?}", stopped.receipt.termination);
    assert_eq!(
        supervisor.process_status(sibling_id),
        Some(ManagedProcessStatus::Running),
        "stopping one process must leave its sibling running"
    );

    // Await cleanup before the Tokio runtime exits. Windows has no graceful
    // process signal, so its backend requests an immediate force-stop.
    for (id, result) in supervisor.shutdown_all(Duration::from_secs(1)).await? {
        println!("{}: {:?}", id.get(), result.receipt.termination);
    }
    Ok(())
}

fn child_command(executable: &std::path::Path, mode: &str) -> Command {
    Command {
        argv: vec![
            executable.display().to_string(),
            "--child".into(),
            mode.into(),
        ],
        cwd: None,
    }
}

async fn run_child(mode: &str) {
    match mode {
        "report" => println!("short job completed"),
        "long" => {
            println!("long job started");
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        _ => eprintln!("unknown child mode: {mode}"),
    }
}
