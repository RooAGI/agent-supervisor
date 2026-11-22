---
hide:
  - toc
---

# Quickstart

Agent Supervisor provides a supervision layer for agent applications. It
manages agent run lifecycles and the tools they launch, with execution policies,
activity tracking, cancellation, and cleanup.

## 1. Install

=== "Rust"

    ```sh
    cargo add agent-supervisor
    cargo add tokio --features macros,rt-multi-thread
    ```

    Use your application's Tokio runtime if it already has one.

=== "Python"

    ```sh
    python -m pip install agent-supervisor
    ```

=== "Node.js"

    The npm package is not published yet. From an Agent Supervisor checkout,
    install it with this one command:

    ```sh
    npm install /path/to/agent-supervisor/clients/node
    ```

    The source install builds the native addon and requires Rust and your
    platform's native build tools. See the [Node package guide](https://github.com/RooAGI/agent-supervisor/blob/main/clients/node/README.md)
    for service mode and framework adapter setup.

## 2. Supervise an agent run

Wrap your framework call in an `AgentRun` so Agent Supervisor can apply a
deadline and clean up processes the run owns. The framework still runs the
agent. Rust wraps the framework future directly; Python and Node examples use
the OpenAI Agents SDK.

=== "Rust"

    Pass your framework's async call to `AgentRun::run()`. Replace
    `your_framework_call()` with the future your framework returns:

    ```rust
    use agent_supervisor::{AgentRunPolicy, Supervisor};
    use std::time::Duration;

    let supervisor = Supervisor::new();
    let agent_run = supervisor.start_agent_run(AgentRunPolicy::default());
    let result = agent_run.run(your_framework_call()).await?;
    agent_run.cancel().await; // Finish the run and clean up its owned processes.
    supervisor.shutdown_all(Duration::from_secs(5)).await?;
    ```

=== "Python"

    Install the adapter and SDK:

    ```sh
    python -m pip install 'agent-supervisor[openai]'
    ```

    Set `OPENAI_API_KEY`, then pass your agent to the supervised `Runner`:

    ```python
    import asyncio
    from agents import Agent
    from agent_supervisor import Supervisor
    from agent_supervisor.openai import Runner

    agent = Agent(
        name="Assistant",
        instructions="Answer the user's question clearly.",
    )

    async def main():
        supervisor = Supervisor()
        try:
            result = await Runner.run(
                agent, "What does Agent Supervisor do?", supervisor=supervisor
            )
            print(result.final_output)
        finally:
            await supervisor.shutdown_all()

    asyncio.run(main())
    ```

=== "Node.js"

    Install `@openai/agents` alongside the Node package and set
    `OPENAI_API_KEY`. This uses the embedded `Supervisor`; no service setup is
    needed.

    ```js
    import { AgentRunPolicy, Supervisor } from '@rooagi/agent-supervisor'
    import { Agent, run } from '@openai/agents'

    const agent = new Agent({
      name: 'Assistant',
      instructions: 'Answer the user’s question clearly.',
    })

    const supervisor = new Supervisor()
    try {
      const agentRun = supervisor.startAgentRun(new AgentRunPolicy())
      const result = await agentRun.run(
        (_scope, { signal }) => run(agent, 'What does Agent Supervisor do?', { signal })
      )
      console.log(result.finalOutput)
    } finally {
      await supervisor.shutdownAll()
    }
    ```

For service-backed Node framework adapters that register run, activity, and
attempt records, see the [Node package guide](https://github.com/RooAGI/agent-supervisor/blob/main/clients/node/README.md).

## 3. Run one supervised command

Start with a single command. `run()` launches the child directly, captures
bounded output, and returns its result.

=== "Rust"

    This small program starts itself once as a child, so it works without
    assuming a particular system command is installed:

    ```rust
    use agent_supervisor::{run, Command, Policy};

    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::args().any(|arg| arg == "--child") {
            println!("hello from a supervised process");
            return Ok(());
        }

        let executable = std::env::current_exe()?;
        let command = Command {
            argv: vec![executable.display().to_string(), "--child".into()],
            cwd: None,
        };
        let result = run(&command, &Policy::default()).await?;
        print!("{}", String::from_utf8_lossy(&result.stdout));
        Ok(())
    }
    ```

=== "Python"

    ```python
    import asyncio
    import sys

    from agent_supervisor import Command, Policy, run

    async def main():
        result = await run(
            Command([sys.executable, "-c", "print('hello from a supervised process')"]),
            policy=Policy(),
        )
        print(result.stdout.decode("utf-8", errors="replace"), end="")

    asyncio.run(main())
    ```

=== "Node.js"

    ```js
    import { Command, Policy, run } from '@rooagi/agent-supervisor'

    const result = await run(
      new Command([process.execPath, '-e', "console.log('hello from a supervised process')"]),
      { policy: new Policy() }
    )
    process.stdout.write(result.stdout)
    ```

The default policy allows host networking and does not restrict filesystem
access. Set a narrower [`Policy`](security-model.md) for tools that need fewer
permissions. Enforcement depends on the operating system.

## One-shot and background tool runs

Use a one-shot command when the tool waits for a result. Use a background job
when the process should continue after the tool call or agent run ends. Each
language has both APIs:

=== "Rust"

    **One-shot within an agent run:**

    ```rust
    // Inside a tool called during `agent_run`:
    let result = agent_run.run_command(&command, &policy, &[]).await?;
    println!("{}", String::from_utf8_lossy(&result.stdout));
    ```

    **Background job:**

    ```rust
    use std::time::Duration;

    let job_id = supervisor.start_background(&command, &policy)?;
    // Later, stop this job and collect its result.
    supervisor.shutdown_process(job_id, Duration::from_secs(2)).await?;
    let result = supervisor.wait_process(job_id).await?.unwrap();
    ```

    See [Rust persistent processes](rust-api.md#persistent-processes).

=== "Python"

    **One-shot within an agent tool:**

    ```python
    import sys

    from agent_supervisor import Command, Policy, run as run_tool_command

    result = await run_tool_command(
        Command(argv=[sys.executable, "-c", "print('done')"]),
        policy=Policy(),
    )
    print(result.termination, result.exit_code, result.stdout.decode())
    ```

    **Background job:**

    ```python
    import sys

    from agent_supervisor import Command, Policy

    job_id = await supervisor.start_background(
        Command([sys.executable, "-m", "http.server"], cwd="."),
        policy=Policy(),
    )
    # Later, stop this job and collect its result.
    await supervisor.shutdown_process(job_id, grace=2.0)
    result = await supervisor.wait_process(job_id)
    print(result.termination, result.exit_code)
    ```

    `shutdown_all()` stops remaining jobs when the application exits. See the
    [Python API](python-api.md#supervisor-manager) for process controls.

=== "Node.js"

    **One-shot within an agent run:**

    ```js
    import { Command, Policy } from '@rooagi/agent-supervisor'

    const result = await agentRun.run(async (scope) =>
      scope.runCommand(
        new Command([process.execPath, '-e', "console.log('done')"]),
        { policy: new Policy() },
      )
    )
    console.log(result.stdout.toString())
    ```

    **Background job:**

    ```js
    import { Command, Policy } from '@rooagi/agent-supervisor'

    const jobId = await supervisor.startBackground(
      new Command([process.execPath, '-e', 'setInterval(() => {}, 1000)']),
      { policy: new Policy() },
    )
    // Later, stop this job and collect its result.
    await supervisor.shutdownProcess(jobId, { graceMs: 2_000 })
    const result = await supervisor.waitProcess(jobId)
    console.log(result?.termination, result?.exitCode)
    ```

    `shutdownAll()` stops remaining jobs when the application exits. See the
    [Node package guide](https://github.com/RooAGI/agent-supervisor/blob/main/clients/node/README.md)
    for process controls.

For streaming output, manually creating an agent run, or saving and resuming
work, see the language references:

- [Rust API](rust-api.md)
- [Python API](python-api.md)
- [Node package guide](https://github.com/RooAGI/agent-supervisor/blob/main/clients/node/README.md)

## Next steps

- [Core concepts](api-reference.md): how commands, policies, run results, and
  process events fit together.
- [Python API](python-api.md): Python arguments, return values, streaming, and
  persistent processes.
- [Rust API](rust-api.md): Rust types, methods, and examples.
- [Node package guide](https://github.com/RooAGI/agent-supervisor/blob/main/clients/node/README.md): embedded Node API, service mode, and framework adapters.
- [Use with agent SDKs](api-boundaries.md): examples for OpenAI Agents SDK,
  Microsoft Agent Framework, LangGraph, PydanticAI, and Google ADK.
- [MCP servers](mcp.md): supervise an existing Rust or Python stdio MCP server.
