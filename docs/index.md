---
hide:
  - navigation
  - toc
---

<div class="landing">
  <section class="landing-hero">
    <div class="landing-hero__copy">
      <p class="eyebrow"><span class="eyebrow__dot"></span> ROOAGI · AGENT SUPERVISOR</p>
      <h1>Supervise agent runs and the tools they start.</h1>
      <p class="landing-hero__lede">Set a deadline and cancellation lifecycle around a framework run. Apply process limits to its local tools, track the processes Agent Supervisor starts, and clean them up when the run ends.</p>
      <div class="landing-hero__actions">
        <a class="md-button md-button--primary" href="quickstart/">Get started</a>
        <a class="md-button" href="https://github.com/RooAGI/agent-supervisor">Explore on GitHub <span aria-hidden="true">↗</span></a>
      </div>
      <div class="landing-hero__meta"><span>OPEN SOURCE</span><span>RUST CORE</span><span>PYTHON SDK</span><span>APACHE 2.0</span></div>
    </div>

    <div class="execution-visual" aria-label="An agent framework run uses Agent Supervisor to manage its run lifecycle and the local tool processes it starts">
      <div class="execution-visual__top"><span><i></i><i></i><i></i></span><span>RUN + TOOL LIFECYCLE</span><span>01 — 04</span></div>
      <div class="execution-flow">
        <div class="flow-node flow-node--runtime"><span class="flow-node__icon">01</span><div><small>YOUR AGENT FRAMEWORK</small><strong>Run the agent</strong></div><b>✓</b></div>
        <div class="flow-link"><span>framework adapter</span></div>
        <div class="flow-node flow-node--sandbox"><span class="flow-node__icon">02</span><div><small>AGENT SUPERVISOR</small><strong>Set run deadline and cancellation</strong></div><b>●</b></div>
        <div class="flow-link"><span>agent calls an approved tool</span></div>
        <div class="flow-node flow-node--sandbox"><span class="flow-node__icon">03</span><div><small>TOOL SUPERVISOR</small><strong>Run local tool with limits</strong></div><b>●</b></div>
        <div class="flow-controls"><span>TIME</span><span>OUTPUT</span><span>FILES</span><span>RESOURCES</span></div>
        <div class="flow-link"><span>tracked and cleaned up</span></div>
        <div class="flow-node flow-node--child"><span class="flow-node__icon">04</span><div><small>LOCAL PROCESS</small><strong>Tool, MCP server, pipeline</strong></div><b>↗</b></div>
      </div>
      <div class="execution-visual__foot"><span><i></i> RUN + PROCESS LIFECYCLE</span><code>coordinate → bound → clean up → report</code></div>
    </div>
  </section>

  <section class="capability-ribbon" aria-label="Project highlights">
    <div><strong>Supported agent SDKs</strong><span>OpenAI Agents SDK · Microsoft Agent Framework · LangGraph · PydanticAI · Google ADK</span></div>
    <div><strong>Tool process control</strong><span>Rust core + Python API</span></div>
    <div><strong>MCP server lifecycle</strong><span>Rust and Python integrations</span></div>
    <div><strong>Structured outcomes</strong><span>with lifecycle records</span></div>
  </section>

  <section class="landing-section">
    <div class="section-heading">
      <p class="eyebrow">FOR AGENT FRAMEWORK RUNS</p>
      <h2>Give an agent run a deadline.<br><span>Clean up the tools it starts.</span></h2>
      <p>Wrap one framework run with Agent Supervisor. It tracks tool processes started through Agent Supervisor, requests cancellation when the run is cancelled or reaches its deadline, then cleans up those processes. Your framework still runs the agent and makes model requests.</p>
    </div>
    <p class="mcp-default-note"><strong>Run boundary:</strong> cancellation is cooperative for agent code and model requests. Agent Supervisor does not sandbox or forcibly terminate the agent’s Python process; it can stop the separate local tool processes it started.</p>
    <a class="text-link" href="api-boundaries/">See framework examples and run lifecycle <span aria-hidden="true">→</span></a>
  </section>

  <section class="landing-section">
    <div class="section-heading">
      <p class="eyebrow">FOR RUST AND PYTHON MCP HOSTS</p>
      <h2>Keep your MCP server.<br><span>Supervise how it runs.</span></h2>
      <p>Whether your host is written in Rust or Python, Agent Supervisor can own a local stdio MCP server's process lifecycle while your MCP client handles protocol messages.</p>
    </div>
    <div class="feature-grid mcp-feature-grid">
      <article><span class="feature-number">01 / RUST</span><h3>Wrap your `rmcp` server</h3><p>Run your existing stdio server through `run_stdio_server`. Agent Supervisor re-launches your program, forwards MCP traffic, and owns the server process tree.</p></article>
      <article><span class="feature-number">02 / PYTHON</span><h3>Change one server import</h3><p>With the official MCP Python SDK or Prefect FastMCP, keep your tools and client setup. The adapter forwards the stdio session to the supervised server.</p></article>
      <article><span class="feature-number">03 / LIFECYCLE</span><h3>Clean up server processes</h3><p>Close the client session, allow a short graceful shutdown, then clean up the MCP server and its child processes.</p></article>
      <article><span class="feature-number">04 / PYTHON CALL CONTROLS</span><h3>Limit tool calls</h3><p>The Python adapters can cap concurrent calls and result sizes, set a call deadline, and deny tools from discovery and direct calls.</p></article>
    </div>
    <p class="mcp-default-note"><strong>Defaults:</strong> Native Rust process policies allow host networking and unrestricted filesystem access, use best-effort enforcement, and clear the child environment. Python adapters preserve existing server configuration and add call and traffic limits. Configure explicit restrictions when needed; process supervision does not automatically create a filesystem or network sandbox.</p>
    <a class="text-link" href="mcp/">See Rust and Python MCP examples <span aria-hidden="true">→</span></a>
    <a class="text-link mcp-rust-link" href="supervisor-lifecycle/">See Rust process supervision <span aria-hidden="true">→</span></a>
  </section>

  <section class="landing-section">
    <div class="section-heading">
      <p class="eyebrow">NEW IN 0.3.0 · BACKGROUND JOBS</p>
      <h2>Start long-running processes.<br><span>Let the supervisor own them.</span></h2>
      <p>A host-owned <code>Supervisor</code> can start background jobs under stable process IDs, drain output, observe exit, and keep the final receipt for later lookup. Watch any job or agent run live with <code>stream()</code>. Rust and Python expose the same lifecycle.</p>
    </div>
    <div class="feature-grid">
      <article><span class="feature-number">01 / START</span><h3>Stable process IDs</h3><p>Start a job with <code>start_background()</code> and check status, wait for it, or remove its result by ID.</p></article>
      <article><span class="feature-number">02 / CONTROL</span><h3>Per-job control</h3><p>Write to stdin, send EOF, or stop one job without affecting the others.</p></article>
      <article><span class="feature-number">03 / AGENT RUNS</span><h3>Agent runs under the supervisor</h3><p>Create runs with <code>start_agent_run()</code>. Their tools share the supervisor's process registry and are cleaned up with the run.</p></article>
      <article><span class="feature-number">04 / SHUTDOWN</span><h3>Awaited cleanup</h3><p>Call <code>shutdown_all()</code> to stop remaining jobs and active runs, and collect their final receipts.</p></article>
    </div>
    <a class="text-link" href="releases/0.3.0/">Read the 0.3.0 release notes <span aria-hidden="true">→</span></a>
    <a class="text-link mcp-rust-link" href="supervisor-lifecycle/">See the supervisor lifecycle <span aria-hidden="true">→</span></a>
  </section>


</div>
