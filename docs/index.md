---
hide:
  - navigation
  - toc
---

<div class="landing">
  <section class="landing-hero">
    <div class="landing-hero__copy">
      <p class="eyebrow"><span class="eyebrow__dot"></span> ROOAGI · AGENT SANDBOX</p>
      <h1>A reliable process boundary for AI agents.</h1>
      <p class="landing-hero__lede">Your runtime decides what an agent may run. Agent Sandbox supervises how it runs: with explicit policy, native operating-system controls, and cleanup you can count on.</p>
      <div class="landing-hero__actions">
        <a class="md-button md-button--primary" href="quickstart/">Get started</a>
        <a class="md-button" href="https://github.com/RooAGI/AgentSandbox">Explore on GitHub <span aria-hidden="true">↗</span></a>
      </div>
      <div class="landing-hero__meta"><span>OPEN SOURCE</span><span>RUST</span><span>APACHE 2.0</span></div>
    </div>

    <div class="execution-visual" aria-label="An approved execution request passes through Agent Sandbox and becomes a supervised child process">
      <div class="execution-visual__top"><span><i></i><i></i><i></i></span><span>EXECUTION FLOW</span><span>01 — 04</span></div>
      <div class="execution-flow">
        <div class="flow-node flow-node--runtime"><span class="flow-node__icon">01</span><div><small>YOUR RUNTIME</small><strong>Authorization</strong></div><b>✓</b></div>
        <div class="flow-link"><span>explicit request</span></div>
        <div class="flow-node flow-node--sandbox"><span class="flow-node__icon">02</span><div><small>AGENT SANDBOX</small><strong>Policy + supervision</strong></div><b>●</b></div>
        <div class="flow-controls"><span>TIME</span><span>OUTPUT</span><span>FILESYSTEM</span><span>RESOURCES</span></div>
        <div class="flow-link"><span>bounded process</span></div>
        <div class="flow-node flow-node--child"><span class="flow-node__icon">03</span><div><small>CHILD PROCESS</small><strong>Tool, MCP server, pipeline</strong></div><b>↗</b></div>
      </div>
      <div class="execution-visual__foot"><span><i></i> SUPERVISED</span><code>cancel → stop → collect result</code></div>
    </div>
  </section>

  <section class="capability-ribbon" aria-label="Project highlights">
    <div><strong>One Rust API</strong><span>for agent runtimes</span></div>
    <div><strong>Linux · macOS · Windows</strong><span>native platform backends</span></div>
    <div><strong>Explicit policy</strong><span>no inferred authority</span></div>
    <div><strong>Predictable cleanup</strong><span>from cancel to result</span></div>
  </section>

  <section class="landing-section">
    <div class="section-heading">
      <p class="eyebrow">CONTROL THE EXECUTION</p>
      <h2>Contain the work.<br><span>Keep authority with your runtime.</span></h2>
      <p>The sandbox takes a request your application has already authorized and gives it a managed lifecycle. Your runtime keeps credentials, identity, and tool policy.</p>
    </div>
    <div class="feature-grid">
      <article><span class="feature-number">01 / BOUND</span><h3>Set limits before launch</h3><p>Choose the executable, environment, filesystem grants, network mode, deadlines, input, output, and resource limits for each request.</p><a href="quickstart/">Build an execution request <span aria-hidden="true">→</span></a></article>
      <article><span class="feature-number">02 / ENFORCE</span><h3>Use native controls</h3><p>Apply operating-system isolation where supported, and report when a requested enforcement mode is unavailable.</p><a href="security-model/">Read the security model <span aria-hidden="true">→</span></a></article>
      <article><span class="feature-number">03 / SUPERVISE</span><h3>Finish cleanly</h3><p>Track process identity and lifecycle, support cancellation, and move from graceful shutdown to bounded forced cleanup.</p><a href="supervisor-lifecycle/">Explore supervision <span aria-hidden="true">→</span></a></article>
    </div>
  </section>

  <section class="workflow-section">
    <div class="workflow-copy"><p class="eyebrow">A CLEAR CONTRACT</p><h2>From authorized request to structured result.</h2><p>Execution stays legible to the system that owns the agent. The caller declares the policy; Agent Sandbox enforces and supervises the process; the runtime receives the outcome.</p><a class="text-link" href="api-boundaries/">Understand the API boundary <span aria-hidden="true">→</span></a></div>
    <div class="workflow-steps" role="list" aria-label="Execution lifecycle">
      <div role="listitem"><span>01</span><strong>Declare</strong><small>Executable · policy · limits</small></div>
      <div role="listitem"><span>02</span><strong>Launch</strong><small>Platform backend applies controls</small></div>
      <div role="listitem"><span>03</span><strong>Supervise</strong><small>Events · cancellation · cleanup</small></div>
      <div role="listitem"><span>04</span><strong>Return</strong><small>Output · status · receipt</small></div>
    </div>
  </section>

  <section class="platform-section">
    <div><p class="eyebrow">BUILT FOR AGENT WORKFLOWS</p><h2>One contract across the tools agents depend on.</h2><p>Run a short-lived tool, keep an MCP server alive, or coordinate a multi-process pipeline through a consistent execution API.</p></div>
    <div class="platform-list"><div><span>01</span><strong>Tool adapters</strong><small>Bound commands, inputs, and output</small></div><div><span>02</span><strong>MCP servers</strong><small>Manage long-lived child processes</small></div><div><span>03</span><strong>Agent platforms</strong><small>Inspect identity, events, and resources</small></div><div><span>04</span><strong>Desktop and CLI agents</strong><small>Support PTYs, cancellation, and cleanup</small></div></div>
  </section>

  <aside class="boundary-note"><span class="boundary-note__mark">i</span><div><strong>A process sandbox, with a clear security boundary.</strong><p>Agent Sandbox is not a complete container runtime. The caller must authenticate and authorize each request. Read the <a href="security-model/">security model</a> before relying on an enforcement feature.</p></div></aside>

  <section class="final-cta"><p class="eyebrow">AGENT SANDBOX · 0.1.0</p><h2>Make every tool run easier to trust.</h2><p>Start with the Rust quickstart, then choose the controls your runtime needs.</p><div><a class="md-button md-button--primary" href="quickstart/">Read the quickstart</a><a class="text-link" href="releases/0.1.0/">Review release scope <span aria-hidden="true">→</span></a></div></section>
</div>
