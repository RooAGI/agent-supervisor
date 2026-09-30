---
hide:
  - navigation
  - toc
---

<div class="landing">
  <section class="landing-hero">
    <div class="landing-hero__copy">
      <p class="eyebrow"><span class="eyebrow__dot"></span> ROOAGI · AGENT SUPERVISOR</p>
      <h1>Control every tool process your application starts.</h1>
      <p class="landing-hero__lede">Bound its output. Cancel it. Clean up its child processes. Record how it finished. Agent Supervisor brings those process responsibilities into one Rust library, so your application does not have to rebuild them around subprocess APIs.</p>
      <div class="landing-hero__actions">
        <a class="md-button md-button--primary" href="quickstart/">Get started</a>
        <a class="md-button" href="https://github.com/RooAGI/agent-supervisor">Explore on GitHub <span aria-hidden="true">↗</span></a>
      </div>
      <div class="landing-hero__meta"><span>OPEN SOURCE</span><span>RUST</span><span>APACHE 2.0</span></div>
    </div>

    <div class="execution-visual" aria-label="An approved execution request passes through Agent Supervisor and becomes a supervised child process">
      <div class="execution-visual__top"><span><i></i><i></i><i></i></span><span>EXECUTION FLOW</span><span>01 — 04</span></div>
      <div class="execution-flow">
        <div class="flow-node flow-node--runtime"><span class="flow-node__icon">01</span><div><small>YOUR RUNTIME</small><strong>Authorization</strong></div><b>✓</b></div>
        <div class="flow-link"><span>explicit request</span></div>
        <div class="flow-node flow-node--sandbox"><span class="flow-node__icon">02</span><div><small>AGENT SUPERVISOR</small><strong>Policy + supervision</strong></div><b>●</b></div>
        <div class="flow-controls"><span>TIME</span><span>OUTPUT</span><span>FILESYSTEM</span><span>RESOURCES</span></div>
        <div class="flow-link"><span>bounded process</span></div>
        <div class="flow-node flow-node--child"><span class="flow-node__icon">03</span><div><small>CHILD PROCESS</small><strong>Tool, MCP server, pipeline</strong></div><b>↗</b></div>
      </div>
      <div class="execution-visual__foot"><span><i></i> SUPERVISED</span><code>bound → cancel → clean up → record</code></div>
    </div>
  </section>

  <section class="capability-ribbon" aria-label="Project highlights">
    <div><strong>Rust core + Python API</strong><span>one-shot execution and streaming</span></div>
    <div><strong>Native · OpenShell preview</strong><span>local execution and adapter experiments</span></div>
    <div><strong>Explicit policy</strong><span>no inferred authority</span></div>
    <div><strong>Structured outcomes</strong><span>with lifecycle records</span></div>
  </section>

  <section class="landing-section">
    <div class="section-heading">
      <p class="eyebrow">CONTROL THE EXECUTION</p>
      <h2>Own the process lifecycle.<br><span>Keep authority with your application.</span></h2>
      <p>Subprocess APIs start a process; your application still has to manage its output, cancellation, descendants, and final status. Agent Supervisor gives those jobs a shared API. Your runtime keeps credentials, identity, and authorization.</p>
    </div>
    <div class="feature-grid">
      <article><span class="feature-number">01 / BOUND</span><h3>Keep output within limits</h3><p>Set stdout, stderr, input, and deadline limits on an execution request, so a noisy or stuck tool cannot consume unbounded application resources.</p><a href="quickstart/">Build an execution request <span aria-hidden="true">→</span></a></article>
      <article><span class="feature-number">02 / CANCEL</span><h3>Stop work and its descendants</h3><p>Cancel natively supervised work and manage process groups through graceful shutdown and bounded forced cleanup, within the controls supported by the platform.</p><a href="supervisor-lifecycle/">Explore supervision <span aria-hidden="true">→</span></a></article>
      <article><span class="feature-number">03 / RECORD</span><h3>Know how it finished</h3><p>Collect output, termination reason, process identity, and lifecycle events as structured results for the runtime that started the work.</p><a href="api-boundaries/">Understand the API boundary <span aria-hidden="true">→</span></a></article>
    </div>
  </section>

  <section class="workflow-section">
    <div class="workflow-copy"><p class="eyebrow">A CLEAR CONTRACT</p><h2>From authorized request to recorded outcome.</h2><p>Your application chooses what may run. Agent Supervisor applies the requested process controls, supervises the lifecycle, and returns the outcome. Enforcement varies by platform and capability; the receipt and capability report show what actually happened.</p><a class="text-link" href="api-boundaries/">Understand the API boundary <span aria-hidden="true">→</span></a></div>
    <div class="workflow-steps" role="list" aria-label="Execution lifecycle">
      <div role="listitem"><span>01</span><strong>Declare</strong><small>Executable · policy · limits</small></div>
      <div role="listitem"><span>02</span><strong>Launch</strong><small>Platform backend applies controls</small></div>
      <div role="listitem"><span>03</span><strong>Supervise</strong><small>Events · cancellation · cleanup</small></div>
      <div role="listitem"><span>04</span><strong>Record</strong><small>Output · termination · identity</small></div>
    </div>
  </section>

  <section class="platform-section">
    <div><p class="eyebrow">FOR DEVELOPERS BUILDING WITH PROCESSES</p><h2>Stop rebuilding subprocess supervision in every application.</h2><p>When a tool times out but its child keeps running, or an MCP server needs an orderly shutdown, custom wrappers grow quickly. Agent Supervisor puts execution, process groups, lifecycle handling, and outcomes behind one Rust API, with an async Python binding for bounded one-shot commands and output streaming. Choose native execution across Linux, macOS, and Windows, or experiment with a separate OpenShell developer adapter for one-shot commands in an existing managed sandbox.</p><a href="python-api/">Read the Python API guide <span aria-hidden="true">→</span></a><br><a href="openshell/">Read the OpenShell developer adapter contract <span aria-hidden="true">→</span></a></div>
    <div class="platform-list"><div><span>01</span><strong>Rust desktop and CLI apps</strong><small>Embed process control in your application</small></div><div><span>02</span><strong>Agent frameworks</strong><small>Share execution and cleanup across tool adapters</small></div><div><span>03</span><strong>MCP server managers</strong><small>Handle startup, readiness, shutdown, and children</small></div><div><span>04</span><strong>Persistent subprocess workflows</strong><small>Track process groups, identity, and lifecycle events</small></div></div>
  </section>

  <aside class="boundary-note"><span class="boundary-note__mark">i</span><div><strong>A process supervisor, with a clear security boundary.</strong><p>Agent Supervisor is not a complete container runtime. The caller must authenticate and authorize each request. Read the <a href="security-model/">security model</a> before relying on an enforcement feature.</p></div></aside>

  <section class="final-cta"><p class="eyebrow">AGENT SUPERVISOR · 0.1.2</p><h2>Give every tool process a lifecycle.</h2><p>Start with the Rust or Python guide. Set the limits you need, supervise the work, and inspect its recorded outcome.</p><div><a class="md-button md-button--primary" href="quickstart/">Rust quickstart</a><a class="md-button" href="python-api/">Python API</a><a class="text-link" href="releases/0.1.2/">Review release scope <span aria-hidden="true">→</span></a></div></section>
</div>
