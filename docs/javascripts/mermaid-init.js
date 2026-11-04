document$.subscribe(() => {
  const mermaidBlocks = document.querySelectorAll("pre.mermaid:not([data-processed])");
  if (mermaidBlocks.length === 0 || !window.mermaid) return;

  window.mermaid.initialize({
    startOnLoad: false,
    themeVariables: {
      fontSize: "20px",
      nodePadding: 16,
    },
  });
  window.mermaid.run({ nodes: mermaidBlocks });
});
