# agent-supervisor for Python

Python bindings for the `agent-supervisor` native process supervisor.

Version 0.1.3 provides bounded one-shot execution and output-event streaming
through the native Linux, macOS, and Windows backends. The Rust crate's optional
WASM runner is not exposed by the Python binding. The package has not yet been
published to PyPI; build it from this checkout.

## Build from this checkout

Create and activate a virtual environment, then run:

```bash
python -m pip install maturin
cd python
python -m maturin develop
```

The API and supported platform behavior are documented in
[`../docs/python-api.md`](../docs/python-api.md).
