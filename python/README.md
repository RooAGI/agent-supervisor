# agent-supervisor for Python

Python bindings for the `agent-supervisor` native process supervisor.

Version 0.1.2 provides bounded one-shot execution and output-event streaming
through the native Linux, macOS, and Windows backends. The package has not yet
been published to PyPI; build it from this checkout.

## Build from this checkout

Create and activate a virtual environment, then run:

```bash
python -m pip install maturin
cd python
python -m maturin develop
```

The API and supported platform behavior are documented in
[`../docs/python-api.md`](../docs/python-api.md).
