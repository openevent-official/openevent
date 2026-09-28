# Contributing

[中文版](CONTRIBUTING_cn.md)

## Development and Validation

Install dependencies and initialize submodules as described in the
[README](README.md#build), then run:

```bash
make test
make test-runtime
make check-docs
```

`make test-runtime` requires `openevent-sdk>=0.10.0`, `pytest`, and `packaging`
already installed in the current Python environment.
Select Python with `PYTHON`. Test data and logs stay under `build/`.

## Contribution Guidelines

- Add or update tests for behavior changes, and update public documentation and both translations.
- The gRPC protocol and API contract are maintained in the
  [SDK repository](https://github.com/openevent-official/openevent-sdk).
  Follow its contribution and validation instructions for SDK changes.
- Keep public documentation focused on usage, configuration, and API behavior,
  with links that work when browsing GitHub.
- Do not commit generated files, runtime data, credentials, or logs.

## Pull Requests

Keep changes small and verifiable. Describe their purpose, validation commands
and results, and compatibility impact, if any.
