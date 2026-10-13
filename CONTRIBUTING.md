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

`make test-runtime` builds the current server, then uses the SDK's `make test-env`
to build and install a fresh SDK wheel from local sources before running in that
test environment. See the [SDK README](openevent-sdk/README.md#build-and-test)
for test environment and dependency rules. Select the Python used to create the
test environment with `PYTHON`. Server test data and logs stay under `build/`.

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
and results.
