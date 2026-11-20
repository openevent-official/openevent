# Contributing

[中文版](CONTRIBUTING_cn.md)

## Development and Validation

Install dependencies and initialize submodules as described in the [README](README.md#build).
The runtime and SDK end-to-end tests install the freshly built SDK in the development
environment selected by `PYTHON`. Use an existing environment, or run the first two
lines in the server directory to create and select one.

```bash
python3 -B -m venv build/venv
export PYTHON="$PWD/build/venv/bin/python"
make test test-runtime &&
    OPENEVENT_SERVER_BIN="$PWD/build/openevent_server" make -C openevent-sdk e2e
```

`make build` configures CMake and builds the server, generating C++ sources from the SDK
submodule's protocol files. `make install` installs the resulting executable.
`make test` enables tests in the current build directory even if a previous build disabled them.
It runs the C++ core tests and checks that the server executable is fully statically linked.
CTest reports failure if it finds no tests.

`make test-runtime` builds the current server, calls the SDK's `make test-env`
to install the freshly built wheel and test dependencies, then runs the server tests
with the same Python. Running `make test test-runtime` builds the server once for both test suites.
Server test data and logs stay under `build/`.
See the [SDK README](openevent-sdk/README.md#build-and-test) for environment selection and installation details.
The SDK's `make e2e` uses the server built above to verify RPC integration and
end-to-end behavior, including batch object metadata queries.

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
