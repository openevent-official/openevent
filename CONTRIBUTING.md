# Contributing

[中文版](CONTRIBUTING_cn.md)

## Development Environment

Before submitting changes, make sure the following checks can pass locally:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

After modifying protobuf or SDK-related files, also verify the SDK submodule
according to `openevent-sdk/README.md`.

## Contribution Guidelines

- Keep the public API behavior aligned with `openevent-sdk/docs/API.md`.
- Do not depend on generated files that are not tracked by Git.
- Keep build products, RocksDB data, temporary tokens, and logs out of commits.
- Prefer small, verifiable changes with corresponding tests.
- Update documentation when behavior, configuration, or public contracts change.
- Keep Markdown links valid for GitHub browsing.

## Pull Requests

A pull request should include:

- A clear summary of the change.
- The commands used for build and test verification.
- Notes about compatibility impact, if any.
- Documentation updates when public behavior changes.
