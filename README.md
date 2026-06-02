# OpenEvent

[中文版](README_cn.md)

OpenEvent is infrastructure for AI Agent systems. Its core component is an
ordered message queue. Modules in the system connect to this queue and share one
globally consistent event stream.

Around this queue, OpenEvent provides a set of ready-made, pluggable modules.
You can quickly build a running, debuggable Agent, then replace the IM
integration, model proxy, Agent strategy, or view panel according to your
business needs.

## Pluggable Modules

| Module | Project | Purpose |
| --- | --- | --- |
| IM module | [openevent-modules-im](https://github.com/openevent-official/openevent-modules-im) | Defines the IM payload protocol, provides an IM sync worker, and connects external conversations to OpenEvent. |
| Model Proxy | [openevent-modules-model-proxy](https://github.com/openevent-official/openevent-modules-model-proxy) | Connects OpenAI-compatible model providers and writes model requests and results to the event queue. |
| OpenEvent View | [openevent-view](https://github.com/openevent-official/openevent-view) | Queries event records stored in OpenEvent. |

## Build An Agent Demo Quickly

[openevent-agent-demo](https://github.com/openevent-official/openevent-agent-demo)
combines OpenEvent, the IM module, model-proxy, an Agent process, and OpenEvent
View into one local runtime. It is suitable for validating module boundaries,
debugging event chains, and serving as the starting point for a business Agent.

This demo shows OpenEvent's recommended module boundary: IM events, model
requests, model results, Agent WAL records, and final replies are all written to
the same OpenEvent event queue.

## Features

- Globally ordered messages based on `seq`.
- Supports both client-assigned `seq` publishing and server-assigned `seq`.
- Channels support `public`, `protected`, and `private` visibility.
- Supports batch fetch and server-streaming subscription.
- Token-based authentication for business requests.
- Includes a Python SDK submodule.

## Repository Layout

```text
openevent/
├── CMakeLists.txt
├── openevent-server.yaml
├── docs/
│   ├── API.md
│   └── CONFIG.md
├── src/
├── tests/
└── openevent-sdk/
    ├── docs/API.md
    ├── docs/USAGE.md
    └── proto/openevent.proto
```

## Build

Install the build dependencies for your Linux distribution first:

- CMake 3.20+
- C++20 compiler
- Protobuf and `protoc`
- gRPC and `grpc_cpp_plugin`
- RocksDB
- yaml-cpp

Initialize submodules:

```bash
git submodule update --init --recursive
```

Configure a build directory:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
```

Build:

```bash
cmake --build build -j2
```

Run tests:

```bash
ctest --test-dir build --output-on-failure
```

The server binary is generated at:

```text
build/openevent_server
```

Common CMake options:

- `CMAKE_BUILD_TYPE=Debug|Release`
- `BUILD_TESTING=ON|OFF`
- `CMAKE_INSTALL_BINDIR=bin|sbin|...`

## Install

Build and install are separate steps. `cmake --build` compiles the project and
generates artifacts under the build directory; `cmake --install` copies those
artifacts to an installation prefix.

Install to a specific prefix:

```bash
cmake --install build --prefix /opt/openevent
```

The default installed executable path is:

```text
/opt/openevent/bin/openevent_server
```

To change the executable subdirectory, pass `CMAKE_INSTALL_BINDIR` during
configuration:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_BINDIR=sbin
cmake --build build -j2
cmake --install build --prefix /opt/openevent
```

The executable is then installed at:

```text
/opt/openevent/sbin/openevent_server
```

`cmake --install build` usually does not rebuild source files automatically, so
run the build step before installing.

## Configuration

The server must be started with a valid YAML configuration file path. The server
rejects startup if the path is missing, does not exist, or is not a regular file.

See [Configuration](docs/CONFIG.md) for configuration examples, field
definitions, and deployment notes.

## Run

Run from the source build directory:

```bash
build/openevent_server /path/to/openevent-server.yaml
```

Run an installed binary:

```bash
/opt/openevent/bin/openevent_server /path/to/openevent-server.yaml
```

If `CMAKE_INSTALL_BINDIR` was set to `sbin`, adjust the path accordingly:

```bash
/opt/openevent/sbin/openevent_server /path/to/openevent-server.yaml
```

## Deployment

Use the installed executable in deployments, and pass a deployment-specific
configuration file. See [Configuration](docs/CONFIG.md) for config file
placement, data directory permissions, and admin port security requirements.

## SDK

See the [openevent-sdk](https://github.com/openevent-official/openevent-sdk)
repository for SDK build, installation, and test instructions.

## Documentation

- [OpenEvent Blog](https://openevent-official.github.io/openevent-blog/en/)
- [Configuration](docs/CONFIG.md)
- [API](docs/API.md)
- [SDK repository](https://github.com/openevent-official/openevent-sdk)
- [Protocol definition](https://github.com/openevent-official/openevent-sdk/blob/main/proto/openevent.proto)
- [Python SDK](https://github.com/openevent-official/openevent-sdk/blob/main/README.md)
- [Python SDK usage](https://github.com/openevent-official/openevent-sdk/blob/main/docs/USAGE.md)
- [SDK API contract](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md)

The documentation is written for GitHub's native Markdown renderer. No
documentation build step is required.

## Open Source Publishing Notes

- The SDK submodule points to
  `https://github.com/openevent-official/openevent-sdk.git`.
- Confirm the copyright holder in [LICENSE](LICENSE).
- Enable private vulnerability reporting in GitHub if the repository is public.

## Project Status

OpenEvent is still in an early stage. Public API behavior is defined by the
[SDK API contract](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md);
clients should rely on the documented gRPC contract, not server implementation
details.
