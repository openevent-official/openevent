# OpenEvent

[中文版](README_cn.md)

OpenEvent is infrastructure for AI Agent systems. Modules share one globally
ordered event queue and collaborate through messages. Start with the available
modules, then replace the IM integration, model proxy, Agent strategy, or view
panel as needed.

## Pluggable Modules

| Module | Project | Purpose |
| --- | --- | --- |
| IM module | [openevent-modules-im](https://github.com/openevent-official/openevent-modules-im) | Connects external IM conversations. |
| Model Proxy | [openevent-modules-model-proxy](https://github.com/openevent-official/openevent-modules-model-proxy) | Connects OpenAI-compatible model providers. |
| Cmd module | [openevent-modules-cmd](https://github.com/openevent-official/openevent-modules-cmd) | Executes local Linux commands and queries their output. |
| OpenEvent View | [openevent-view](https://github.com/openevent-official/openevent-view) | Queries event records. |

## Agent Demo

[openevent-agent-demo](https://github.com/openevent-official/openevent-agent-demo)
combines OpenEvent, IM, model-proxy, an Agent process, and OpenEvent View into a
local runtime for debugging event chains or starting a business Agent.

## Features

- Globally ordered messages, server-allocated UUIDs, and message deduplication.
- Publishing with client-assigned or server-assigned `seq`.
- Channels with `public`, `protected`, and `private` visibility.
- Batch fetch and streaming subscriptions.
- Immutable object storage up to 4 MiB, with object read credentials attached to messages.
- Business authentication by `(principal, token)` pair and a separate admin endpoint.
- A Python SDK.

## Build

Install the dependencies for your Linux distribution: CMake 3.20+, a C++20
compiler, Protobuf and `protoc`, gRPC and `grpc_cpp_plugin`, RocksDB, OpenSSL,
yaml-cpp, and `pkg-config`.

```bash
git submodule update --init --recursive
make build
```

The binary is `build/openevent_server`. Set build options through `BUILD_TYPE`,
`JOBS`, and `CMAKE_ARGS`, for example:

```bash
make build BUILD_TYPE=Release JOBS=4
```

See [Contributing](CONTRIBUTING.md) for test commands.

## Install

```bash
make install PREFIX=/opt/openevent BUILD_TYPE=Release
```

This builds the current source and installs it to
`/opt/openevent/bin/openevent_server` only after a successful build.
The default installation prefix is `/usr/local`; override it with `PREFIX`.

## Run and Deploy

Prepare a YAML file using the [configuration guide](docs/CONFIG.md), then run:

```bash
/opt/openevent/bin/openevent_server /etc/openevent/openevent-server.yaml
```

For local development, use `build/openevent_server /path/to/openevent-server.yaml`.
Before deploying, read the filesystem, data directory, and backup requirements in
the configuration guide and the endpoint and transport requirements in the
[security policy](SECURITY.md).

## SDK and Documentation

- [Python SDK](https://github.com/openevent-official/openevent-sdk): build, installation, and usage.
- [API and protocols](docs/API.md)
- [Configuration](docs/CONFIG.md)
- [Contributing](CONTRIBUTING.md)
- [OpenEvent Blog](https://openevent-official.github.io/openevent-blog/en/)

## Project Status

Use matching `0.11.1` server and SDK versions.
