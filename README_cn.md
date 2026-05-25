# OpenEvent

[English version](README.md)

OpenEvent 是一个基于 gRPC 的事件通道服务，适用于需要全局有序事件流和
Channel 级访问控制的应用。

服务端提供消息发布、历史消息拉取、新消息订阅、Channel 管理和 token 管理
接口。共享的 Protocol Buffers 协议和 SDK 文档位于 `openevent-sdk` 子模块。

## 特性

- 基于 `seq` 的全局有序消息。
- 支持客户端指定 `seq` 发布，也支持服务端自动分配 `seq`。
- Channel 支持 public、protected、private 三种可见性。
- 支持批量拉取和服务端流式订阅。
- 基于 token 的业务请求认证。
- 提供 SDK 子模块入口。

## 仓库结构

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

## 构建

请先安装当前 Linux 发行版对应的构建依赖：

- CMake 3.20+
- C++20 编译器
- Protobuf 和 `protoc`
- gRPC 和 `grpc_cpp_plugin`
- RocksDB
- yaml-cpp

拉取子模块：

```bash
git submodule update --init --recursive
```

配置构建目录：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
```

构建：

```bash
cmake --build build -j2
```

运行测试：

```bash
ctest --test-dir build --output-on-failure
```

构建产物位于：

```text
build/openevent_server
```

常用 CMake 选项：

- `CMAKE_BUILD_TYPE=Debug|Release`
- `BUILD_TESTING=ON|OFF`
- `CMAKE_INSTALL_BINDIR=bin|sbin|...`

## 安装

构建和安装是两个不同步骤。`cmake --build` 负责编译并生成构建目录中的产物，
`cmake --install` 负责把已经构建好的产物复制到指定安装路径。

安装到指定路径：

```bash
cmake --install build --prefix /opt/openevent
```

默认安装后的可执行文件路径为：

```text
/opt/openevent/bin/openevent_server
```

如需修改可执行文件安装子目录，在配置阶段传入 `CMAKE_INSTALL_BINDIR`：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_BINDIR=sbin
cmake --build build -j2
cmake --install build --prefix /opt/openevent
```

此时可执行文件会安装到：

```text
/opt/openevent/sbin/openevent_server
```

`cmake --install build` 通常不会自动重新编译源码；安装前请先执行构建步骤。

## 配置

服务端启动时必须传入一个有效的 YAML 配置文件路径；配置文件不存在或不是普通文件时，
服务端会拒绝启动。

配置示例、字段说明和部署注意事项见 [配置说明](docs/CONFIG_cn.md)。

## 运行

从源码构建目录运行：

```bash
build/openevent_server /path/to/openevent-server.yaml
```

安装后运行：

```bash
/opt/openevent/bin/openevent_server /path/to/openevent-server.yaml
```

如果安装时把 `CMAKE_INSTALL_BINDIR` 设为 `sbin`，运行路径相应为：

```bash
/opt/openevent/sbin/openevent_server /path/to/openevent-server.yaml
```

## 部署

部署时请使用安装后的可执行文件，并传入部署环境中的配置文件路径。配置文件位置、
数据目录权限和管理端口安全要求见 [配置说明](docs/CONFIG_cn.md)。

## SDK

SDK 的构建、安装和测试说明见
[openevent-sdk](https://github.com/openevent2026/openevent-sdk) 仓库。

## 文档

- [配置说明](docs/CONFIG_cn.md)
- [API 说明](docs/API_cn.md)
- [SDK 仓库](https://github.com/openevent2026/openevent-sdk)
- [协议定义](https://github.com/openevent2026/openevent-sdk/blob/main/proto/openevent.proto)
- [Python SDK](https://github.com/openevent2026/openevent-sdk/blob/main/README_cn.md)
- [Python SDK 使用指南](https://github.com/openevent2026/openevent-sdk/blob/main/docs/USAGE_cn.md)
- [SDK API 契约](https://github.com/openevent2026/openevent-sdk/blob/main/docs/API_cn.md)

文档面向 GitHub 原生 Markdown 渲染编写，不需要额外的文档构建步骤。

## GitHub 开源前检查

- SDK 子模块已指向 `https://github.com/openevent2026/openevent-sdk.git`。
- 确认 [LICENSE](LICENSE) 中的版权主体。
- 仓库公开后，建议启用 GitHub private vulnerability reporting。

## 项目状态

OpenEvent 仍处于早期阶段。公开 API 行为以
[SDK API 契约](https://github.com/openevent2026/openevent-sdk/blob/main/docs/API_cn.md)
为准；调用方应依赖文档化的 gRPC 契约，而不是服务端实现细节。
