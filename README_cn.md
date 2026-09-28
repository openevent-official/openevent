# OpenEvent

[English version](README.md)

OpenEvent 是面向 AI Agent 系统的基础设施。各模块连接到同一条全局有序事件队列，通过消息协作。
开发者可以使用现成模块搭建 Agent，再按业务需要替换 IM 接入、模型代理、Agent 策略或查看面板。

## 可插拔模块

| 模块 | 项目 | 作用 |
| --- | --- | --- |
| IM 模块 | [openevent-modules-im](https://github.com/openevent-official/openevent-modules-im) | 接入外部 IM 会话。 |
| Model Proxy | [openevent-modules-model-proxy](https://github.com/openevent-official/openevent-modules-model-proxy) | 对接 OpenAI 兼容模型 provider。 |
| Cmd 模块 | [openevent-modules-cmd](https://github.com/openevent-official/openevent-modules-cmd) | 执行 Linux 本地命令并查询输出。 |
| OpenEvent View | [openevent-view](https://github.com/openevent-official/openevent-view) | 查询事件记录。 |

## Agent Demo

[openevent-agent-demo](https://github.com/openevent-official/openevent-agent-demo)
把 OpenEvent、IM、model-proxy、Agent 进程和 OpenEvent View 组合成一套本地运行环境，可用来调试事件链路，
也可作为业务 Agent 的起点。

## 特性

- 全局有序消息、服务端 UUID 分配和消息去重。
- 支持客户端指定 `seq` 发布和服务端自动分配 `seq`。
- Channel 支持 public、protected、private 三种可见性。
- 批量拉取和流式订阅。
- 最大 4 MiB 的不可变对象存储，消息可携带对象读取凭据。
- 基于 `(principal, token)` 组合的业务认证，以及独立管理端口。
- 提供 Python SDK。

## 构建

先安装当前 Linux 发行版对应的依赖：CMake 3.20+、C++20 编译器、Protobuf 和 `protoc`、
gRPC 和 `grpc_cpp_plugin`、RocksDB、OpenSSL、yaml-cpp。

```bash
git submodule update --init --recursive
make build
```

产物为 `build/openevent_server`。可通过 `BUILD_TYPE`、`JOBS` 和 `CMAKE_ARGS` 设置构建参数，例如：

```bash
make build BUILD_TYPE=Release JOBS=4
```

测试命令见 [参与贡献](CONTRIBUTING_cn.md)。

## 安装

```bash
make install PREFIX=/opt/openevent BUILD_TYPE=Release
```

该命令先构建当前源码，成功后安装到 `/opt/openevent/bin/openevent_server`。
默认安装前缀为 `/usr/local`，使用 `PREFIX` 修改。

## 运行与部署

按 [配置说明](docs/CONFIG_cn.md) 准备 YAML 配置文件，然后运行：

```bash
/opt/openevent/bin/openevent_server /etc/openevent/openevent-server.yaml
```

本地开发也可以使用 `build/openevent_server /path/to/openevent-server.yaml`。
部署前请阅读配置说明中的文件系统、数据目录和备份要求，以及 [安全策略](SECURITY_cn.md) 中的端口与传输保护要求。

## SDK 与文档

- [Python SDK](https://github.com/openevent-official/openevent-sdk)：构建、安装与使用入口。
- [API 与协议](docs/API_cn.md)
- [配置说明](docs/CONFIG_cn.md)
- [参与贡献](CONTRIBUTING_cn.md)
- [OpenEvent 博客](https://openevent-official.github.io/openevent-blog/)

## 项目状态

服务端和 SDK 使用 `0.11.0` 配套版本，升级时使用新数据目录。
