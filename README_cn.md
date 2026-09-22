# OpenEvent

[English version](README.md)

OpenEvent 是面向 AI Agent 系统的基础设施，核心组件是一条有序消息队列，系统中的各个模块统一连接到这条消息队列，从而获得全局一致的事件队列。

围绕这条队列，OpenEvent 提供了一组现成的可插拔模块。开发者可以先用这些模块快速搭建
一个可运行、易调试的 Agent，再按业务场景逐步替换 IM 接入、模型代理、Agent 策略或查看面板。

## 可插拔模块

| 模块 | 项目 | 作用 |
| --- | --- | --- |
| IM 模块 | [openevent-modules-im](https://github.com/openevent-official/openevent-modules-im) | 定义 IM payload 协议，提供 IM 同步 worker，把外部会话接入 OpenEvent。 |
| Model Proxy | [openevent-modules-model-proxy](https://github.com/openevent-official/openevent-modules-model-proxy) | 对接 OpenAI 兼容模型 provider，把模型请求和结果写入事件队列。 |
| Cmd 模块 | [openevent-modules-cmd](https://github.com/openevent-official/openevent-modules-cmd) | 定义 `cmd.v1` 命令执行协议，提供 Linux 本地命令执行 worker，把命令请求、执行结果和输出查询写入事件队列。 |
| OpenEvent View | [openevent-view](https://github.com/openevent-official/openevent-view) | 查询 OpenEvent 中的事件记录。 |

## 快速搭建一个 Agent Demo

[openevent-agent-demo](https://github.com/openevent-official/openevent-agent-demo)
已经把 OpenEvent、IM 模块、model-proxy、Agent 进程和 OpenEvent View 组合成一套本地运行环境，
适合用来验证模块边界、调试事件链路，并作为业务 Agent 的起点。

这个 demo 展示了 OpenEvent 推荐的模块边界：IM 事件、模型请求、模型结果、Agent WAL
和最终回复都会写入同一条 OpenEvent 事件队列。

## 特性

- 基于 `seq` 的全局有序消息。
- 永久保留 `seq=0` 的 `system.v1` 初始化消息，业务消息从 `seq=1` 开始。
- 服务端分配单调递增 UUID，用于消息去重和已提交 seq 查询。
- 支持客户端指定 `seq` 发布，也支持服务端自动分配 `seq`。
- Channel 支持 public、protected、private 三种可见性。
- 支持批量拉取和服务端流式订阅。
- 支持最大 4 MiB 的不可变对象存储，并可把有序 ObjectKey capability 附加到消息。
- 基于 token 的业务请求认证。
- 提供 SDK 子模块入口。

## 仓库结构

```text
openevent/
├── Makefile
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
    └── proto/
        ├── admin.proto
        └── openevent.proto
```

## 构建

请先安装当前 Linux 发行版对应的构建依赖：

- CMake 3.20+
- C++20 编译器
- Protobuf 和 `protoc`
- gRPC 和 `grpc_cpp_plugin`
- RocksDB
- OpenSSL
- yaml-cpp

拉取子模块：

```bash
git submodule update --init --recursive
```

配置构建目录：

```bash
make configure
```

构建：

```bash
make build
```

运行测试：

```bash
make test
```

`make test` 包含核心和存储故障注入测试。运行服务端启动、重启与关闭回归：

```bash
make test-runtime
```

该 target 使用当前 Python 环境中已安装的 `openevent-sdk>=0.8.0`、`pytest` 和 `packaging`，
不自动安装 SDK。通过 `PYTHON` 选择 Python 环境，通过 `BUILD_TYPE`、`JOBS` 和 `CMAKE_ARGS` 设置构建参数。
测试数据与日志保存在 `build/`。
文档修改后运行 `make check-docs`，检查中英文章节结构、当前版本引用和本地链接。

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
make build BUILD_TYPE=Release CMAKE_ARGS="-DCMAKE_INSTALL_BINDIR=sbin"
cmake --install build --prefix /opt/openevent
```

此时可执行文件会安装到：

```text
/opt/openevent/sbin/openevent_server
```

`cmake --install build` 通常不会自动重新编译源码；安装前请先执行构建步骤。

也可以使用 Makefile 封装，一步完成构建和安装：

```bash
make install PREFIX=/opt/openevent
```

`make install` 先构建当前源码，只有构建成功后才安装本次产物；构建失败时停止安装。
默认安装前缀为 `/usr/local`，可以通过 `PREFIX` 修改。

## 配置

服务端启动时必须传入一个有效的 YAML 配置文件路径；配置文件缺失或类型无效时，
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
[openevent-sdk](https://github.com/openevent-official/openevent-sdk) 仓库。

## 文档

- [OpenEvent 博客](https://openevent-official.github.io/openevent-blog/)
- [配置说明](docs/CONFIG_cn.md)
- [API 说明](docs/API_cn.md)
- [SDK 仓库](https://github.com/openevent-official/openevent-sdk)
- [业务协议定义](https://github.com/openevent-official/openevent-sdk/blob/main/proto/openevent.proto)
- [管理协议定义](https://github.com/openevent-official/openevent-sdk/blob/main/proto/admin.proto)
- [Python SDK](https://github.com/openevent-official/openevent-sdk/blob/main/README_cn.md)
- [Python SDK 使用指南](https://github.com/openevent-official/openevent-sdk/blob/main/docs/USAGE_cn.md)
- [SDK API 契约](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md)
- [系统消息协议](https://github.com/openevent-official/openevent-sdk/blob/main/docs/SYSTEM_PROTOCOL_cn.md)

文档面向 GitHub 原生 Markdown 渲染编写，不需要额外的文档构建步骤。

## GitHub 开源前检查

- SDK 子模块已指向 `https://github.com/openevent-official/openevent-sdk.git`。
- 确认 [LICENSE](LICENSE) 中的版权主体。
- 仓库公开后，建议启用 GitHub private vulnerability reporting。

## 项目状态

当前服务端版本为 `0.8.0`，SDK 版本为 `0.8.1`，按新数据目录部署，不兼容旧数据目录或旧协议。公开 API 行为以
[SDK API 契约](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md)
为准；调用方应依赖文档化的 gRPC 契约，避免依赖服务端实现细节。
