# OpenEvent 配置说明

[English version](CONFIG.md)

服务端启动时必须传入一个有效的 YAML 配置文件路径。配置文件不存在或不是普通文件时，
服务端会拒绝启动。

```bash
build/openevent_server /path/to/openevent-server.yaml
```

## 示例

```yaml
grpc:
  listen_addr: "0.0.0.0:9527"

admin:
  listen_addr: "127.0.0.1:9528"

storage:
  path: "/var/lib/openevent/data"

limits:
  max_payload_bytes: 16777216
```

## 字段

### `grpc`

- `listen_addr`：字符串，默认 `0.0.0.0:9527`
- 业务 gRPC 接口监听地址。
- 不能为空。

### `admin`

- `listen_addr`：字符串，默认 `127.0.0.1:9528`
- `AdminService` 监听地址。
- 不能为空，且不得与 `grpc.listen_addr` 相同。

### `storage`

- `path`：字符串，无默认值，必须通过配置显式提供。
- OpenEvent 数据根目录，由服务端管理消息、Channel、token 和对象数据。
- 不能为空。首次部署使用不存在或为空的新目录；目录不存在时由服务端初始化并写入永久的 seq 0
  `system.v1` 消息，运行用户必须拥有对应父目录的写入权限。后续启动只接受完整的目标数据目录；
  初始化未完成或目录不完整时，服务端拒绝启动，不在线修复。已经完成初始化的目录按正常启动流程检查。
- 消息和对象按 [API 保留策略](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#1-基础约定)
  永久保存；容量规划、告警阈值和停机处置由部署方按需安排，服务端不提供这些管理功能。

### `limits`

- `max_payload_bytes`：无符号整数，默认 `16777216`（16 MiB）
- 只限制 `Publish` 和 `PublishAutoSeq` 写入的单条消息 `payload` 字节数，不限制已提交消息的读取。
- `Publish` 和 `PublishAutoSeq` 收到超过该限制的消息时返回 `RESOURCE_EXHAUSTED`。
- 必须大于 0，且不能超过 `62914560` bytes（60 MiB）。超过上限时配置校验失败，服务端在打开两个监听
  端口前退出。

## 传输和分页预算

- 两个 gRPC Server 的单条收发消息硬上限固定为 `67108864` bytes（64 MiB），不随 `max_payload_bytes` 改变。
- Fetch 和 ListMessages 的每页响应软预算固定为 `17825792` bytes（17 MiB），不随写入限制改变。
  单条消息超过软预算时仍可单独返回；预算只决定一页装多少消息，不作为历史消息的拒绝条件。
- 下调 `max_payload_bytes` 后，历史消息仍按
  [Payload API](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#23-payload) 读取。
  使用自定义客户端或代理时，其传输上限也必须能够容纳这些响应，不能根据当前写入限制调小。

## 关闭流程

收到 `SIGINT`、`SIGTERM` 或发生致命存储错误时，服务端停止接收新调用，并主动结束已经建立的
Subscribe stream；普通在途调用按 API 契约完成或返回错误。
致命存储错误最终仍使进程以非零状态退出。

## 安全提示

管理端口隔离、传输保护和凭据保管要求统一见 [安全策略](../SECURITY_cn.md#部署安全提示)。

## 部署提示

- 配置文件建议放在 `/etc/openevent/openevent-server.yaml` 或部署系统管理的等价路径。
- 数据目录建议使用绝对路径，例如 `/var/lib/openevent/data`。
- 运行服务的系统用户必须能读取配置文件，并能创建和写入配置中的数据目录。
- 必须使用支持持久文件 `fsync`、目录 `fsync` 和不覆盖原子
  `renameat2(RENAME_NOREPLACE)` 的本地 Linux 文件系统。不支持 NFS 或缺少这些语义的文件系统。
- 备份和恢复只能在服务停止时进行。必须等待服务端进程完全退出，并在整个操作期间保持停服。
  存储目录必须作为一个整体备份和恢复；当前不支持在线备份，也不能只复制其中一部分。

## 文档边界

- 公开 RPC 的错误码和可观察行为见 [API 契约](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md)。
- 数据目录内部布局、恢复检查和存储损坏处理属于服务端内部设计，不在本配置文档中展开。

## 校验错误

- `config path must be provided`
- `config file does not exist: <path>`
- `config path is not a regular file: <path>`
- `grpc.listen_addr must not be empty`
- `admin.listen_addr must not be empty`
- `grpc.listen_addr and admin.listen_addr must be different`
- `storage.path must not be empty`
- `limits.max_payload_bytes must be greater than 0`
- `limits.max_payload_bytes must not exceed 62914560 (60 MiB)`
