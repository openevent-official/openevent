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

shutdown:
  grace_seconds: 10
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
- 唯一 RocksDB 数据目录；metadata 和消息分别位于同一 DB 的不同 Column Family。
- 不能为空。首次部署使用不存在或为空的新目录；目录不存在时由服务端初始化，运行用户必须拥有
  对应父目录的写入权限。后续启动只接受完整的目标 schema。

### `limits`

- `max_payload_bytes`：无符号整数，默认 `16777216`（16 MiB）
- 单条消息 `payload` 的最大字节数。
- `Publish` 和 `PublishAutoSeq` 收到超过该限制的消息时返回 `RESOURCE_EXHAUSTED`。
- 两个 gRPC Server 的收发硬上限由 `max_payload_bytes + 2 MiB` 推导，并封顶为 gRPC 接受的
  最大值；Fetch 和管理消息分页使用独立的 `max_payload_bytes + 1 MiB` 应用层响应软预算。
- 必须大于 0。

### `shutdown`

- `grace_seconds`：无符号整数，默认 `10`。
- 收到 `SIGINT` 或 `SIGTERM` 后的优雅关闭窗口。两个 gRPC 服务会停止接收新请求，
  并等待在途请求完成；达到 deadline 后仍未结束的 RPC（包括流式请求）会被取消，然后关闭存储。
- 必须大于 0。

## 安全提示

- `AdminService` 请求不携带业务 `principal/token`。
- 管理端口应只监听本机或可信管理网络。
- 不要把管理端口直接暴露到公网。

## 部署提示

- 配置文件建议放在 `/etc/openevent/openevent-server.yaml` 或部署系统管理的等价路径。
- 数据目录建议使用绝对路径，例如 `/var/lib/openevent/data`。
- 运行服务的系统用户必须能读取配置文件，并能创建和写入配置中的数据目录。

## 校验错误

- `config path must be provided`
- `config file does not exist: <path>`
- `config path is not a regular file: <path>`
- `grpc.listen_addr must not be empty`
- `admin.listen_addr must not be empty`
- `grpc.listen_addr and admin.listen_addr must be different`
- `storage.path must not be empty`
- `limits.max_payload_bytes must be greater than 0`
- `shutdown.grace_seconds must be greater than 0`
