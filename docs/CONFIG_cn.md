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
  metadata_path: "/var/lib/openevent/meta"

store:
  rocksdb:
    path: "/var/lib/openevent/messages"

limits:
  max_payload_bytes: 16777216

log:
  level: "info"
```

## 字段

### `grpc`

- `listen_addr`：字符串，默认 `0.0.0.0:9527`
- 业务 gRPC 接口监听地址。
- 不能为空。

### `admin`

- `listen_addr`：字符串，默认 `127.0.0.1:9528`
- `AdminService` 监听地址。
- 不能为空。

### `storage`

- `metadata_path`：字符串，无默认值，必须通过配置显式提供。
- 服务端元数据存储路径。
- 不能为空；目录不存在时由服务端创建，运行用户必须拥有对应父目录的写入权限。

### `store.rocksdb`

- `path`：字符串，无默认值，必须通过配置显式提供。
- 事件数据存储路径。
- 不能为空；目录不存在时由服务端创建，运行用户必须拥有对应父目录的写入权限。

### `limits`

- `max_payload_bytes`：无符号整数，默认 `16777216`（16 MiB）
- 单条消息 `payload` 的最大字节数。
- `Publish` 和 `PublishAutoSeq` 收到超过该限制的消息时返回 `RESOURCE_EXHAUSTED`。
- 必须大于 0。

### `log`

- `level`：字符串，示例值 `info`
- 日志级别。

## 安全提示

- `AdminService` 请求不携带业务 `principal/token`。
- 管理端口应只监听本机或可信管理网络。
- 不要把管理端口直接暴露到公网。

## 部署提示

- 配置文件建议放在 `/etc/openevent/openevent-server.yaml` 或部署系统管理的等价路径。
- 数据目录建议使用绝对路径，例如 `/var/lib/openevent/meta` 和 `/var/lib/openevent/messages`。
- 运行服务的系统用户必须能读取配置文件，并能创建和写入配置中的数据目录。

## 校验错误

- `config path must be provided`
- `config file does not exist: <path>`
- `config path is not a regular file: <path>`
- `grpc.listen_addr must not be empty`
- `admin.listen_addr must not be empty`
- `storage.metadata_path must not be empty`
- `store.rocksdb.path must not be empty`
- `limits.max_payload_bytes must be greater than 0`
