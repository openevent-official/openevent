# OpenEvent 配置说明

[English version](CONFIG.md)

启动时传入 YAML 配置文件路径；该路径须指向已有的普通文件。

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

- `path`：必填的非空字符串，无默认值。
- OpenEvent 数据根目录，由服务端管理消息、Channel、token 和对象数据。
- 首次部署使用不存在或为空的新目录，后续启动复用完整的数据目录。
- 消息和对象按 [API 保留策略](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#1-基础约定)
  永久保存，部署方负责容量规划与监控。

### `limits`

- `max_payload_bytes`：无符号整数，默认 `16777216`（16 MiB）
- 控制 `Publish` 和 `PublishAutoSeq` 新写入的单条消息 `payload` 字节数。
- `Publish` 和 `PublishAutoSeq` 收到超过该限制的消息时返回 `RESOURCE_EXHAUSTED`。
- 有效范围为 `1..62914560` bytes（60 MiB）；配置非法时拒绝启动。

## 传输和分页预算

- 两个 gRPC Server 的单条收发消息硬上限固定为 `67108864` bytes（64 MiB），不随 `max_payload_bytes` 改变。
- Fetch 和 ListMessages 的每页响应软预算固定为 `17825792` bytes（17 MiB），不随写入限制改变。
  单条消息超过软预算时单独返回。
- 下调 `max_payload_bytes` 后，历史消息仍按
  [Payload API](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#23-payload) 读取。
  自定义客户端或代理的传输上限应能容纳这些历史消息响应。

## 关闭流程

收到 `SIGINT`、`SIGTERM` 或发生致命存储错误时，服务端停止接收新调用，并主动结束已经建立的
Subscribe stream；普通在途调用按 API 契约完成或返回错误。
致命存储错误最终仍使进程以非零状态退出。

## 安全提示

管理端口隔离、传输保护和凭据保管要求统一见 [安全策略](../SECURITY_cn.md#部署安全提示)。

## 部署提示

- 配置文件建议放在 `/etc/openevent/openevent-server.yaml` 或部署系统管理的等价路径。
- 数据目录建议使用绝对路径，例如 `/var/lib/openevent/data`。
- 运行用户需要读取配置文件、创建和写入数据目录的权限；创建目录时还需要父目录写权限。
- 必须使用支持持久文件 `fsync`、目录 `fsync` 和不覆盖原子
  `renameat2(RENAME_NOREPLACE)` 的本地 Linux 文件系统。
- 备份或恢复前等待服务进程完全退出，全程保持停服，并将整个数据目录作为一个整体处理。
