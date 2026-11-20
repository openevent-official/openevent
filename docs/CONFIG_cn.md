# OpenEvent 配置说明

[English version](CONFIG.md)

启动时传入 YAML 配置文件路径；该路径必须指向已有的普通文件。

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

业务和管理接口的 TCP 地址都必须写明固定端口，例如 `localhost:9527`、`127.0.0.1:9527` 或
`[::1]:9527`。端口号用十进制表示，范围是 `1` 到 `65535`，也可以填写对应固定非零 TCP 端口的服务名。
支持 `dns:///` 前缀。每个字段只能写一个地址。请直接填写 TCP 地址，不要使用 URI 转义。
端口填 `0` 或省略端口时，服务端报配置错误并在打开数据目录前退出。
`unix:` 和 `unix-abstract:` 套接字地址不需要端口。

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
- OpenEvent 数据根目录，用于保存服务端管理的消息、频道、token 和对象数据。
- 相对路径以服务进程启动时的工作目录为基准。例如，从 `/opt/openevent` 启动时，`data` 表示
  `/opt/openevent/data`。服务端在打开数据目录前打印所用的绝对路径。
- 首次部署使用不存在或为空的数据目录。若确认首次初始化未完成，停服后删除该数据目录，再启动服务，由服务端重建目录并重新初始化。
  初始化完成后，重启时继续使用原数据目录。
- 消息和对象按 [API 保留策略](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#1-基础约定)
  永久保存，部署方负责容量规划与监控。

### `limits`

- `max_payload_bytes`：无符号整数，默认 `16777216`（16 MiB）
- 设置 `Publish` 和 `PublishAutoSeq` 发布新消息时的 `payload` 字节上限。使用已提交消息的 UUID
  重试时，错误优先级见 [API 的消息 UUID 规则](../openevent-sdk/docs/API_cn.md#24-消息-uuid)。
- 有效范围为 `1..62914560` 字节（60 MiB）；配置非法时拒绝启动。

## 传输和分页预算

- 业务和管理两个 gRPC 服务收发的单条消息上限均为 `67108864` 字节（64 MiB），不随 `max_payload_bytes` 改变。
- Fetch 和 ListMessages 以 `17825792` 字节（17 MiB）为每页的大小目标。只计算本页各条 `EventMessage` 序列化后的大小之和，
  不计外层 `messages` 字段的标签、长度前缀和分页字段 `next_seq`、`last_seq`。调整写入限制不会改变这个目标。
  单条消息大于 17 MiB 时单独返回。页内消息总大小达到或超过 17 MiB 时，本页立即结束，
  `next_seq` 等于最后一条消息的序号加一。完整响应仍须在 gRPC 的 64 MiB 上限内。
- 下调 `max_payload_bytes` 后，历史消息仍按
  [Payload API](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#23-payload) 读取。
  自定义客户端或代理的传输上限应能容纳这些历史消息响应。

## 关闭流程

收到 `SIGINT`、`SIGTERM` 或发生致命存储错误时，服务端停止接收新调用，并主动结束已经建立的订阅。
已经开始的普通调用按各自接口的规则完成或返回错误。
致命存储错误最终仍使进程以非零状态退出。

## 安全提示

管理端口隔离、传输保护和凭据保管要求统一见 [安全策略](../SECURITY_cn.md#部署安全提示)。

## 部署提示

- 配置文件建议放在 `/etc/openevent/openevent-server.yaml` 或部署系统管理的等价路径。
- 数据目录建议使用绝对路径，例如 `/var/lib/openevent/data`。
- 运行用户需要读取配置文件、创建和写入数据目录的权限；创建目录时还需要父目录写权限。
- 必须使用支持排他创建文件（`O_CREAT|O_EXCL`）、文件 `fsync` 和目录 `fsync` 的本地 Linux 文件系统。
- 备份或恢复前等待服务进程完全退出，全程保持停服，并将整个数据目录作为一个整体处理。
  恢复旧备份后，在恢复业务流量前关闭并重建仍在运行的 Python `OpenEventClient`，丢弃客户端预领的 UUID。
  游标、重试状态和资源引用按 [API 的数据历史边界](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md#12-数据历史与备份恢复) 处理。
