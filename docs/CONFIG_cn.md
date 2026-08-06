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
- OpenEvent 数据根目录，固定布局为：`path/db` 保存一个 RocksDB 实例，其中包含 `default`
  （metadata）、`messages` 和 `objects` 三个 Column Family；`path/objects` 为每个 committed 对象
  保存一个不可变文件，文件名是十进制 object ID。
- 不能为空。首次部署使用不存在或为空的新目录；目录不存在时由服务端初始化，运行用户必须拥有
  对应父目录的写入权限。后续启动只接受完整的目标 schema。初始化中断或非空目录布局不完整时，
  服务端拒绝启动，不在线修复。
- 对象 data 不进入 RocksDB，每个对象最大 4 MiB。当前服务端不更新、删除或回收对象，因此每个
  对象永久占用 data 空间和一个 inode。

### `limits`

- `max_payload_bytes`：无符号整数，默认 `16777216`（16 MiB）
- 单条消息 `payload` 的最大字节数。
- `Publish` 和 `PublishAutoSeq` 收到超过该限制的消息时返回 `RESOURCE_EXHAUSTED`。
- 两个 gRPC Server 的收发硬上限由 `max(max_payload_bytes, 4 MiB) + 2 MiB` 推导，并封顶为
  gRPC 接受的最大值；Fetch 和管理消息分页使用独立的 `max_payload_bytes + 1 MiB` 应用层响应软预算。
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
- 必须使用支持持久文件 `fsync`、目录 `fsync` 和不覆盖原子
  `renameat2(RENAME_NOREPLACE)` 的本地 Linux 文件系统。不支持 NFS 或缺少这些语义的文件系统。
- 同时监控 `storage.path` 下的可用字节数和 inode；服务端没有对象删除或后台垃圾回收。
- 必须在服务停止时，或使用具有同等一致性的文件系统/存储快照，把 `db/` 和 `objects/` 作为一个
  一致单元备份。单独复制任一子目录可能得到缺少对应对象 data 的 committed metadata。
- 启动不扫描历史消息，也不比较消息水位和已存消息记录。对象恢复只扫描已知的未完成写入，
  不扫描 committed 对象 metadata、文件或完整对象目录。ReadObject 访问时才会发现 committed
  对象 data 缺失、不是普通文件或与 metadata 大小不一致，返回 `DATA_LOSS`，并使服务端以非零状态
  退出；同大小的内容变更不会被发现。
- 服务期间任何 RocksDB 操作失败都会使服务端以非零状态退出。RocksDB 损坏返回 `DATA_LOSS`，其他
  RocksDB 错误返回 `UNAVAILABLE`。正常查询不存在的 token、Channel 或 object 不属于 RocksDB 故障。
  启动期间任何 RocksDB 操作失败都会拒绝启动。
- 已读出的持久化 bytes 不符合内部记录格式或必要记录关系时按存储损坏处理；遇到该数据的请求返回
  `DATA_LOSS`，并使服务端以非零状态退出。启动仍不全量扫描历史消息或 committed 对象。

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
