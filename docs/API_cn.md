# OpenEvent API 说明

[English version](API.md)

OpenEvent 通过共享 protobuf 协议定义 gRPC 服务：

- 业务协议定义：
  [`openevent-sdk/proto/openevent.proto`](https://github.com/openevent-official/openevent-sdk/blob/main/proto/openevent.proto)
- 管理协议定义：
  [`openevent-sdk/proto/admin.proto`](https://github.com/openevent-official/openevent-sdk/blob/main/proto/admin.proto)
- API 行为和错误语义：
  [`openevent-sdk/docs/API_cn.md`](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md)

API 契约包括：

- `EventService`：状态查询、UUID 分配和已提交 seq 查询、消息发布、批量拉取和订阅。
- `ObjectStorageService`：不可变对象写入、metadata 查询和部分读取。
- `ChannelService`：Channel 创建、查询、列表和成员管理。
- `AdminService`：token 管理和全部消息的管理查询。

客户端应用应依赖 protobuf 协议和文档化的 gRPC 状态码，不应依赖服务端实现细节。
