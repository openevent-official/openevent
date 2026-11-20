# OpenEvent API 说明

[English version](API.md)

OpenEvent 通过共享 protobuf 协议定义 gRPC 服务：

- 业务协议定义：
  [`openevent-sdk/proto/openevent/sdk/proto/openevent.proto`](https://github.com/openevent-official/openevent-sdk/blob/main/proto/openevent/sdk/proto/openevent.proto)
- 管理协议定义：
  [`openevent-sdk/proto/openevent/sdk/proto/admin.proto`](https://github.com/openevent-official/openevent-sdk/blob/main/proto/openevent/sdk/proto/admin.proto)
- 各接口的行为和错误码：
  [`openevent-sdk/docs/API_cn.md`](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API_cn.md)
- 全局初始化、频道初始化和成员变更的系统消息协议：
  [`openevent-sdk/docs/SYSTEM_PROTOCOL_cn.md`](https://github.com/openevent-official/openevent-sdk/blob/main/docs/SYSTEM_PROTOCOL_cn.md)

四项服务及其用途如下：

- `EventService`：状态查询、分配 UUID、查询已提交消息的序号、消息发布、批量拉取和订阅。
- `ObjectStorageService`：不可变对象写入、查询对象元数据和读取指定范围的数据。
- `ChannelService`：创建和查询频道、列出频道和管理成员。
- `AdminService`：token 管理和全部消息的管理查询。

编写客户端时，以 protobuf 协议和公开的 gRPC 状态码为准。
