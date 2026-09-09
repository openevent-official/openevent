# OpenEvent API

[中文版](API_cn.md)

OpenEvent exposes gRPC services through the shared protobuf schema:

- Business protocol definition:
  [`openevent-sdk/proto/openevent.proto`](https://github.com/openevent-official/openevent-sdk/blob/main/proto/openevent.proto)
- Admin protocol definition:
  [`openevent-sdk/proto/admin.proto`](https://github.com/openevent-official/openevent-sdk/blob/main/proto/admin.proto)
- API behavior and error semantics:
  [`openevent-sdk/docs/API.md`](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md)
- System Channel initialization-message protocol:
  [`openevent-sdk/docs/SYSTEM_PROTOCOL.md`](https://github.com/openevent-official/openevent-sdk/blob/main/docs/SYSTEM_PROTOCOL.md)

The API contract covers:

- `EventService`: status query, UUID allocation and committed-sequence lookup, message publishing, batch fetch, and
  subscription.
- `ObjectStorageService`: immutable object writes, metadata queries, and partial reads.
- `ChannelService`: channel creation, query, listing, and member management.
- `AdminService`: token management and administrative message queries.

Client applications should depend on the protobuf schema and documented gRPC
status codes, not server implementation details.
