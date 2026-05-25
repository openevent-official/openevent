# OpenEvent API

[中文版](API_cn.md)

OpenEvent exposes gRPC services through the shared protobuf schema:

- Protocol definition:
  [`openevent-sdk/proto/openevent.proto`](https://github.com/openevent2026/openevent-sdk/blob/main/proto/openevent.proto)
- API behavior and error semantics:
  [`openevent-sdk/docs/API.md`](https://github.com/openevent2026/openevent-sdk/blob/main/docs/API.md)

The API contract covers:

- `EventService`: status query, message publishing, batch fetch, and subscription.
- `ChannelService`: channel creation, query, listing, and member management.
- `AdminService`: token management.

Client applications should depend on the protobuf schema and documented gRPC
status codes, not server implementation details.
