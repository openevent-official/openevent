# OpenEvent Configuration

[中文版](CONFIG_cn.md)

Pass a YAML configuration path at startup; it must name an existing regular file.

```bash
build/openevent_server /path/to/openevent-server.yaml
```

## Example

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

## Fields

### `grpc`

- `listen_addr`: string, default `0.0.0.0:9527`.
- Business gRPC listen address.
- Must not be empty.

### `admin`

- `listen_addr`: string, default `127.0.0.1:9528`.
- `AdminService` listen address.
- Must not be empty and must differ from `grpc.listen_addr`.

### `storage`

- `path`: required nonempty string, no default.
- The OpenEvent data root managed by the server for messages, Channels, tokens,
  and object data.
- Use a missing or empty new directory for the first deployment, and reuse the
  complete data directory on subsequent starts.
- Messages and objects are stored permanently under the
  [API retention policy](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md#1-basic-conventions).
  Deployments are responsible for capacity planning and monitoring.

### `limits`

- `max_payload_bytes`: unsigned integer, default `16777216` (16 MiB).
- Controls payload bytes per message for new `Publish` and `PublishAutoSeq` writes.
- `Publish` and `PublishAutoSeq` return `RESOURCE_EXHAUSTED` when the payload
  exceeds this limit.
- Valid range: `1..62914560` bytes (60 MiB). Invalid configuration prevents startup.

## Transport and Page Budgets

- Both gRPC servers use a fixed per-message send/receive hard limit of
  `67108864` bytes (64 MiB), independent of `max_payload_bytes`.
- Fetch and ListMessages use a fixed soft response budget of `17825792` bytes
  (17 MiB) per page, independent of the write limit. A single message larger than
  the soft budget is returned on its own.
- After lowering `max_payload_bytes`, historical messages remain readable under
  the [Payload API](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md#23-payload).
  Custom clients and proxies need transport limits that accommodate these
  historical message responses.

## Shutdown Behavior

On `SIGINT`, `SIGTERM`, or a fatal storage error, the server stops accepting new
calls and actively ends established Subscribe streams. Ordinary in-flight calls
complete or return an error according to the API contract. A fatal storage error
still causes a non-zero final process exit status.

## Security Notes

Admin port isolation, transport protection, and credential handling requirements
are defined in the [security policy](../SECURITY.md#deployment-security-notes).

## Deployment Notes

- Place the config file under `/etc/openevent/openevent-server.yaml` or an
  equivalent deployment-managed path.
- Use an absolute data path, for example `/var/lib/openevent/data`.
- The service user needs permission to read the config file and create/write the
  data directory, including parent-directory write access when creating it.
- Use a local Linux filesystem that supports durable file `fsync`, directory
  `fsync`, and non-overwriting atomic `renameat2(RENAME_NOREPLACE)`.
- Before backup or restore, wait for the server process to exit completely.
  Keep it stopped throughout the operation and handle the entire data directory as one unit.
