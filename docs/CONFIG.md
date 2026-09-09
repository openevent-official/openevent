# OpenEvent Configuration

[中文版](CONFIG_cn.md)

The server must be started with a valid YAML configuration file path. The server
rejects startup if the path is missing, does not exist, or is not a regular file.

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

- `path`: string, no default, must be explicitly configured.
- The OpenEvent data root managed by the server for messages, Channels, tokens,
  and object data.
- Must not be empty. A first deployment uses a missing or empty new directory;
  the server initializes a missing directory, writes the permanent `system.v1`
  message at seq 0, and the runtime user must have write permission to its
  parent. Later starts accept only a complete target data directory; unfinished
  initialization or an incomplete directory is rejected rather than repaired.
  A directory whose initialization is complete undergoes normal startup checks.
- Messages and objects are stored permanently under the
  [API retention policy](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md#1-basic-conventions).
  Capacity planning, alert thresholds, and operational shutdown procedures are
  deployment choices; the server does not provide these management functions.

### `limits`

- `max_payload_bytes`: unsigned integer, default `16777216` (16 MiB).
- Limits only the payload bytes of new `Publish` and `PublishAutoSeq` writes; it
  does not limit reads of committed messages.
- `Publish` and `PublishAutoSeq` return `RESOURCE_EXHAUSTED` when the payload
  exceeds this limit.
- Must be greater than 0 and no greater than `62914560` bytes (60 MiB). If the
  configured value exceeds this limit, configuration validation fails and the
  server exits before opening either listen port.

## Transport and Page Budgets

- Both gRPC servers use a fixed per-message send/receive hard limit of
  `67108864` bytes (64 MiB), independent of `max_payload_bytes`.
- Fetch and ListMessages use a fixed soft response budget of `17825792` bytes
  (17 MiB) per page, independent of the write limit. A single message larger than
  the soft budget can still be returned on its own; the budget determines page
  boundaries and does not reject historical messages.
- After lowering `max_payload_bytes`, historical messages remain readable under
  the [Payload API](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md#23-payload).
  Custom clients and proxies must allow these responses through their transport
  limits, which must not be reduced to match the current write limit.

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
- The service user must be able to read the config file and create/write the
  configured data directories.
- Use a local Linux filesystem that supports durable file `fsync`, directory
  `fsync`, and non-overwriting atomic `renameat2(RENAME_NOREPLACE)`. NFS and
  filesystems without those semantics are unsupported.
- Back up and restore only while the server is stopped. Wait for the server
  process to exit completely and keep it stopped throughout the operation.
  Back up and restore the entire storage directory as one unit. Online backups
  and partial directory copies are unsupported.

## Documentation Boundary

- Public RPC error codes and observable behavior are defined in the [API contract](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md).
- Internal directory layout, recovery checks, and storage-corruption handling are
  server design details and are not expanded in this configuration document.

## Validation Errors

- `config path must be provided`
- `config file does not exist: <path>`
- `config path is not a regular file: <path>`
- `grpc.listen_addr must not be empty`
- `admin.listen_addr must not be empty`
- `grpc.listen_addr and admin.listen_addr must be different`
- `storage.path must not be empty`
- `limits.max_payload_bytes must be greater than 0`
- `limits.max_payload_bytes must not exceed 62914560 (60 MiB)`
