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

TCP addresses on both interfaces must specify a fixed port, for example `localhost:9527`,
`127.0.0.1:9527`, or `[::1]:9527`. Use a decimal port from `1` to `65535`, or a service
name that resolves to a fixed, nonzero TCP port. The `dns:///` prefix is supported.
Each field takes one address. Write TCP addresses literally, without URI escapes.
A zero or missing port causes a configuration error before the server opens the data
directory. The `unix:` and `unix-abstract:` socket address forms do not require a port.

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
- Relative paths are resolved against the server process's working directory at
  startup. For example, when started from `/opt/openevent`, `data` resolves to
  `/opt/openevent/data`. The server prints the absolute path before opening it.
- Use a nonexistent or empty data directory for the first deployment. If you confirm that initialization
  did not finish, stop the server, delete that directory, and restart the server to recreate and initialize it.
  Once initialization completes, reuse the same data directory on restart.
- Messages and objects are stored permanently under the
  [API retention policy](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md#1-basic-conventions).
  Deployments are responsible for capacity planning and monitoring.

### `limits`

- `max_payload_bytes`: unsigned integer, default `16777216` (16 MiB).
- Limits payload size for each new message written by `Publish` or `PublishAutoSeq`.
  For error precedence when retrying with the UUID of a committed message, see the
  [API rules for message UUIDs](../openevent-sdk/docs/API.md#24-message-uuids).
- Valid range: `1..62914560` bytes (60 MiB). Invalid configuration prevents startup.

## Transport and Page Budgets

- Both gRPC servers use a fixed per-message send/receive hard limit of
  `67108864` bytes (64 MiB), independent of `max_payload_bytes`.
- Fetch and ListMessages sum the serialized sizes of the `EventMessage` records
  returned on each page against a fixed soft budget of `17825792` bytes (17 MiB),
  independent of the write limit. The budget excludes each record's outer field
  tag and length prefix and the response fields `next_seq` and `last_seq`.
  A single message larger than the budget is returned on its own. When the page
  reaches or exceeds the budget, it ends immediately with `next_seq` set to the
  last returned message's sequence number plus one. The gRPC hard limit still
  applies to the complete response.
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

- The recommended config file location is `/etc/openevent/openevent-server.yaml` or an
  equivalent deployment-managed path.
- An absolute data path is recommended, for example `/var/lib/openevent/data`.
- The service user needs permission to read the config file and create/write the
  data directory, including parent-directory write access when creating it.
- OpenEvent requires a local Linux filesystem that supports exclusive file creation
  (`O_CREAT|O_EXCL`), file `fsync`, and directory `fsync`.
- Before backup or restore, wait for the server process to exit completely.
  Keep it stopped throughout the operation and handle the entire data directory as one unit.
  After restoring an older backup, close and recreate any live Python `OpenEventClient`
  instances before resuming traffic to discard their preallocated UUIDs.
  For cursors, retry state, and resource references, follow the
  [API's data history boundary](https://github.com/openevent-official/openevent-sdk/blob/main/docs/API.md#12-data-history-and-backup-restoration).
