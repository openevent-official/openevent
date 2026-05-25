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
  metadata_path: "/var/lib/openevent/meta"

store:
  rocksdb:
    path: "/var/lib/openevent/messages"

limits:
  max_payload_bytes: 16777216

log:
  level: "info"
```

## Fields

### `grpc`

- `listen_addr`: string, default `0.0.0.0:9527`.
- Business gRPC listen address.
- Must not be empty.

### `admin`

- `listen_addr`: string, default `127.0.0.1:9528`.
- `AdminService` listen address.
- Must not be empty.

### `storage`

- `metadata_path`: string, no default, must be explicitly configured.
- Server metadata storage path.
- Must not be empty. The server creates the directory if it does not exist; the
  runtime user must have write permission to the parent directory.

### `store.rocksdb`

- `path`: string, no default, must be explicitly configured.
- Event data storage path.
- Must not be empty. The server creates the directory if it does not exist; the
  runtime user must have write permission to the parent directory.

### `limits`

- `max_payload_bytes`: unsigned integer, default `16777216` (16 MiB).
- Maximum size of a single message `payload`.
- `Publish` and `PublishAutoSeq` return `RESOURCE_EXHAUSTED` when the payload
  exceeds this limit.
- Must be greater than 0.

### `log`

- `level`: string, example `info`.
- Log level.

## Security Notes

- `AdminService` requests do not carry business `principal/token`.
- The admin port should only listen on localhost or a trusted management
  network.
- Do not expose the admin port directly to the public internet.

## Deployment Notes

- Place the config file under `/etc/openevent/openevent-server.yaml` or an
  equivalent deployment-managed path.
- Use absolute data paths, for example `/var/lib/openevent/meta` and
  `/var/lib/openevent/messages`.
- The service user must be able to read the config file and create/write the
  configured data directories.

## Validation Errors

- `config path must be provided`
- `config file does not exist: <path>`
- `config path is not a regular file: <path>`
- `grpc.listen_addr must not be empty`
- `admin.listen_addr must not be empty`
- `storage.metadata_path must not be empty`
- `store.rocksdb.path must not be empty`
- `limits.max_payload_bytes must be greater than 0`
