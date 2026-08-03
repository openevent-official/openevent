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

shutdown:
  grace_seconds: 10
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
- The OpenEvent data root. It has a fixed layout: `path/db` contains one RocksDB
  instance with `default` (metadata), `messages`, and `objects` Column Families;
  `path/objects` contains one immutable file per committed object, named by its
  decimal object ID.
- Must not be empty. A first deployment uses a missing or empty new directory;
  the server initializes a missing directory, and the runtime user must have
  write permission to its parent. Later starts accept only the complete target
  schema.
- Object data is limited to 4 MiB per object and is not stored in RocksDB. Objects
  are never updated, deleted, or garbage-collected by the current server, so each
  object permanently consumes data space and one inode.

### `limits`

- `max_payload_bytes`: unsigned integer, default `16777216` (16 MiB).
- Maximum size of a single message `payload`.
- `Publish` and `PublishAutoSeq` return `RESOURCE_EXHAUSTED` when the payload
  exceeds this limit.
- Both gRPC servers derive their send/receive hard limit from
  `max(max_payload_bytes, 4 MiB) + 2 MiB`, capped at the largest value accepted by gRPC.
  Fetch and administrative message pages use a separate soft response budget of
  `max_payload_bytes + 1 MiB`.
- Must be greater than 0.

### `shutdown`

- `grace_seconds`: unsigned integer, default `10`.
- Grace period used after `SIGINT` or `SIGTERM`. Both gRPC servers stop accepting
  new calls and drain in-flight calls until this deadline; any RPC that remains
  active, including streaming calls, is then cancelled before storage is closed.
- Must be greater than 0.

## Security Notes

- `AdminService` requests do not carry business `principal/token`.
- The admin port should only listen on localhost or a trusted management
  network.
- Do not expose the admin port directly to the public internet.

## Deployment Notes

- Place the config file under `/etc/openevent/openevent-server.yaml` or an
  equivalent deployment-managed path.
- Use an absolute data path, for example `/var/lib/openevent/data`.
- The service user must be able to read the config file and create/write the
  configured data directories.
- Use a local Linux filesystem that supports durable file `fsync`, directory
  `fsync`, and non-overwriting atomic `renameat2(RENAME_NOREPLACE)`. NFS and
  filesystems without those semantics are unsupported.
- Monitor both free bytes and free inodes under `storage.path`. The server has no
  object deletion or background garbage collection.
- Back up `db/` and `objects/` as one consistent unit using a stopped service or a
  filesystem/storage snapshot with equivalent consistency. Copying either child
  independently can produce committed metadata without matching object data.
- Startup only recovers known incomplete object writes; it does not scan every
  committed object file. Missing or corrupted committed data is detected when
  ReadObject accesses it, returns `DATA_LOSS`, and causes a nonzero server exit.

## Validation Errors

- `config path must be provided`
- `config file does not exist: <path>`
- `config path is not a regular file: <path>`
- `grpc.listen_addr must not be empty`
- `admin.listen_addr must not be empty`
- `grpc.listen_addr and admin.listen_addr must be different`
- `storage.path must not be empty`
- `limits.max_payload_bytes must be greater than 0`
- `shutdown.grace_seconds must be greater than 0`
