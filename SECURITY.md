# Security Policy

[中文版](SECURITY_cn.md)

## Supported Versions

Until OpenEvent publishes stable releases, security fixes are handled on the
default branch.

## Reporting a Vulnerability

Use GitHub private vulnerability reporting if it is enabled for the public
repository. If it is not enabled yet, open a public issue without exploit
details and ask for a private reporting channel.

Please include:

- Affected version or commit.
- A clear description of the impact.
- Minimal reproduction steps or proof-of-concept details shared privately.
- Relevant deployment assumptions, especially admin port exposure and token
  handling.

## Deployment Security Notes

- Do not expose `AdminService` directly to the public internet.
- Bind the admin port to localhost or a trusted management network.
- Protect principal tokens and object tokens in logs, shell history, CI output,
  traces, and issue reports.
- An ObjectKey is a permanent transferable bearer credential. Anyone holding its
  object ID and token can read the object without principal authentication. There
  is currently no revocation, rotation, or deletion operation.
- Attaching an ObjectKey to a message reveals it to every caller allowed to read
  that message and to AdminService ListMessages. Apply Channel ACLs accordingly.
- RocksDB stores principal and object tokens in plaintext. Restrict filesystem
  access to `storage.path`, encrypt storage and backups when required, and handle
  backups as credential-bearing sensitive data.
- The reference server uses insecure gRPC credentials. Put business and admin
  endpoints behind trusted network boundaries or a TLS/mTLS proxy appropriate to
  the deployment.
