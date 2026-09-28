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

- `AdminService` does not authenticate callers. Bind its port to localhost or a trusted management network.
- Keep principal tokens and ObjectKeys private, including in logs and public output.
- An ObjectKey is a transferable, permanent read credential: anyone holding it can read the object.
  Attaching it to a message shares it with that message's readers and the admin endpoint.
- The data directory and backups contain plaintext credentials. Restrict file access and encrypt as needed.
- The server uses plaintext gRPC. Run it within a trusted network or behind a TLS/mTLS proxy.
