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
- Protect token material in logs, shell history, CI output, and issue reports.
