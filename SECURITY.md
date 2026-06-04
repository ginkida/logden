# Security

## Threat model

- **Authentication is a shared token.** `LOG_TOKEN` proves "this is one of our
  logs"; authorization is NOT per-project — any token holder can write under any
  `project`. This is a deliberate trade-off for simplicity. The token is compared
  in constant time; multiple tokens are supported for rotation.
- **ClickHouse is not exposed externally.** Ports 8123/9000/9363 are reachable
  only from the docker network. Do NOT add `ports:` for them. The `default` user
  has no password but is restricted to loopback
  (`docker/clickhouse-access.xml`); only `writer` (INSERT) and `reader` (SELECT)
  are reachable from the network.
- **TLS.** The gateway listens over HTTP — the token and logs travel in
  cleartext. In production put a reverse proxy (caddy/nginx) with TLS in front of
  the gateway and bind the gateway to loopback
  (`LISTEN_ADDR=127.0.0.1:8080`). Example Caddyfile:
  ```
  logs.example.com {
      reverse_proxy 127.0.0.1:8080
      header Strict-Transport-Security "max-age=31536000"
  }
  ```
- **source_ip / X-Forwarded-For.** XFF is honored only if the connection arrives
  from an address in `TRUSTED_PROXIES` (CIDR). Without it, the real peer is
  always used. If the gateway sits behind a proxy, set its CIDR — otherwise
  source_ip can be spoofed by the client.

## Defenses

- Rate limiting (`RATE_LIMIT_RPS`) against abuse with a leaked token.
- `/metrics` without `METRICS_TOKEN` is open to everyone (the gateway logs a
  warning at startup): the build version and stream statistics leak out. If the
  gateway faces the internet, set `METRICS_TOKEN` or block the path at the
  reverse proxy.
- Body/message/context/event-count limits; gzip-bomb protection.
- `project` validation (charset + length) and a `level` whitelist — against
  cardinality DoS.
- Gateway container: `read_only`, `cap_drop: ALL`, `no-new-privileges`,
  distroless nonroot.
- Secrets can be supplied via files (`*_FILE`), not only via env.

## Secret rotation

- Token — see [RUNBOOK.md](RUNBOOK.md#rotating-the-shared-token-no-downtime).
- ClickHouse passwords: `ALTER USER writer IDENTIFIED BY '…'` + restart the
  gateway with the new `CLICKHOUSE_PASSWORD`.

## Reporting a vulnerability

Email ginkida@gmail.com. Please do not open a public issue before a fix.
