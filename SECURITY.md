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
  source_ip can be spoofed by the client. The chain is read **right to left**:
  the first entry that is not itself a trusted proxy wins, because the leftmost
  entry is whatever the client typed and every appending proxy forwards it
  verbatim. So list *every* hop in front of the gateway — with an intermediate
  missing from `TRUSTED_PROXIES`, source_ip records that hop rather than the
  client. When the whole chain is trusted the leftmost entry is kept (an internal
  sender behind the same proxy, which the peer would collapse onto the proxy
  address). An entry that does not parse fails closed onto the real peer. Only the
  last 16 entries are examined, so a padded chain resolves to a near hop instead
  of turning every request into thousands of parse attempts. Repeated
  `X-Forwarded-For` header *lines* are read as one chain, so a client that sends
  its own line cannot get ahead of the proxy's.

## Defenses

- **Rate limiting (`RATE_LIMIT_RPS`) caps total ingest; it does not isolate a
  sender.** It is ONE global token bucket for the whole gateway, and it runs
  *after* authentication — so it bounds what a leaked token can cost the box
  (ClickHouse write load, disk), but a flood on a stolen token also rate-limits
  every legitimate sender, since they share the bucket. There is no per-token,
  per-project or per-IP limiting. Against a leaked token the actual remedy is
  rotation (below); the limiter is a blast-radius cap, not an authorization
  control. `RATE_BURST` is the bucket ceiling, floored at 1 token.
- **Admission control** (`MAX_INFLIGHT_BODY_BYTES`) bounds the request bodies
  being processed concurrently, charged as they are read and measured on the
  *decompressed* stream, so a burst of large or gzip-bombed uploads degrades to
  `503` instead of an OOM kill.
- `/metrics` without `METRICS_TOKEN` is open to everyone (the gateway logs a
  warning at startup): the build version, stream statistics and the per-project
  counters (i.e. your project names and their volumes) leak out. If the
  gateway faces the internet, set `METRICS_TOKEN` or block the path at the
  reverse proxy.
- Body/message/context/event-count limits; gzip-bomb protection.
- `project` validation (charset + length) and a `level` whitelist — against
  cardinality DoS. That validation is also what makes `project` safe as a metric
  label: the per-project counters do not escape label values, and the label set
  is capped at 64 with an `<overflow>` bucket a sender cannot name.
- **Error bodies reflect nothing.** A failed `/logs` answers
  `{"error":"<reason>"}` where the reason is a compile-time literal from a closed
  vocabulary — no part of the request is echoed back.
- Gateway container: `read_only`, `cap_drop: ALL`, `no-new-privileges`,
  distroless nonroot.
- Secrets can be supplied via files (`*_FILE`), not only via env:
  `LOG_TOKEN_FILE`, `CLICKHOUSE_PASSWORD_FILE`, `METRICS_TOKEN_FILE` for the
  gateway, `CH_WRITER_PASSWORD_FILE` / `CH_READER_PASSWORD_FILE` for the
  ClickHouse init script. The file wins over the plain variable and its contents
  are trimmed.
- **The env files hold every secret in this stack — keep them 0600.**
  `chmod 600 .env` after `cp .env.example .env`, and install the bare-metal one
  as `install -m 600 -o root -g root deploy/logden.env.example /etc/logden.env`
  (systemd reads it as root before dropping to the DynamicUser, so 0600 costs
  nothing). With the shipped compose the same values are also visible through
  `docker inspect`, `docker compose config`, the container config JSON under
  `/var/lib/docker/containers` and `/proc/<pid>/environ` — on a single-VPS
  deployment that is the same root boundary that already reads `.env`, but treat
  that output as secret-bearing and never paste it into an issue or hand it to
  an agent. `docker-compose.yml` documents the compose-`secrets:` opt-out.

## Secret rotation

- Token — see [RUNBOOK.md](RUNBOOK.md#rotating-the-shared-token-no-downtime).
  Several comma-separated tokens are valid at once, which is what makes the
  three-step rotation zero-downtime.
- ClickHouse passwords — see
  [RUNBOOK.md](RUNBOOK.md#rotating-the-clickhouse-passwords). Both init paths use
  `CREATE USER OR REPLACE`, so re-running them *is* the rotation. An in-place
  `ALTER USER writer IDENTIFIED BY '…'` still works, but then `.env`'s
  `CH_WRITER_PASSWORD` must be updated in the same step — it is what the gateway
  presents *and* what a rebuilt `ch-data` volume would recreate the account with,
  so the two silently drift otherwise. Either way the gateway needs the new
  `CLICKHOUSE_PASSWORD` and a restart.
- The tracked `clickhouse/users.sql` deliberately contains no usable credential:
  its two `sha256_hash` sentinels are not 64 hex characters, so ClickHouse
  refuses the file if it is ever run unedited. Keep real values in a gitignored
  `clickhouse/users.local.sql` or substitute them at run time (README,
  "Installation without Docker").

## Reporting a vulnerability

Email ginkida@gmail.com. Please do not open a public issue before a fix.
