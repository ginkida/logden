# Agent access to logs (SQL / MCP)

Analysis is plain SQL under the read-only `reader` user (`SELECT` only,
a profile with memory/time limits + readonly mode, access to the needed system tables).
The agent can neither write nor bypass the limits (see [Limits](#limits-the-reader-runs-under)).

## Where ClickHouse lives

By default the ClickHouse ports are NOT published externally (docker network only). To
let an agent/MCP server reach it, there are two paths.

### A. Expose HTTP on loopback only (local agent)

Copy `docker-compose.override.yml.example` → `docker-compose.override.yml`
(picked up automatically). It binds 8123 to `127.0.0.1` only — externally the
port stays closed:

```yaml
services:
  clickhouse:
    ports:
      - "127.0.0.1:8123:8123"
```

### B. MCP server on the same docker network

Run ClickHouse-MCP as a service in compose — it talks to `clickhouse:8123` over
the internal network, so there's no need to publish the CH port externally.

## ClickHouse MCP server

The official server is `mcp-clickhouse`. The config points it at `reader`:

```json
{
  "mcpServers": {
    "logs": {
      "command": "uvx",
      "args": ["mcp-clickhouse"],
      "env": {
        "CLICKHOUSE_HOST": "127.0.0.1",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_SECURE": "false",
        "CLICKHOUSE_USER": "reader",
        "CLICKHOUSE_PASSWORD": "<paste CH_READER_PASSWORD from .env>",
        "CLICKHOUSE_DATABASE": "logs"
      }
    }
  }
}
```

Two things this JSON will not do for you:

- **`CLICKHOUSE_SECURE` must be set to `"false"` explicitly.** `mcp-clickhouse`
  defaults it to `true` and then speaks TLS to a plain-HTTP port: 8123 is the
  cleartext port (8443 is the TLS one), so leaving the variable out gives a
  connection error at startup, not a working client. Flip it back to `"true"`
  only if you put a TLS proxy in front of ClickHouse and point the config at it.
- **It is a config file, not a shell** — nothing here is expanded. Paste the
  literal value of `CH_READER_PASSWORD` from your `.env`; `$CH_READER_PASSWORD`
  written into the JSON is sent to ClickHouse verbatim, as that literal string,
  and authentication fails.

The host above is for path A. On path B (MCP inside compose) the host is the
service name instead: `"CLICKHOUSE_HOST": "clickhouse"`, same port.

From there the agent sees `logs.logs` and the system tables and runs the SQL from
`clickhouse/queries.sql` (project inventory, spike and silence detectors, top
errors, level breakdown over time, search by `message`/`context`).

## Direct SQL without MCP

The reader password lives in `.env` and is not exported into your shell by
anything else, so load it first:

```bash
set -a; . ./.env; set +a   # note: exports every variable from .env into this shell
curl -s 'http://127.0.0.1:8123/' \
  -H "X-ClickHouse-User: reader" -H "X-ClickHouse-Key: $CH_READER_PASSWORD" \
  --data-binary "SELECT project, count() FROM logs.logs
                 WHERE timestamp > now()-INTERVAL 1 HOUR GROUP BY project FORMAT JSON"
```

This needs path A (8123 bound to loopback). Without it, run the same SQL inside
the container instead — no port and no `.env` sourcing required:

```bash
docker compose exec clickhouse clickhouse-client \
  --user reader --password "$(grep -E '^CH_READER_PASSWORD=' .env | cut -d= -f2-)" \
  -q "SELECT project, count() FROM logs.logs
      WHERE timestamp > now()-INTERVAL 1 HOUR GROUP BY project"
```

## Limits the reader runs under

From the `readonly` profile in `clickhouse/users.d/low-mem.xml`. Readonly mode
means the reader cannot raise any of them per query — an agent that "fixes" a
limit error with `SETTINGS max_execution_time=300` gets a permission error:

| Setting | Value | What hitting it looks like |
| --- | --- | --- |
| `max_execution_time` | 30 s | `TIMEOUT_EXCEEDED` — narrow the time window |
| `max_memory_usage` | 300 MB | `MEMORY_LIMIT_EXCEEDED` — usually a `GROUP BY` over a high-cardinality column |
| `max_threads` | 2 | nothing; queries are just slower (1-vCPU host) |
| `max_result_rows` | 200000 | `TOO_MANY_ROWS` — add a `LIMIT` or aggregate |

`result_overflow_mode` is `throw`, deliberately: the alternative (`break`) stops
an over-cap query and returns the partial result **with no error at all**, which
hands an agent a wrong aggregate that looks complete. A loud failure costs one
retry; a silent truncation costs a wrong conclusion. So a query either answers
fully or fails — a result of exactly 200000 rows is a real result, not a cut-off
one.

Practical consequence for agent prompts: bound every query by `timestamp` (the
table is partitioned by day, so a time filter is what keeps a query inside the
30 s budget), prefer aggregates over row dumps, and treat any error above as
"ask a narrower question", not as "the database is broken".

The reader's reach is `logs.logs` plus exactly six system tables —
`system.parts`, `system.metrics`, `system.events`, `system.asynchronous_metrics`,
`system.asynchronous_insert_log`, `system.errors` (the `GRANT`s in
`clickhouse/users.sql`). Anything else, including `system.text_log` and
`system.query_log`, answers with a permission error rather than an empty result:
that is a missing grant, not a missing table. Reach them with `default` from
inside the container (`docker compose exec`, see RUNBOOK) if you actually need
them.

## Security

- `reader` — `SELECT` only + readonly mode (cannot change settings/bypass limits).
- Do NOT publish 8123 to a public network — use loopback binding (A) or the internal network (B).
- For a remote agent, put a TLS proxy in front of ClickHouse (and only then set
  `CLICKHOUSE_SECURE` back to `"true"`).
