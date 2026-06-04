# Agent access to logs (SQL / MCP)

Analysis is plain SQL under the read-only `reader` user (`SELECT` only,
a profile with memory/time limits + readonly mode, access to the needed system tables).
The agent can neither write nor bypass the limits.

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
        "CLICKHOUSE_USER": "reader",
        "CLICKHOUSE_PASSWORD": "<CH_READER_PASSWORD>",
        "CLICKHOUSE_DATABASE": "logs"
      }
    }
  }
}
```

From there the agent sees `logs.logs` and the system tables and runs the SQL from
`clickhouse/queries.sql` (top errors, level breakdown over time, search by `message`/`context`).

## Direct SQL without MCP

```bash
curl -s 'http://127.0.0.1:8123/' \
  -H "X-ClickHouse-User: reader" -H "X-ClickHouse-Key: $CH_READER_PASSWORD" \
  --data-binary "SELECT project, count() FROM logs.logs
                 WHERE timestamp > now()-INTERVAL 1 HOUR GROUP BY project FORMAT JSON"
```

## Security

- `reader` — `SELECT` only + readonly mode (cannot change settings/bypass limits).
- Do NOT publish 8123 to a public network — use loopback binding (A) or the internal network (B).
- For a remote agent, put a TLS proxy in front of ClickHouse.
