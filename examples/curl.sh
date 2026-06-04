#!/usr/bin/env bash
# Ship logs from anything — a single command.
set -euo pipefail

: "${LOG_TOKEN:?set LOG_TOKEN}"
ENDPOINT="${ENDPOINT:-http://localhost:8080}"
AUTH=(-H "Authorization: Bearer $LOG_TOKEN" -H "Content-Type: application/json")

# single log
curl -fsS -X POST "$ENDPOINT/logs" "${AUTH[@]}" \
  -d '{"project":"billing-api","level":"error","message":"Payment gateway timeout","context":{"order_id":123}}'

# batch (JSON array)
curl -fsS -X POST "$ENDPOINT/logs" "${AUTH[@]}" \
  -d '[{"project":"web","message":"login"},{"project":"web","level":"warn","message":"slow query"}]'

# NDJSON
printf '%s\n%s' \
  '{"project":"api","level":"info","message":"request"}' \
  '{"project":"api","level":"error","message":"db down"}' \
  | curl -fsS -X POST "$ENDPOINT/logs" "${AUTH[@]}" --data-binary @-

# gzip
printf '{"project":"cron","message":"nightly job done"}' \
  | gzip \
  | curl -fsS -X POST "$ENDPOINT/logs" "${AUTH[@]}" -H "Content-Encoding: gzip" --data-binary @-

echo "sent."
