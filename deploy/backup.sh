#!/usr/bin/env bash
# Daily backup of the logs.logs table via native BACKUP to the 'backups' disk
# (config.d/backup.xml). Run from the repo root; add it to cron, e.g.:
#   0 4 * * *  cd /opt/logden && ./deploy/backup.sh >> /var/log/logden-backup.log 2>&1
#
# NOTE: backups live in the ch-data volume. For real DR, copy them to another
# host (rsync the volume directory or push to object storage) — see the RSYNC line below.
#
# Tunables (environment):
#   RETAIN_DAYS     how long to keep local backups (default 7). Every backup is a
#                   FULL copy of the table, so N days of them cost N x the table
#                   size on the SAME disk the database lives on.
#   MIN_FREE_BYTES  extra free space to leave untouched (default 2 GiB, the same
#                   threshold as the ClickHouseDiskLow alert).
set -euo pipefail
cd "$(dirname "$0")/.."

RETAIN_DAYS=${RETAIN_DAYS:-7}
MIN_FREE_BYTES=${MIN_FREE_BYTES:-2147483648}

STAMP=$(date -u +%Y%m%d-%H%M%S)
NAME="logs-${STAMP}.zip"

ch() { docker compose exec -T clickhouse "$@"; }

# Rotate FIRST: expired backups are the cheapest space to reclaim, and they must
# not count against the free-space check below.
if ! ch find /var/lib/clickhouse/backups -name 'logs-*.zip' -mtime "+${RETAIN_DAYS}" -delete; then
  echo "[$(date -u)] WARNING: rotation failed, old backups may be piling up" >&2
fi

# Refuse rather than fill the disk: a full disk makes ClickHouse reject inserts,
# which costs more than a missed backup. Disk is the most likely exhaustion
# vector on a small VPS (see RUNBOOK), and these backups share the data volume.
TABLE_BYTES=$(ch clickhouse-client -q \
  "SELECT ifNull(sum(bytes_on_disk), 0) FROM system.parts WHERE database='logs' AND table='logs' AND active" |
  tr -cd '0-9')
AVAIL_BYTES=$(ch df -Pk /var/lib/clickhouse | awk 'NR==2 {printf "%d", $4 * 1024}')
NEED_BYTES=$(( ${TABLE_BYTES:-0} + MIN_FREE_BYTES ))

if [ "${AVAIL_BYTES:-0}" -lt "$NEED_BYTES" ]; then
  echo "[$(date -u)] SKIPPED ${NAME}: need ${NEED_BYTES} bytes free (table ${TABLE_BYTES:-0} + reserve ${MIN_FREE_BYTES}), have ${AVAIL_BYTES:-0}" >&2
  echo "[$(date -u)] free space up (RUNBOOK: DROP PARTITION / prune backups) or lower RETAIN_DAYS" >&2
  exit 1
fi

echo "[$(date -u)] backup ${NAME} (table ${TABLE_BYTES:-0} bytes, ${AVAIL_BYTES} free, keeping ${RETAIN_DAYS}d)"
ch clickhouse-client -q "BACKUP TABLE logs.logs TO Disk('backups', '${NAME}')"

# DR (uncomment and set the destination):
# RSYNC_DEST="user@backup-host:/srv/logden-backups/"
# docker run --rm -v logden_ch-data:/data -v "$HOME/.ssh:/root/.ssh:ro" \
#   instrumentisto/rsync-ssh rsync -az /data/backups/ "$RSYNC_DEST"

echo "[$(date -u)] done"
