#!/usr/bin/env bash
# Daily backup of the logs.logs table via native BACKUP to the 'backups' disk
# (config.d/backup.xml). Run from the repo root; add it to cron, e.g.:
#   0 4 * * *  cd /opt/logden && ./deploy/backup.sh >> /var/log/logden-backup.log 2>&1
#
# NOTE: backups live in the ch-data volume. For real DR, copy them to another
# host (rsync the volume directory or push to object storage) — see the RSYNC line below.
set -euo pipefail
cd "$(dirname "$0")/.."

STAMP=$(date -u +%Y%m%d-%H%M%S)
NAME="logs-${STAMP}.zip"

echo "[$(date -u)] backup ${NAME}"
docker compose exec -T clickhouse clickhouse-client -q \
  "BACKUP TABLE logs.logs TO Disk('backups', '${NAME}')"

# Rotation: delete backups older than 30 days
docker compose exec -T clickhouse \
  find /var/lib/clickhouse/backups -name 'logs-*.zip' -mtime +30 -delete || true

# DR (uncomment and set the destination):
# RSYNC_DEST="user@backup-host:/srv/logden-backups/"
# docker run --rm -v logden_ch-data:/data -v "$HOME/.ssh:/root/.ssh:ro" \
#   instrumentisto/rsync-ssh rsync -az /data/backups/ "$RSYNC_DEST"

echo "[$(date -u)] done"
