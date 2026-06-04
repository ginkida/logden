#!/usr/bin/env bash
# Ежедневный бэкап таблицы logs.logs нативным BACKUP в диск 'backups'
# (config.d/backup.xml). Запуск из корня репозитория; поставить в cron, напр.:
#   0 4 * * *  cd /opt/logden && ./deploy/backup.sh >> /var/log/logden-backup.log 2>&1
#
# ВНИМАНИЕ: бэкапы лежат в томе ch-data. Для настоящего DR копируйте их на другой
# хост (rsync каталога тома или объектное хранилище) — см. строку RSYNC ниже.
set -euo pipefail
cd "$(dirname "$0")/.."

STAMP=$(date -u +%Y%m%d-%H%M%S)
NAME="logs-${STAMP}.zip"

echo "[$(date -u)] backup ${NAME}"
docker compose exec -T clickhouse clickhouse-client -q \
  "BACKUP TABLE logs.logs TO Disk('backups', '${NAME}')"

# Ротация: удалить бэкапы старше 30 дней
docker compose exec -T clickhouse \
  find /var/lib/clickhouse/backups -name 'logs-*.zip' -mtime +30 -delete || true

# DR (раскомментируйте и настройте назначение):
# RSYNC_DEST="user@backup-host:/srv/logden-backups/"
# docker run --rm -v logden_ch-data:/data -v "$HOME/.ssh:/root/.ssh:ro" \
#   instrumentisto/rsync-ssh rsync -az /data/backups/ "$RSYNC_DEST"

echo "[$(date -u)] done"
