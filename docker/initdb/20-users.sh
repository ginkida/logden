#!/bin/bash
# Создаёт writer/reader из переменных окружения. Выполняется один раз при первой
# инициализации тома ClickHouse, после 10-schema.sql. Профиль 'readonly' уже
# загружен из users.d/low-mem.xml на старте.
set -e

clickhouse-client --multiquery <<SQL
CREATE USER IF NOT EXISTS writer IDENTIFIED BY '${CH_WRITER_PASSWORD}';
GRANT INSERT ON logs.logs TO writer;

CREATE USER IF NOT EXISTS reader IDENTIFIED BY '${CH_READER_PASSWORD}';
GRANT SELECT ON logs.logs TO reader;
ALTER USER reader SETTINGS PROFILE 'readonly';

-- read-only доступ к системным таблицам для мониторинга
GRANT SELECT ON system.parts TO reader;
GRANT SELECT ON system.metrics TO reader;
GRANT SELECT ON system.events TO reader;
GRANT SELECT ON system.asynchronous_metrics TO reader;
GRANT SELECT ON system.asynchronous_insert_log TO reader;
GRANT SELECT ON system.errors TO reader;
SQL
