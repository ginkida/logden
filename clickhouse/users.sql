-- Два пользователя с минимальными правами (bare-metal путь).
-- Запускать под пользователем с access management:
--   clickhouse-client --multiquery < clickhouse/users.sql
-- ВАЖНО: замените пароли ниже на свои.
-- Профиль 'readonly' должен быть загружен из users.d/low-mem.xml.

CREATE USER IF NOT EXISTS writer IDENTIFIED BY 'CHANGE_ME_writer';
GRANT INSERT ON logs.logs TO writer;

CREATE USER IF NOT EXISTS reader IDENTIFIED BY 'CHANGE_ME_reader';
GRANT SELECT ON logs.logs TO reader;
ALTER USER reader SETTINGS PROFILE 'readonly';

-- read-only доступ к системным таблицам для мониторинга
GRANT SELECT ON system.parts TO reader;
GRANT SELECT ON system.metrics TO reader;
GRANT SELECT ON system.events TO reader;
GRANT SELECT ON system.asynchronous_metrics TO reader;
GRANT SELECT ON system.asynchronous_insert_log TO reader;
GRANT SELECT ON system.errors TO reader;
