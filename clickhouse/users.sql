-- Two least-privilege users (bare-metal path).
-- Run as a user with access management:
--   clickhouse-client --multiquery < clickhouse/users.sql
-- IMPORTANT: replace the passwords below with your own.
-- The 'readonly' profile must be loaded from users.d/low-mem.xml.
-- A password with a quote or a backslash breaks the string literal; either avoid
-- those characters or use the hash form, which is what the docker path does
-- (docker/initdb/20-users.sh):
--   IDENTIFIED WITH sha256_hash BY '<printf %s "$PASSWORD" | sha256sum>'

CREATE USER IF NOT EXISTS writer IDENTIFIED BY 'CHANGE_ME_writer';
GRANT INSERT ON logs.logs TO writer;

CREATE USER IF NOT EXISTS reader IDENTIFIED BY 'CHANGE_ME_reader';
GRANT SELECT ON logs.logs TO reader;
ALTER USER reader SETTINGS PROFILE 'readonly';

-- read-only access to system tables for monitoring
GRANT SELECT ON system.parts TO reader;
GRANT SELECT ON system.metrics TO reader;
GRANT SELECT ON system.events TO reader;
GRANT SELECT ON system.asynchronous_metrics TO reader;
GRANT SELECT ON system.asynchronous_insert_log TO reader;
GRANT SELECT ON system.errors TO reader;
