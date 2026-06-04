#!/bin/bash
# Creates writer/reader from environment variables. Runs once on the first
# initialization of the ClickHouse volume, after 10-schema.sql. The 'readonly'
# profile is already loaded from users.d/low-mem.xml at startup.
set -e

clickhouse-client --multiquery <<SQL
CREATE USER IF NOT EXISTS writer IDENTIFIED BY '${CH_WRITER_PASSWORD}';
GRANT INSERT ON logs.logs TO writer;

CREATE USER IF NOT EXISTS reader IDENTIFIED BY '${CH_READER_PASSWORD}';
GRANT SELECT ON logs.logs TO reader;
ALTER USER reader SETTINGS PROFILE 'readonly';

-- read-only access to system tables for monitoring
GRANT SELECT ON system.parts TO reader;
GRANT SELECT ON system.metrics TO reader;
GRANT SELECT ON system.events TO reader;
GRANT SELECT ON system.asynchronous_metrics TO reader;
GRANT SELECT ON system.asynchronous_insert_log TO reader;
GRANT SELECT ON system.errors TO reader;
SQL
