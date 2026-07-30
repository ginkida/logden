#!/bin/bash
# Creates writer/reader from environment variables. Runs once on the first
# initialization of the ClickHouse volume, after 10-schema.sql. The 'readonly'
# profile is already loaded from users.d/low-mem.xml at startup.
set -euo pipefail

: "${CH_WRITER_PASSWORD:?CH_WRITER_PASSWORD must be set}"
: "${CH_READER_PASSWORD:?CH_READER_PASSWORD must be set}"

# Passwords go in as a SHA-256 hash, never as a literal: hex needs no SQL
# escaping (a quote or a backslash in the password would otherwise abort the
# whole init or create a user whose password is not the configured one), and a
# failing statement echoed by clickhouse-client cannot leak the secret into the
# container log.
hash_pw() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$1" | sha256sum | cut -d' ' -f1
  elif command -v openssl >/dev/null 2>&1; then
    printf '%s' "$1" | openssl dgst -sha256 -r | cut -d' ' -f1
  else
    echo "20-users.sh: need sha256sum or openssl to hash the passwords" >&2
    exit 1
  fi
}

WRITER_HASH=$(hash_pw "$CH_WRITER_PASSWORD")
READER_HASH=$(hash_pw "$CH_READER_PASSWORD")

clickhouse-client --multiquery <<SQL
CREATE USER IF NOT EXISTS writer IDENTIFIED WITH sha256_hash BY '${WRITER_HASH}';
GRANT INSERT ON logs.logs TO writer;

CREATE USER IF NOT EXISTS reader IDENTIFIED WITH sha256_hash BY '${READER_HASH}';
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
