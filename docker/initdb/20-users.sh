#!/bin/bash
# Creates writer/reader from the environment. Runs once, on the first
# initialization of the ClickHouse volume, after 10-schema.sql. The 'readonly'
# profile is already loaded from users.d/low-mem.xml at startup.
#
# This file MUST stay mode 0755. Git tracks the executable bit and compose
# bind-mounts the host file with its own mode, so the checkout decides. The
# image entrypoint executes a .sh in /docker-entrypoint-initdb.d only when it is
# executable; otherwise it `.`-sources it into the entrypoint's own shell, where
# `set -euo pipefail` below is not scoped to this script - the entrypoint runs
# the rest of init, its temporary-server teardown and its final exec under the -u
# this line adds, and the `exit 1` below aborts the entrypoint rather than just
# this step. Sourcing still happens to create the users, which is why mode 0644
# went unnoticed.
set -euo pipefail

# Passwords arrive either directly in CH_*_PASSWORD (what docker-compose.yml
# does) or, under compose `secrets:`, as a path in CH_*_PASSWORD_FILE -- the same
# *_FILE pattern the gateway implements in readSecret (gateway/util.go), so both
# halves of the stack can be switched over together. docker-compose.yml explains
# why plain env vars are the default.
read_secret() {
  local name="$1" file_var="${1}_FILE"
  if [ -n "${!file_var:-}" ]; then
    # Both halves of the stack must derive the SAME secret from the same file, so
    # this mirrors the gateway's strings.TrimSpace (gateway/util.go readSecret):
    # strip leading and trailing whitespace, keep internal spaces. Command
    # substitution alone drops only trailing newlines, so a file saved with a
    # trailing space or CRLF would hash a different password here than the
    # gateway presents, and every insert would fail to authenticate.
    local v
    v="$(cat -- "${!file_var}")"
    v="${v#"${v%%[![:space:]]*}"}"
    v="${v%"${v##*[![:space:]]}"}"
    printf '%s' "$v"
  else
    printf '%s' "${!name:-}"
  fi
}

WRITER_PASSWORD="$(read_secret CH_WRITER_PASSWORD)"
READER_PASSWORD="$(read_secret CH_READER_PASSWORD)"

if [ -z "$WRITER_PASSWORD" ]; then
  echo "20-users.sh: set CH_WRITER_PASSWORD or CH_WRITER_PASSWORD_FILE" >&2
  exit 1
fi
if [ -z "$READER_PASSWORD" ]; then
  echo "20-users.sh: set CH_READER_PASSWORD or CH_READER_PASSWORD_FILE" >&2
  exit 1
fi

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

WRITER_HASH=$(hash_pw "$WRITER_PASSWORD")
READER_HASH=$(hash_pw "$READER_PASSWORD")

# CREATE USER OR REPLACE, not IF NOT EXISTS: on a first init the two are
# identical because nothing exists yet, but OR REPLACE also makes re-running this
# script the way to rotate a password afterwards --
#   docker compose exec -e CH_WRITER_PASSWORD=new clickhouse \
#     /docker-entrypoint-initdb.d/20-users.sh
# -- where IF NOT EXISTS reported success and changed nothing. OR REPLACE drops
# the user together with its grants; everything these two need is re-granted
# below, but a grant added by hand elsewhere is lost. After rotating writer, the
# gateway needs the new CLICKHOUSE_PASSWORD and a restart (SECURITY.md).
clickhouse-client --multiquery <<SQL
CREATE USER OR REPLACE writer IDENTIFIED WITH sha256_hash BY '${WRITER_HASH}';
GRANT INSERT ON logs.logs TO writer;

CREATE USER OR REPLACE reader IDENTIFIED WITH sha256_hash BY '${READER_HASH}';
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
