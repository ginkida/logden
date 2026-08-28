-- Two least-privilege users (bare-metal path). The docker path runs the same
-- statements from docker/initdb/20-users.sh on the volume's first start.
--
-- This file is TRACKED and deliberately contains no usable credential. The two
-- values below are sentinels, and ClickHouse refuses a sha256_hash that is not
-- 64 hex characters, so running this file verbatim aborts on the very first
-- statement. That is the point: it used to ship 'CHANGE_ME_writer' as a real
-- password behind CREATE USER IF NOT EXISTS, which meant a run that was never
-- corrected produced accounts whose passwords are published in this repository,
-- and the corrective re-run afterwards was a silent no-op.
--
-- Substitute the hashes from the environment; no plaintext ever reaches disk:
--
--   pwhash() { printf '%s' "$1" | sha256sum | cut -d' ' -f1; }
--   # no sha256sum (BSD/macOS)?  openssl dgst -sha256 -r | cut -d' ' -f1
--   set -a; . ./.env; set +a                    # or export the two vars yourself
--   sed -e "s/__WRITER_HASH__/$(pwhash "$CH_WRITER_PASSWORD")/" \
--       -e "s/__READER_HASH__/$(pwhash "$CH_READER_PASSWORD")/" clickhouse/users.sql \
--     | clickhouse-client --multiquery
--
-- If you would rather keep an edited copy, copy it to clickhouse/users.local.sql
-- and edit that: .gitignore ignores *.local.sql precisely so real credentials
-- cannot be committed. Never edit this file in place.
--
-- Hashes rather than IDENTIFIED BY '<password>': hex needs no SQL escaping (a
-- quote or a backslash in the password otherwise aborts the run, or silently
-- creates a user whose password is not the configured one), and a statement
-- echoed back by clickhouse-client on error cannot leak the secret into the
-- terminal or the shell history.
--
-- The 'readonly' profile must already be loaded from users.d/low-mem.xml, and
-- 'default' needs access management (docker/clickhouse-access.xml).

-- OR REPLACE, not IF NOT EXISTS: re-running this file is how a password gets
-- rotated, and IF NOT EXISTS made that a no-op that reported success while the
-- account kept the old secret. OR REPLACE drops the user together with its
-- grants, so every grant these two need is re-applied below -- a grant added by
-- hand outside this file is lost and has to be re-applied too.
CREATE USER OR REPLACE writer IDENTIFIED WITH sha256_hash BY '__WRITER_HASH__';
GRANT INSERT ON logs.logs TO writer;

CREATE USER OR REPLACE reader IDENTIFIED WITH sha256_hash BY '__READER_HASH__';
GRANT SELECT ON logs.logs TO reader;
ALTER USER reader SETTINGS PROFILE 'readonly';

-- read-only access to system tables for monitoring
GRANT SELECT ON system.parts TO reader;
GRANT SELECT ON system.metrics TO reader;
GRANT SELECT ON system.events TO reader;
GRANT SELECT ON system.asynchronous_metrics TO reader;
GRANT SELECT ON system.asynchronous_insert_log TO reader;
GRANT SELECT ON system.errors TO reader;
