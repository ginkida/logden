-- Live-schema migrations.
--
-- IMPORTANT: schema.sql uses CREATE TABLE IF NOT EXISTS and does NOT apply
-- changes to an already-initialized volume. Make any change to an existing
-- table with a separate ALTER from here (as a user with ALTER privileges —
-- inside docker that is default on loopback). Apply with:
--   docker compose exec clickhouse clickhouse-client --multiquery < clickhouse/migrations.sql

-- --- Pin the timestamp column to UTC (tables created before this was in schema.sql) ---
-- The gateway writes a naive UTC string; a DateTime64(3) without a timezone is
-- parsed in the SERVER's timezone, so on a non-UTC host every row lands shifted
-- (verified: Asia/Almaty stored 12:00 UTC as 07:00 UTC). Metadata-only, instant:
-- ALTER TABLE logs.logs MODIFY COLUMN timestamp DateTime64(3, 'UTC');
-- Check the server first: SELECT timezone();  -- 'UTC' => nothing was shifted
-- NOTE: rows already written on a non-UTC server keep their shift; the ALTER only
-- fixes how new values are parsed and how all of them are displayed.

-- --- Add a column (safe: JSONEachRow by name, old INSERTs keep working) ---
-- ALTER TABLE logs.logs ADD COLUMN IF NOT EXISTS trace_id String DEFAULT '' CODEC(ZSTD(1));
-- After this the gateway can start sending the trace_id field; old rows get the DEFAULT.

-- --- Add a skip index to an existing table ---
-- ALTER TABLE logs.logs ADD INDEX IF NOT EXISTS idx_msg_tokens message TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 4;
-- ALTER TABLE logs.logs MATERIALIZE INDEX idx_msg_tokens;   -- in background; for 1GB run during off-hours

-- --- Change retention (TTL) ---
-- ALTER TABLE logs.logs MODIFY TTL toDateTime(timestamp) + INTERVAL 14 DAY DELETE;
-- Applies on new merges. To apply immediately: MATERIALIZE TTL (expensive).

-- --- Enable cheap partition drop (if the table was created without it) ---
-- ALTER TABLE logs.logs MODIFY SETTING ttl_only_drop_parts = 1;

-- --- Manually drop an old partition (e.g. when the disk fills up) ---
-- ALTER TABLE logs.logs DROP PARTITION '20260101';

-- ORDER BY/PARTITION BY CANNOT be changed after the fact without recreating the table.
-- If needed: create logs.logs_v2 with the new key, INSERT SELECT, switch the gateway over.
