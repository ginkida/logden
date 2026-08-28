-- Ready-made analysis queries for the read-only reader user.
--
-- The reader runs under the 'readonly' profile (clickhouse/users.d/low-mem.xml):
-- 30 s max_execution_time, 300 MB per query, 2 threads, 200000 max_result_rows,
-- and readonly mode means none of that can be raised per query. Crossing the row
-- cap fails loudly with TOO_MANY_ROWS (result_overflow_mode = throw), so a
-- truncated answer is never handed back as if it were complete — on that error
-- narrow the window or add a LIMIT, never rerun the query unchanged.
--
-- Every query here is bounded by time so that PARTITION BY toYYYYMMDD(timestamp)
-- prunes whole days before anything is read; dropping the timestamp filter is
-- what turns a 200 ms query into a 30 s timeout. Text search goes through
-- hasToken() (tokenbf skip indexes), never LIKE.

-- =====================================================================
-- Analytics (for the reader user / agent)
-- =====================================================================

-- Project inventory: who sends, how much, and who went quiet. The first query to
-- run on an unfamiliar installation — a mistyped project name shows up as a
-- near-duplicate row, a dead sender as a stale last_seen.
-- Bounded to 7 days deliberately (the table keeps 30), so first_seen means
-- "first seen inside the window", not "ever".
SELECT
    project,
    count()                                                       AS events,
    countIf(level IN ('error', 'critical', 'alert', 'emergency')) AS errors,
    min(timestamp)                                                AS first_seen,
    max(timestamp)                                                AS last_seen,
    dateDiff('minute', max(timestamp), now())                     AS silent_minutes
FROM logs.logs
WHERE timestamp > now() - INTERVAL 7 DAY
GROUP BY project
ORDER BY last_seen DESC;

-- Spike detector: a (project, level) pair whose last hour is far above its own
-- 7-day baseline. Self-relative on purpose — an absolute threshold has to be
-- retuned for every project, this one does not.
-- 167 = the remaining hours of the 7-day window (168 minus the current one).
-- The `last_hour >= 10` floor is what makes it usable: without it a project with
-- a zero baseline reports an infinite spike the first time it logs one warning.
-- The level filter also prunes granules — level is the second column of
-- ORDER BY (project, level, timestamp).
SELECT
    project,
    level,
    countIf(timestamp >  now() - INTERVAL 1 HOUR)                  AS last_hour,
    round(countIf(timestamp <= now() - INTERVAL 1 HOUR) / 167, 2)  AS baseline_per_hour,
    round(last_hour / greatest(baseline_per_hour, 0.01), 1)        AS ratio
FROM logs.logs
WHERE timestamp > now() - INTERVAL 7 DAY
  AND level IN ('warning', 'error', 'critical', 'alert', 'emergency')
GROUP BY project, level
HAVING last_hour >= 10 AND last_hour > 3 * baseline_per_hour
ORDER BY ratio DESC, last_hour DESC;

-- Silence detector (dead man's switch): a project that was steadily sending over
-- the past week and has sent nothing in the last hour. The mirror image of the
-- spike query — a crashed cron or a broken LOG_TOKEN is invisible in every
-- "top errors" view, because the symptom is the absence of rows.
-- `baseline_per_hour >= 1` skips projects that are naturally bursty (a weekly
-- job is silent 167 hours out of 168 and is not an incident).
SELECT
    project,
    max(timestamp)                                                AS last_seen,
    dateDiff('minute', max(timestamp), now())                     AS silent_minutes,
    round(countIf(timestamp <= now() - INTERVAL 1 HOUR) / 167, 2) AS baseline_per_hour
FROM logs.logs
WHERE timestamp > now() - INTERVAL 7 DAY
GROUP BY project
HAVING baseline_per_hour >= 1
   AND countIf(timestamp > now() - INTERVAL 1 HOUR) = 0
ORDER BY silent_minutes DESC;

-- Top errors per project over the last 24 hours
SELECT project, count() AS errors
FROM logs.logs
WHERE timestamp > now() - INTERVAL 24 HOUR
  AND level IN ('error', 'critical', 'alert', 'emergency')
GROUP BY project
ORDER BY errors DESC;

-- Per-level trend, hourly, over one day
SELECT toStartOfHour(timestamp) AS hour, level, count() AS n
FROM logs.logs
WHERE timestamp > now() - INTERVAL 24 HOUR
GROUP BY hour, level
ORDER BY hour, level;

-- Loudest error signatures for one project: the follow-up to a spike ("which
-- message is it?"). project + level + time is exactly the ORDER BY
-- (project, level, timestamp) prefix, so this reads a narrow range of granules.
-- The grouping key is a prefix, not a normalized signature: an id early in the
-- message splits one incident across rows — widen the cut, or strip the numbers
-- with replaceRegexpAll(message, '[0-9]+', 'N') if that happens.
SELECT
    substringUTF8(message, 1, 120) AS signature,
    count()                        AS n,
    max(timestamp)                 AS last_seen
FROM logs.logs
WHERE project = 'billing-api'
  AND level IN ('error', 'critical', 'alert', 'emergency')
  AND timestamp > now() - INTERVAL 24 HOUR
GROUP BY signature
ORDER BY n DESC
LIMIT 20;

-- Latest errors for a specific project
SELECT timestamp, level, message, context
FROM logs.logs
WHERE project = 'billing-api' AND level = 'error'
ORDER BY timestamp DESC
LIMIT 50;

-- Search a field inside context (JSON stored as a string).
-- ALWAYS bound it by time: JSONExtract reads the whole context column, no index
-- applies, and without the partition filter the reader profile hits its 30s
-- max_execution_time on a table of any size. hasToken narrows the scan first
-- (the value must be a whole token — for a substring drop that line).
SELECT timestamp, project, message, JSONExtractInt(context, 'order_id') AS order_id
FROM logs.logs
WHERE timestamp > now() - INTERVAL 24 HOUR
  AND hasToken(context, '123')
  AND JSONExtractInt(context, 'order_id') = 123
ORDER BY timestamp DESC
LIMIT 100;

-- Full-text search over the message.
-- hasToken uses the idx_msg_tokens skip index (fast, no full scan).
SELECT timestamp, project, level, message
FROM logs.logs
WHERE hasToken(message, 'timeout')
  AND timestamp > now() - INTERVAL 7 DAY
ORDER BY timestamp DESC
LIMIT 100;
-- For an arbitrary substring (slower, the index does not apply): message ILIKE '%timeou%'

-- =====================================================================
-- Health / monitoring (reader needs system GRANTs)
-- =====================================================================

-- On-disk size of log storage (bytes come from system.parts:
-- in system.columns these columns are not populated and read 0).
SELECT
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM system.parts
WHERE database = 'logs' AND table = 'logs' AND active;

-- SILENT loss: async insert errors over the last hour (status != 'Ok').
-- NOTE: the table is created lazily — until the first async insert it does not exist (UNKNOWN_TABLE).
SELECT status, count() AS n
FROM system.asynchronous_insert_log
WHERE event_time > now() - INTERVAL 1 HOUR
GROUP BY status;

-- Number of active parts per partition (growth = merges falling behind)
SELECT partition, count() AS parts
FROM system.parts
WHERE database = 'logs' AND table = 'logs' AND active
GROUP BY partition
ORDER BY partition DESC;

-- Rows and disk per day, newest first. Reads part metadata only (no table scan),
-- so it is the cheap way to see which day is eating the disk before a
-- DROP PARTITION (RUNBOOK.md) — and a day missing from this list is a day the
-- gateway wrote nothing at all.
SELECT
    partition                                      AS day,
    sum(rows)                                      AS row_count,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed
FROM system.parts
WHERE database = 'logs' AND table = 'logs' AND active
GROUP BY partition
ORDER BY day DESC;

-- ClickHouse memory relative to the cap (768 MB). MemoryResident is an async
-- metric; MemoryTracking is a CurrentMetric and lives in system.metrics — asking
-- for it in asynchronous_metrics silently returns nothing.
SELECT metric, formatReadableSize(value) AS v
FROM system.asynchronous_metrics
WHERE metric = 'MemoryResident'
UNION ALL
SELECT metric, formatReadableSize(value)
FROM system.metrics
WHERE metric = 'MemoryTracking';

-- Accumulated server errors
SELECT name, value FROM system.errors WHERE value > 0 ORDER BY value DESC;
