-- =====================================================================
-- Analytics (for the reader user / agent)
-- =====================================================================

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

-- Latest errors for a specific project
SELECT timestamp, level, message, context
FROM logs.logs
WHERE project = 'billing-api' AND level = 'error'
ORDER BY timestamp DESC
LIMIT 50;

-- Search a field inside context (JSON stored as a string)
SELECT timestamp, project, message, JSONExtractInt(context, 'order_id') AS order_id
FROM logs.logs
WHERE JSONExtractInt(context, 'order_id') = 123
ORDER BY timestamp DESC;

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

-- ClickHouse memory relative to the cap (768 MB)
SELECT metric, formatReadableSize(value) AS v
FROM system.asynchronous_metrics
WHERE metric IN ('MemoryResident', 'MemoryTracking');

-- Accumulated server errors
SELECT name, value FROM system.errors WHERE value > 0 ORDER BY value DESC;
