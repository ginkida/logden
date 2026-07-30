-- Log storage schema.
-- Apply with: clickhouse-client --multiquery < clickhouse/schema.sql
-- Edits here are NOT applied to an already-initialized table (IF NOT EXISTS);
-- for live-schema changes see clickhouse/migrations.sql.

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.logs
(
    -- 'UTC' is pinned deliberately: the gateway sends a naive "YYYY-MM-DD hh:mm:ss.mmm"
    -- string in UTC, and a column without a timezone is parsed in the SERVER's
    -- timezone — on a host set to, say, Asia/Almaty every row would land 5 hours
    -- off (measured), taking the partition day and the TTL with it.
    timestamp  DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    project    LowCardinality(String),
    level      LowCardinality(String) DEFAULT 'info',
    message    String                 CODEC(ZSTD(1)),
    context    String DEFAULT '{}'    CODEC(ZSTD(1)),   -- arbitrary JSON as a string
    source_ip  String DEFAULT '',
    -- token-bloom indexes speed up word search without a full column scan
    INDEX idx_msg_tokens message TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 4,
    INDEX idx_ctx_tokens context TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)           -- retention = drop whole partitions by day
ORDER BY (project, level, timestamp)          -- tuned for "project + level + period" filters
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1,                  -- cheap drop of whole expired partitions
    merge_with_ttl_timeout = 3600;
