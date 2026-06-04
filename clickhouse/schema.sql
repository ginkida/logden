-- Схема хранилища логов.
-- Применить: clickhouse-client --multiquery < clickhouse/schema.sql
-- На уже инициализированной таблице правки сюда НЕ применяются (IF NOT EXISTS);
-- изменения живой схемы — см. clickhouse/migrations.sql.

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.logs
(
    timestamp  DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    project    LowCardinality(String),
    level      LowCardinality(String) DEFAULT 'info',
    message    String                 CODEC(ZSTD(1)),
    context    String DEFAULT '{}'    CODEC(ZSTD(1)),   -- произвольный JSON строкой
    source_ip  String DEFAULT '',
    -- токен-bloom индексы ускоряют поиск по словам без полного скана столбца
    INDEX idx_msg_tokens message TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 4,
    INDEX idx_ctx_tokens context TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)           -- ретеншн = дроп целых партиций по дню
ORDER BY (project, level, timestamp)          -- под фильтры «проект + уровень + период»
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1,                  -- дешёвый дроп просроченных партиций целиком
    merge_with_ttl_timeout = 3600;
