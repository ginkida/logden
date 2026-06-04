-- =====================================================================
-- Аналитика (под reader-юзера / агента)
-- =====================================================================

-- Топ ошибок по проектам за последние 24 часа
SELECT project, count() AS errors
FROM logs.logs
WHERE timestamp > now() - INTERVAL 24 HOUR
  AND level IN ('error', 'critical', 'alert', 'emergency')
GROUP BY project
ORDER BY errors DESC;

-- Динамика по уровням, по часам, за сутки
SELECT toStartOfHour(timestamp) AS hour, level, count() AS n
FROM logs.logs
WHERE timestamp > now() - INTERVAL 24 HOUR
GROUP BY hour, level
ORDER BY hour, level;

-- Последние ошибки конкретного проекта
SELECT timestamp, level, message, context
FROM logs.logs
WHERE project = 'billing-api' AND level = 'error'
ORDER BY timestamp DESC
LIMIT 50;

-- Поиск по полю внутри context (JSON хранится строкой)
SELECT timestamp, project, message, JSONExtractInt(context, 'order_id') AS order_id
FROM logs.logs
WHERE JSONExtractInt(context, 'order_id') = 123
ORDER BY timestamp DESC;

-- Полнотекстовый поиск по сообщению.
-- hasToken использует skip-индекс idx_msg_tokens (быстро, без полного скана).
SELECT timestamp, project, level, message
FROM logs.logs
WHERE hasToken(message, 'timeout')
  AND timestamp > now() - INTERVAL 7 DAY
ORDER BY timestamp DESC
LIMIT 100;
-- Для произвольной подстроки (медленнее, индекс не работает): message ILIKE '%timeou%'

-- =====================================================================
-- Здоровье / мониторинг (нужны системные GRANT'ы для reader)
-- =====================================================================

-- Размер хранилища логов на диске (байты берём из system.parts:
-- в system.columns эти колонки не заполняются и дают 0).
SELECT
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM system.parts
WHERE database = 'logs' AND table = 'logs' AND active;

-- ТИХИЕ потери: ошибки async-вставок за последний час (status != 'Ok').
-- ВНИМАНИЕ: таблица создаётся лениво — до первой async-вставки её нет (UNKNOWN_TABLE).
SELECT status, count() AS n
FROM system.asynchronous_insert_log
WHERE event_time > now() - INTERVAL 1 HOUR
GROUP BY status;

-- Число активных партов по партициям (рост = мерджи не успевают)
SELECT partition, count() AS parts
FROM system.parts
WHERE database = 'logs' AND table = 'logs' AND active
GROUP BY partition
ORDER BY partition DESC;

-- Память ClickHouse относительно потолка (768 МБ)
SELECT metric, formatReadableSize(value) AS v
FROM system.asynchronous_metrics
WHERE metric IN ('MemoryResident', 'MemoryTracking');

-- Накопленные ошибки сервера
SELECT name, value FROM system.errors WHERE value > 0 ORDER BY value DESC;
