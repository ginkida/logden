-- Миграции живой схемы.
--
-- ВАЖНО: schema.sql использует CREATE TABLE IF NOT EXISTS и на уже
-- инициализированном томе НЕ применяет изменения. Любое изменение существующей
-- таблицы делайте отдельным ALTER отсюда (под пользователем с правами ALTER —
-- внутри docker это default на loopback). Применение:
--   docker compose exec clickhouse clickhouse-client --multiquery < clickhouse/migrations.sql

-- --- Добавить колонку (безопасно: JSONEachRow по именам, старые INSERT не ломаются) ---
-- ALTER TABLE logs.logs ADD COLUMN IF NOT EXISTS trace_id String DEFAULT '' CODEC(ZSTD(1));
-- После этого шлюз может начать слать поле trace_id; старые строки получат DEFAULT.

-- --- Добавить skip-индекс на существующей таблице ---
-- ALTER TABLE logs.logs ADD INDEX IF NOT EXISTS idx_msg_tokens message TYPE tokenbf_v1(16384, 3, 0) GRANULARITY 4;
-- ALTER TABLE logs.logs MATERIALIZE INDEX idx_msg_tokens;   -- фоном; на 1ГБ — в тихий час

-- --- Изменить ретеншн (TTL) ---
-- ALTER TABLE logs.logs MODIFY TTL toDateTime(timestamp) + INTERVAL 14 DAY DELETE;
-- Применяется к новым мерджам. Для немедленного применения: MATERIALIZE TTL (дорого).

-- --- Включить дешёвый дроп партиций (если таблица создана без него) ---
-- ALTER TABLE logs.logs MODIFY SETTING ttl_only_drop_parts = 1;

-- --- Ручной дроп старой партиции (например, при заполнении диска) ---
-- ALTER TABLE logs.logs DROP PARTITION '20260101';

-- НЕЛЬЗЯ менять ORDER BY/PARTITION BY задним числом без пересоздания таблицы.
-- При необходимости: создать logs.logs_v2 с новым ключом, INSERT SELECT, переключить шлюз.
