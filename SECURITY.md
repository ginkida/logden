# Security

## Модель угроз

- **Аутентификация — общий токен.** `LOG_TOKEN` подтверждает «это наш лог»;
  авторизация НЕ на уровне проекта — любой держатель токена может писать под
  любым `project`. Это осознанный компромисс ради простоты. Токен сравнивается
  константно по времени; поддерживается несколько токенов для ротации.
- **ClickHouse не публикуется наружу.** Порты 8123/9000/9363 доступны только из
  docker-сети. НЕ добавляйте им `ports:`. Пользователь `default` беспарольный, но
  ограничен loopback (`docker/clickhouse-access.xml`); из сети ходят только
  `writer` (INSERT) и `reader` (SELECT).
- **TLS.** Шлюз слушает HTTP — токен и логи идут открытым текстом. В проде
  ставьте reverse-proxy (caddy/nginx) с TLS перед шлюзом, шлюз — на loopback
  (`LISTEN_ADDR=127.0.0.1:8080`). Пример Caddyfile:
  ```
  logs.example.com {
      reverse_proxy 127.0.0.1:8080
      header Strict-Transport-Security "max-age=31536000"
  }
  ```
- **source_ip / X-Forwarded-For.** XFF принимается только если соединение пришло
  с адреса из `TRUSTED_PROXIES` (CIDR). Без него — всегда реальный peer. Если
  шлюз стоит за прокси, укажите его CIDR, иначе source_ip спуфится клиентом.

## Защитные меры

- Rate limiting (`RATE_LIMIT_RPS`) против абуза утёкшим токеном.
- `/metrics` без `METRICS_TOKEN` открыт всем (шлюз пишет warning при старте):
  наружу утекают версия сборки и статистика потока. Если шлюз торчит в интернет —
  задайте `METRICS_TOKEN` или закройте путь на reverse-proxy.
- Лимиты тела/сообщения/контекста/числа событий; защита от gzip-bomb.
- Валидация `project` (charset+длина) и whitelist `level` — против cardinality-DoS.
- Контейнер шлюза: `read_only`, `cap_drop: ALL`, `no-new-privileges`, distroless nonroot.
- Секреты можно подавать файлами (`*_FILE`), не только через env.

## Ротация секретов

- Токен — см. [RUNBOOK.md](RUNBOOK.md#ротация-общего-токена-без-даунтайма).
- Пароли ClickHouse: `ALTER USER writer IDENTIFIED BY '…'` + перезапуск шлюза с
  новым `CLICKHOUSE_PASSWORD`.

## Сообщить об уязвимости

Пишите на ginkida@gmail.com. Пожалуйста, не открывайте публичный issue до фикса.
