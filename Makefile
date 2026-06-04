GATEWAY := gateway

.PHONY: build test vet fmt lint run up down logs ps smoke clean help

help: ## показать список целей
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-10s %s\n", $$1, $$2}'

build: ## собрать бинарь шлюза
	cd $(GATEWAY) && CGO_ENABLED=0 go build -ldflags='-s -w' -o logden .

test: ## юнит-тесты с race-детектором
	cd $(GATEWAY) && go test -race ./...

vet: ## go vet
	cd $(GATEWAY) && go vet ./...

fmt: ## форматирование
	cd $(GATEWAY) && gofmt -w .

lint: ## vet + staticcheck
	cd $(GATEWAY) && go vet ./... && go run honnef.co/go/tools/cmd/staticcheck@latest ./...

run: ## локальный запуск шлюза (нужен ClickHouse и LOG_TOKEN)
	cd $(GATEWAY) && go run .

up: ## поднять стек в docker
	docker compose up -d --build

down: ## остановить стек
	docker compose down

logs: ## логи стека
	docker compose logs -f --tail=100

ps: ## статус контейнеров
	docker compose ps

smoke: ## отправить тестовый лог
	LOG_TOKEN=$${LOG_TOKEN:-test_shared_token_change_me} bash examples/curl.sh

loadtest: ## нагрузочный тест (нужен поднятый стек + LOG_TOKEN)
	cd tools/loadtest && go run . -token $${LOG_TOKEN:?set LOG_TOKEN}

clean: ## убрать артефакты сборки
	rm -f $(GATEWAY)/logden $(GATEWAY)/coverage.out
