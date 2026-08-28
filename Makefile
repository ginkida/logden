GATEWAY := gateway

.PHONY: build test vet fmt lint run up down logs ps smoke clean help

help: ## show the list of targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-10s %s\n", $$1, $$2}'

build: ## build the gateway binary
	cd $(GATEWAY) && CGO_ENABLED=0 go build -ldflags='-s -w' -o logden .

test: ## unit tests with the race detector
	cd $(GATEWAY) && go test -race ./...

vet: ## go vet
	cd $(GATEWAY) && go vet ./...

fmt: ## formatting
	cd $(GATEWAY) && gofmt -w .

lint: ## vet + staticcheck
# Same pin as .github/workflows/ci.yml. On @latest a local `make lint` and CI can
# run different analyzer versions, so the check that gates the merge is not the
# check the author ran; bump both in one commit.
	cd $(GATEWAY) && go vet ./... && go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

run: ## run the gateway locally (needs ClickHouse and LOG_TOKEN)
	cd $(GATEWAY) && go run .

up: ## bring the stack up in docker
	docker compose up -d --build

down: ## stop the stack
	docker compose down

logs: ## stack logs
	docker compose logs -f --tail=100

ps: ## container status
	docker compose ps

smoke: ## send a test log
	LOG_TOKEN=$${LOG_TOKEN:-test_shared_token_change_me} bash examples/curl.sh

loadtest: ## load test (needs a running stack + LOG_TOKEN)
	cd tools/loadtest && go run . -token $${LOG_TOKEN:?set LOG_TOKEN}

clean: ## remove build artifacts
	rm -f $(GATEWAY)/logden $(GATEWAY)/coverage.out
