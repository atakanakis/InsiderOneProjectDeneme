.PHONY: up down logs test test-unit lint cover build load

up: ## Start the whole stack (API, 2 workers, Postgres, Redis, mock provider, Jaeger, Prometheus)
	docker compose up --build -d
	@echo "API docs: http://localhost:8080/docs   Jaeger: http://localhost:16686   Prometheus: http://localhost:9095"

down: ## Stop the stack and delete its data
	docker compose down -v

logs:
	docker compose logs -f api worker

test: ## All tests, including integration tests against real Postgres/Redis (needs Docker)
	go test -race -count=1 ./...

test-unit: ## Fast tests only, no Docker needed
	go test -race -count=1 -short ./...

cover:
	go test -count=1 -coverpkg=./internal/... -coverprofile=coverage.out ./internal/...
	go tool cover -func=coverage.out | tail -1

lint:
	docker run --rm -v $(CURDIR):/app -w /app golangci/golangci-lint:v2.15.0 golangci-lint run ./...

build:
	go build -o bin/ ./cmd/...

load: ## Send 2000 notifications and report throughput (stack must be running)
	python3 scripts/loadtest.py
