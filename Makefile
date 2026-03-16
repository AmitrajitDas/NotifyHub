.PHONY: all build test test-integration lint run-api run-worker \
        docker-up docker-down migrate-up migrate-down generate seed

# Load .env if it exists, then export all vars to subprocesses
-include .env
export

# ── Build ──────────────────────────────────────────────────────────────────────

all: lint test build

build:
	go build -o bin/api    ./cmd/api
	go build -o bin/worker ./cmd/worker

# ── Test ───────────────────────────────────────────────────────────────────────

test:
	go test ./... -v -race -cover

test-integration:
	go test ./... -v -tags=integration -race -timeout=120s

# ── Lint ───────────────────────────────────────────────────────────────────────

lint:
	golangci-lint run ./...

# ── Run ────────────────────────────────────────────────────────────────────────

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

# ── Database ───────────────────────────────────────────────────────────────────

migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

migrate-drop:
	migrate -path migrations -database "$(DATABASE_URL)" drop -f

# ── Code Generation ────────────────────────────────────────────────────────────

generate:
	sqlc generate

# ── Docker ─────────────────────────────────────────────────────────────────────

docker-up:
	docker compose -f deployments/docker-compose.yml up -d

docker-up-infra:
	docker compose -f deployments/docker-compose.yml up -d postgres redis kafka kafka-ui prometheus grafana

docker-down:
	docker compose -f deployments/docker-compose.yml down

docker-down-volumes:
	docker compose -f deployments/docker-compose.yml down -v

docker-logs:
	docker compose -f deployments/docker-compose.yml logs -f

# ── Seed ───────────────────────────────────────────────────────────────────────

seed:
	go run ./scripts/seed.go

# ── Helpers ────────────────────────────────────────────────────────────────────

tidy:
	go mod tidy

vet:
	go vet ./...

.DEFAULT_GOAL := build
