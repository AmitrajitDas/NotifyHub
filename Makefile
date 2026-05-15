.PHONY: all build test test-integration lint run-api run-worker \
        docker-up docker-down migrate-up migrate-down generate seed \
        loadtest-k6 loadtest-k6-smoke loadtest-k6-docker loadtest-k6-docker-smoke \
        loadtest-inapp loadtest-inapp-smoke loadtest-inapp-prod1000 \
        loadtest-inapp-docker loadtest-inapp-docker-smoke loadtest-inapp-docker-prod1000

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

# ── Load Testing ───────────────────────────────────────────────────────────────

loadtest-k6:
	k6 run scripts/loadtest/send_notification.js

loadtest-k6-smoke:
	K6_SCENARIO=smoke k6 run scripts/loadtest/send_notification.js

loadtest-k6-docker:
	docker run --rm -v "$(PWD)/scripts/loadtest:/scripts" -e NOTIFYHUB_BASE_URL=$${NOTIFYHUB_BASE_URL:-http://host.docker.internal:8080} -e NOTIFYHUB_API_KEY=$${NOTIFYHUB_API_KEY:-loadtest-api-key} -e NOTIFYHUB_CHANNEL=$${NOTIFYHUB_CHANNEL:-inapp} -e NOTIFYHUB_RECIPIENT_PREFIX=$${NOTIFYHUB_RECIPIENT_PREFIX:-k6-user} -e NOTIFYHUB_IDEMPOTENCY=$${NOTIFYHUB_IDEMPOTENCY:-false} -e K6_SCENARIO=$${K6_SCENARIO:-baseline} grafana/k6:latest run /scripts/send_notification.js

loadtest-k6-docker-smoke:
	K6_SCENARIO=smoke $(MAKE) loadtest-k6-docker

loadtest-inapp:
	k6 run scripts/loadtest/inapp_realtime.js

loadtest-inapp-smoke:
	K6_SCENARIO=smoke k6 run scripts/loadtest/inapp_realtime.js

loadtest-inapp-prod1000:
	K6_SCENARIO=prod_1000 k6 run scripts/loadtest/inapp_realtime.js

loadtest-inapp-docker:
	docker run --rm -v "$(PWD)/scripts/loadtest:/scripts" -e NOTIFYHUB_BASE_URL=$${NOTIFYHUB_BASE_URL:-http://host.docker.internal:8080} -e NOTIFYHUB_WS_URL=$${NOTIFYHUB_WS_URL:-ws://host.docker.internal:8080} -e NOTIFYHUB_API_KEY=$${NOTIFYHUB_API_KEY:-loadtest-api-key} -e NOTIFYHUB_RECIPIENT_PREFIX=$${NOTIFYHUB_RECIPIENT_PREFIX:-k6-inapp-user} -e NOTIFYHUB_CONNECT_JITTER_MS=$${NOTIFYHUB_CONNECT_JITTER_MS:-1000} -e NOTIFYHUB_FIRST_SEND_DELAY_MS=$${NOTIFYHUB_FIRST_SEND_DELAY_MS:-1000} -e NOTIFYHUB_SEND_INTERVAL_MS=$${NOTIFYHUB_SEND_INTERVAL_MS:-5000} -e NOTIFYHUB_SENDS_PER_CLIENT=$${NOTIFYHUB_SENDS_PER_CLIENT:-1} -e K6_SCENARIO=$${K6_SCENARIO:-smoke} grafana/k6:latest run /scripts/inapp_realtime.js

loadtest-inapp-docker-smoke:
	K6_SCENARIO=smoke $(MAKE) loadtest-inapp-docker

loadtest-inapp-docker-prod1000:
	K6_SCENARIO=prod_1000 $(MAKE) loadtest-inapp-docker

# ── Helpers ────────────────────────────────────────────────────────────────────

tidy:
	go mod tidy

vet:
	go vet ./...

.DEFAULT_GOAL := build
