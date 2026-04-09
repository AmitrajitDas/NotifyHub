# NotifyHub

Production-grade, multi-channel notification system for general-purpose use cases (user alerts, status updates, reminders, real-time events). Supports both **single-tenant** and **multi-tenant** deployments from the same codebase.

## Tech Stack

- **Language:** Go 1.22+ (use stdlib where possible)
- **Router:** `go-chi/chi/v5`
- **Database:** PostgreSQL 16 via `jackc/pgx/v5` — NO ORM. Use `sqlc` for type-safe queries, `golang-migrate` for migrations.
- **Cache / Rate Limiting:** Redis 7 via `redis/go-redis/v9`
- **Message Queue:** Apache Kafka (KRaft mode, no Zookeeper) via `segmentio/kafka-go`
- **Observability:** `log/slog` (logging), OpenTelemetry (tracing), Prometheus (metrics)
- **Circuit Breaker:** `sony/gobreaker`
- **Testing:** `go test` + `testcontainers-go` for integration tests
- **Validation:** `go-playground/validator/v10`

## Multi-Tenancy

- Every resource (`tenants`, `api_clients`, `templates`, `preferences`, `notifications`) is scoped by `tenant_id`.
- Auth middleware resolves the API key to an `APIClient` (which carries `TenantID`) and stores it in `context.Context`.
- All repository methods accept `tenantID uuid.UUID` and filter on it — no cross-tenant data leaks.
- Redis keys are namespaced: `ratelimit:{tenant_id}:{user_id}:{channel}`, `idempotency:{tenant_id}:{key}`.
- Kafka messages carry `tenant_id` in the payload.
- **Single-tenant mode** (`TENANT_MODE=single`): seed one default tenant; callers never pass `tenant_id` explicitly — the server resolves it automatically.
- Tenant provisioning is not in the public API; use `scripts/seed.go` or the restricted `/internal/tenants` endpoint (protected by `X-Admin-Token`).

## Design Principles

1. **Dependency injection everywhere** — no global state, all deps passed via constructors
2. **Interface-based design** — repositories, providers, queue ops behind interfaces
3. **Context propagation** — pass `context.Context` through every layer
4. **Graceful shutdown** — handle SIGINT/SIGTERM, drain queues, wait for in-flight work
5. **Idempotency** — every notification safely retryable without duplicates (scoped per tenant)
6. **Structured errors** — wrap with `fmt.Errorf("...: %w", err)`
7. **No magic** — explicit config, no auto-wiring frameworks
8. **Test at boundaries** — unit test business logic, integration test with real infra via testcontainers

## Common Commands

```bash
make build              # Build api + worker binaries
make test               # Run unit tests with race detector
make test-integration   # Run integration tests (spins up containers)
make lint               # Run golangci-lint
make docker-up          # Start full local stack (postgres, redis, kafka, etc.)
make docker-down        # Stop local stack
make migrate-up         # Apply database migrations
make migrate-down       # Roll back one migration
make generate           # Run sqlc generate
make run-api            # Run API server locally
make run-worker         # Run worker locally
```

## Project Layout

- `cmd/api/` — API server entrypoint
- `cmd/worker/` — Worker process entrypoint
- `internal/domain/` — Core domain types (no external deps)
- `internal/api/` — HTTP handlers, middleware, router, response helpers
- `internal/service/` — Business logic layer
- `internal/repository/` — Database access layer (wraps sqlc)
- `internal/queue/` — Kafka producer/consumer abstraction
- `internal/worker/` — Channel worker pool
- `internal/provider/` — External delivery providers (SES, FCM, Twilio)
- `internal/observability/` — Metrics, tracing, health checks
- `internal/config/` — Environment-based configuration
- `migrations/` — SQL migration files (golang-migrate format)
- `queries/` — sqlc query definitions
- `templates/` — Notification templates (Go html/text template)
- `deployments/` — Docker Compose, Dockerfile, Prometheus config
- `docs/` — Architecture docs, ADRs, OpenAPI spec
- `scripts/` — Seed data, load tests

## Reference

Full specification: `NOTIFICATION_SYSTEM_PLAN.md`
