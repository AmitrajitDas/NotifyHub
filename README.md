# NotifyHub

Production-grade, multi-channel notification system built in Go. Supports **email**, **SMS**, **push** (FCM), and **in-app** channels with real-time WebSocket delivery. Designed for both single-tenant and multi-tenant deployments from the same codebase.

## Features

- **Multi-channel delivery** — Email (AWS SES), SMS (Twilio), Push (FCM), In-app (WebSocket + Redis pub/sub)
- **Multi-tenancy** — every resource scoped by `tenant_id`; API key auth per tenant
- **Kafka-backed queue** — per-channel topics with per-recipient ordering; worker pool with configurable concurrency
- **Idempotency** — duplicate suppression per tenant via idempotency key
- **Scheduled notifications** — `scheduled_at` support; scheduler worker enqueues at the right time
- **Templates** — Go `text/template` rendering with per-channel file-based templates
- **User preferences** — enabled/disabled per channel, quiet hours (IANA timezone), frequency caps
- **Rate limiting** — Redis sliding-window; per-recipient per-channel + per-API-client HTTP limits
- **Dead-letter queue** — poison messages routed to per-channel DLQ topics; persisted to DB; admin replay/delete endpoints
- **Observability** — structured logging (`slog`), distributed tracing (OTel → Tempo), metrics (Prometheus → Grafana)
- **Webhook delivery** — outbound webhooks for notification events
- **Circuit breaker** — `sony/gobreaker` on provider calls

## Tech Stack

| Concern | Choice |
|---|---|
| Language | Go 1.25 |
| Router | go-chi/chi v5 |
| Database | PostgreSQL 16 (pgx/v5 + sqlc, no ORM) |
| Cache / Rate limit | Redis 7 |
| Message queue | Apache Kafka (KRaft, no Zookeeper) |
| Observability | slog + OpenTelemetry + Prometheus |
| Email | AWS SES v2 |
| SMS | Twilio |
| Push | Firebase FCM |
| WebSocket | coder/websocket |

## Architecture

```
domain  ←  service  ←  handler (HTTP)
  ↑            ↓
  └──── repository ←── db (sqlc-generated)
              ↓
           queue (Kafka)
```

The worker side runs independent per-channel `worker.Pool`s. Each message goes through a 7-step pipeline: fetch → mark processing → preference check → rate-limit check → template render → provider send (with retry/backoff) → write delivery log.

Offsets are committed only after successful processing — infrastructure errors leave messages unconsumed for reprocessing.

## Quick Start

### Prerequisites

- Docker + Docker Compose
- Go 1.25+
- `make`

### Run full local stack

```bash
make docker-up
```

Starts PostgreSQL, Redis, Kafka, the API server, and the worker. Grafana/Loki/Tempo/Prometheus observability stack is also included.

### Run infra only (develop locally)

```bash
make docker-up-infra   # start postgres, redis, kafka
make migrate-up        # apply DB migrations
make seed              # provision test tenant + API key + starter templates
make run-api           # start API server (default :8080)
make run-worker        # start worker
```

The seed script prints the `X-API-Key` value to use in requests.

## API

### Send a notification

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "X-API-Key: <key>" \
  -H "Content-Type: application/json" \
  -d '{
    "recipient_id": "user-123",
    "channel": "email",
    "template_id": "welcome",
    "payload": {"name": "Alice"}
  }'
```

Returns `202 Accepted`. Delivery is async.

### In-app inbox

```bash
# List inbox messages
GET /api/v1/inbox
X-API-Key: <key>
X-Recipient-ID: user-123

# WebSocket stream (real-time)
GET /api/v1/inbox/stream
Authorization: Bearer <ws-jwt>
```

### Admin / internal

| Method | Path | Description |
|---|---|---|
| `POST` | `/internal/tenants` | Provision tenant |
| `POST` | `/internal/tenants/:id/api-keys` | Issue API key |
| `GET` | `/internal/dlq` | List DLQ messages |
| `POST` | `/internal/dlq/:id/replay` | Replay DLQ message |
| `DELETE` | `/internal/dlq/:id` | Delete DLQ message |

Protected by `X-Admin-Token` header.

Full OpenAPI spec: [`docs/api.yaml`](docs/api.yaml)

## Development

```bash
make build              # build api + worker → bin/
make test               # unit tests with race detector + coverage
make test-integration   # integration tests (testcontainers)
make lint               # golangci-lint
make generate           # regenerate sqlc queries (internal/db/)
make migrate-up         # apply migrations
make migrate-down       # roll back one migration
```

### Adding a channel provider

1. Create `internal/provider/{name}/{name}.go` implementing the `provider.Provider` interface (`Send`, `Channel`)
2. Return `provider.ErrDeliveryTemporary` for transient failures, `provider.ErrDeliveryPermanent` for terminal failures
3. Register in `cmd/worker/main.go` via `registry.Register(provider)`

### Modifying DB queries

1. Edit SQL in `queries/*.sql`
2. Run `make generate`
3. Update the repository wrapper in `internal/repository/`

## Load Testing

k6 script at [`scripts/loadtest/send_notification.js`](scripts/loadtest/send_notification.js). See [`docs/load-testing.md`](docs/load-testing.md) for results and methodology.

## Observability

| Signal | Stack |
|---|---|
| Logs | slog → Grafana Alloy → Loki |
| Traces | OTel SDK → OTel Collector → Tempo |
| Metrics | Prometheus → Grafana |

Grafana dashboards provisioned automatically via `deployments/grafana/provisioning/`.

## Docs

- [`docs/api.yaml`](docs/api.yaml) — OpenAPI 3.0 spec
- [`docs/db-schema.md`](docs/db-schema.md) — database schema reference
- [`docs/deployment.md`](docs/deployment.md) — deployment guide
- [`docs/production-grade.md`](docs/production-grade.md) — production readiness notes
- [`docs/adr/`](docs/adr/) — architecture decision records (Go, Kafka, pgx, sqlc)
- [`docs/incident-story.md`](docs/incident-story.md) — fictional on-call incident walkthrough

## Project Structure

```
cmd/
  api/        # API server entrypoint
  worker/     # Worker entrypoint
internal/
  api/        # HTTP handlers, middleware, router
  auth/       # JWT utilities
  config/     # Config loading
  db/         # sqlc-generated queries (do not edit)
  domain/     # Pure Go types and errors (no external imports)
  observability/  # Metrics, tracing, logging, health
  provider/   # Channel providers (ses, twilio, fcm, inapp, mock)
  queue/      # Kafka producer + consumer
  realtime/   # WebSocket hub + Redis pub/sub fan-out
  repository/ # DB access wrappers
  service/    # Business logic
  template/   # Template file loader
  worker/     # Pool, processor, DLQ consumer, scheduler
scripts/
  seed.go         # Provision local tenant + API key
  loadtest/       # k6 load test scripts
deployments/      # Dockerfile, docker-compose, OTel/Loki/Tempo/Prometheus configs
docs/             # API spec, ADRs, deployment docs
queries/          # Raw SQL (input to sqlc)
migrations/       # golang-migrate SQL migrations
```
