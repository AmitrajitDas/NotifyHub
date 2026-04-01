# NotifyHub — Production-Grade Notification System

## Project Overview

Build a production-grade, multi-channel notification system in Go that handles push, email, SMS, and in-app notifications with templating, user preferences, rate limiting, scheduling, retries, and observability. The system should be designed as a real production backend service — clean architecture, proper error handling, structured logging, and full test coverage.

**Domain context:** Frame this around an insurance-tech domain (policy renewals, claim status updates, field agent alerts, payment reminders) to give it realistic business context.

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Language | Go 1.22+ | Use standard library where possible |
| HTTP Router | chi (go-chi/chi/v5) | Lightweight, idiomatic |
| Message Queue | Apache Kafka (KRaft mode, no Zookeeper) | Use `segmentio/kafka-go` client |
| Primary DB | PostgreSQL 16 | Use `jackc/pgx/v5` driver directly, NO ORM |
| Query Gen | sqlc | Type-safe SQL queries from raw SQL |
| Migrations | golang-migrate/migrate | SQL-based migrations |
| Cache / Rate Limit | Redis 7 | Use `redis/go-redis/v9` |
| Template Engine | Go `html/template` + `text/template` | For email HTML and SMS/push text |
| Email Provider | AWS SES (or Mailgun as fallback) | Use official SDK |
| Push Provider | Firebase Cloud Messaging (FCM) | Use `firebase.google.com/go/v4` |
| SMS Provider | Twilio | Use `twilio/twilio-go` |
| Containerization | Docker + Docker Compose | Multi-stage builds, scratch-based final image |
| Observability | OpenTelemetry (tracing), Prometheus (metrics), slog (logging) | Use Go's built-in `log/slog` |
| Circuit Breaker | sony/gobreaker | For provider outage resilience |
| API Docs | OpenAPI 3.0 | Generate with `swaggo/swag` |
| Testing | Go testing + testcontainers-go | Integration tests with real Postgres/Redis/Kafka |
| CI | GitHub Actions | Lint, test, build, docker push |
| Load Testing | k6 | Performance benchmarks |

---

## Project Structure

```
notifyhub/
├── cmd/
│   ├── api/                    # API server entrypoint
│   │   └── main.go
│   └── worker/                 # Worker process entrypoint
│       └── main.go
├── internal/
│   ├── config/                 # Configuration loading (env, YAML)
│   │   └── config.go
│   ├── domain/                 # Core domain types (no external deps)
│   │   ├── notification.go     # Notification entity
│   │   ├── template.go         # Template entity
│   │   ├── preference.go       # User preference entity
│   │   └── delivery.go         # Delivery log entity
│   ├── api/                    # HTTP handlers
│   │   ├── router.go           # Route definitions
│   │   ├── middleware/         
│   │   │   ├── auth.go         # API key authentication
│   │   │   ├── ratelimit.go    # Request rate limiting
│   │   │   └── requestid.go    # Request ID injection
│   │   ├── handler/
│   │   │   ├── notification.go # Send/query notifications
│   │   │   ├── template.go     # CRUD templates
│   │   │   └── preference.go   # CRUD user preferences
│   │   └── response/           # Standard API response helpers
│   │       └── response.go
│   ├── queue/                  # Message queue abstraction
│   │   ├── publisher.go        # Produce messages to Kafka topics
│   │   ├── consumer.go         # Kafka consumer group reader
│   │   └── kafka.go            # Kafka connection & topic management
│   ├── worker/                 # Channel workers
│   │   ├── pool.go             # Worker pool with goroutines
│   │   ├── email.go            # Email delivery worker
│   │   ├── push.go             # Push notification worker
│   │   ├── sms.go              # SMS delivery worker
│   │   └── inapp.go            # In-app (WebSocket/SSE) worker
│   ├── provider/               # External delivery providers
│   │   ├── provider.go         # Provider interface
│   │   ├── ses/                # AWS SES implementation
│   │   │   └── ses.go
│   │   ├── fcm/                # Firebase Cloud Messaging
│   │   │   └── fcm.go
│   │   ├── twilio/             # Twilio SMS
│   │   │   └── twilio.go
│   │   └── mock/               # Mock provider for testing
│   │       └── mock.go
│   ├── repository/             # Database access layer
│   │   ├── notification.go
│   │   ├── template.go
│   │   ├── preference.go
│   │   └── delivery.go
│   ├── service/                # Business logic layer
│   │   ├── notification.go     # Orchestrates send flow
│   │   ├── template.go         # Template rendering
│   │   ├── preference.go       # Preference checking
│   │   ├── ratelimit.go        # Per-user rate limiting
│   │   └── scheduler.go        # Delayed/scheduled sends
│   └── observability/          # Metrics, tracing, health
│       ├── metrics.go          # Prometheus metrics
│       ├── tracing.go          # OpenTelemetry setup
│       └── health.go           # Health check endpoints
├── migrations/                 # SQL migration files
│   ├── 000001_init.up.sql
│   └── 000001_init.down.sql
├── queries/                    # sqlc query definitions
│   ├── notification.sql
│   ├── template.sql
│   ├── preference.sql
│   └── delivery.sql
├── templates/                  # Notification templates (Handlebars/Go tmpl)
│   ├── email/
│   │   ├── claim_status.html
│   │   ├── policy_renewal.html
│   │   └── payment_reminder.html
│   └── sms/
│       ├── claim_status.txt
│       └── otp.txt
├── docs/
│   ├── architecture.md         # Architecture overview with diagrams
│   ├── adr/                    # Architecture Decision Records
│   │   ├── 001-go-over-node.md
│   │   ├── 002-kafka-over-rabbitmq.md
│   │   ├── 003-pgx-over-gorm.md
│   │   └── 004-sqlc-for-queries.md
│   └── api.yaml                # OpenAPI spec
├── deployments/
│   ├── docker-compose.yml      # Full local stack
│   ├── docker-compose.test.yml # Test stack
│   └── Dockerfile              # Multi-stage Go build
├── scripts/
│   ├── seed.go                 # Seed test data
│   └── loadtest/               # k6 load test scripts
│       └── send_notification.js
├── sqlc.yaml                   # sqlc config
├── .golangci.yml               # Linter config
├── .github/
│   └── workflows/
│       ├── ci.yml              # Lint + test + build
│       └── docker.yml          # Build and push image
├── Makefile                    # Common commands
├── go.mod
├── go.sum
└── README.md
```

---

## Database Schema (PostgreSQL)

Create these as the initial migration `000001_init.up.sql`:

```sql
-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- API clients
CREATE TABLE api_clients (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(255) NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Notification templates
CREATE TABLE templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    channel VARCHAR(50) NOT NULL, -- 'email', 'push', 'sms', 'inapp'
    subject_template TEXT, -- for email
    body_template TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    version INT NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_templates_channel ON templates(channel);
CREATE INDEX idx_templates_name ON templates(name);

-- User notification preferences
CREATE TABLE preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(255) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    quiet_hours_start TIME, -- e.g., '22:00'
    quiet_hours_end TIME,   -- e.g., '08:00'
    frequency_cap INT, -- max notifications per window
    frequency_window_minutes INT DEFAULT 60,
    timezone VARCHAR(100) DEFAULT 'UTC',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, channel)
);

CREATE INDEX idx_preferences_user ON preferences(user_id);

-- Notifications (the core table)
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) UNIQUE, -- prevent duplicates
    type VARCHAR(100) NOT NULL, -- 'claim_status', 'policy_renewal', etc.
    channel VARCHAR(50) NOT NULL,
    recipient_id VARCHAR(255) NOT NULL,
    recipient_address VARCHAR(500), -- email/phone/device_token
    template_id UUID REFERENCES templates(id),
    payload JSONB NOT NULL DEFAULT '{}', -- template variables
    priority INT NOT NULL DEFAULT 5, -- 1 (highest) to 10 (lowest)
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
        -- pending -> queued -> processing -> delivered / failed / dropped
    scheduled_at TIMESTAMPTZ, -- NULL = send immediately
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_recipient ON notifications(recipient_id);
CREATE INDEX idx_notifications_scheduled ON notifications(scheduled_at) WHERE scheduled_at IS NOT NULL AND status = 'pending';
CREATE INDEX idx_notifications_idempotency ON notifications(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_notifications_created ON notifications(created_at);

-- Delivery attempts log
CREATE TABLE delivery_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID NOT NULL REFERENCES notifications(id),
    channel VARCHAR(50) NOT NULL,
    provider VARCHAR(100) NOT NULL, -- 'ses', 'fcm', 'twilio'
    status VARCHAR(50) NOT NULL, -- 'success', 'failed', 'bounced', 'rejected'
    provider_message_id VARCHAR(500),
    error_message TEXT,
    error_code VARCHAR(100),
    retry_count INT NOT NULL DEFAULT 0,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_delivery_notification ON delivery_logs(notification_id);
CREATE INDEX idx_delivery_status ON delivery_logs(status);
CREATE INDEX idx_delivery_attempted ON delivery_logs(attempted_at);
```

---

## Core Domain Types

### internal/domain/notification.go
```go
package domain

import (
    "time"
    "github.com/google/uuid"
)

type Channel string

const (
    ChannelEmail Channel = "email"
    ChannelPush  Channel = "push"
    ChannelSMS   Channel = "sms"
    ChannelInApp Channel = "inapp"
)

type NotificationStatus string

const (
    StatusPending    NotificationStatus = "pending"
    StatusQueued     NotificationStatus = "queued"
    StatusProcessing NotificationStatus = "processing"
    StatusDelivered  NotificationStatus = "delivered"
    StatusFailed     NotificationStatus = "failed"
    StatusDropped    NotificationStatus = "dropped" // rate-limited or preference-blocked
)

type Priority int

const (
    PriorityCritical Priority = 1
    PriorityHigh     Priority = 3
    PriorityNormal   Priority = 5
    PriorityLow      Priority = 7
    PriorityBulk     Priority = 10
)

type Notification struct {
    ID               uuid.UUID          `json:"id"`
    IdempotencyKey   *string            `json:"idempotency_key,omitempty"`
    Type             string             `json:"type"`
    Channel          Channel            `json:"channel"`
    RecipientID      string             `json:"recipient_id"`
    RecipientAddress string             `json:"recipient_address"`
    TemplateID       *uuid.UUID         `json:"template_id,omitempty"`
    Payload          map[string]any     `json:"payload"`
    Priority         Priority           `json:"priority"`
    Status           NotificationStatus `json:"status"`
    ScheduledAt      *time.Time         `json:"scheduled_at,omitempty"`
    CreatedAt        time.Time          `json:"created_at"`
    UpdatedAt        time.Time          `json:"updated_at"`
}

// SendRequest is the inbound API request
type SendRequest struct {
    Type             string         `json:"type" validate:"required"`
    Channel          Channel        `json:"channel" validate:"required,oneof=email push sms inapp"`
    RecipientID      string         `json:"recipient_id" validate:"required"`
    RecipientAddress string         `json:"recipient_address" validate:"required"`
    TemplateID       *uuid.UUID     `json:"template_id,omitempty"`
    Payload          map[string]any `json:"payload"`
    Priority         Priority       `json:"priority" validate:"min=1,max=10"`
    IdempotencyKey   *string        `json:"idempotency_key,omitempty"`
    ScheduledAt      *time.Time     `json:"scheduled_at,omitempty"`
}

// BulkSendRequest allows sending to multiple recipients
type BulkSendRequest struct {
    Type       string         `json:"type" validate:"required"`
    Channel    Channel        `json:"channel" validate:"required"`
    TemplateID *uuid.UUID     `json:"template_id,omitempty"`
    Payload    map[string]any `json:"payload"`
    Priority   Priority       `json:"priority"`
    Recipients []Recipient    `json:"recipients" validate:"required,min=1,max=1000"`
}

type Recipient struct {
    RecipientID      string         `json:"recipient_id" validate:"required"`
    RecipientAddress string         `json:"recipient_address" validate:"required"`
    Payload          map[string]any `json:"payload,omitempty"` // per-recipient overrides
}
```

---

## API Endpoints

### Notifications
```
POST   /api/v1/notifications            # Send single notification
POST   /api/v1/notifications/bulk       # Send bulk notifications (up to 1000)
GET    /api/v1/notifications/:id        # Get notification by ID
GET    /api/v1/notifications            # List notifications (paginated, filterable)
DELETE /api/v1/notifications/:id        # Cancel a pending/scheduled notification
```

### Templates
```
POST   /api/v1/templates               # Create template
GET    /api/v1/templates               # List templates
GET    /api/v1/templates/:id           # Get template
PUT    /api/v1/templates/:id           # Update template (creates new version)
DELETE /api/v1/templates/:id           # Soft delete template
POST   /api/v1/templates/:id/preview   # Preview rendered template with sample payload
```

### Preferences
```
GET    /api/v1/users/:user_id/preferences           # Get all preferences for user
PUT    /api/v1/users/:user_id/preferences/:channel   # Upsert preference for channel
DELETE /api/v1/users/:user_id/preferences/:channel   # Delete preference (revert to defaults)
```

### System
```
GET    /health                          # Health check (DB, Redis, Kafka)
GET    /ready                           # Readiness probe
GET    /metrics                         # Prometheus metrics endpoint
```

All endpoints require `X-API-Key` header. Responses follow a standard envelope:

```json
{
    "success": true,
    "data": { ... },
    "meta": {
        "page": 1,
        "per_page": 20,
        "total": 150,
        "request_id": "req_abc123"
    }
}
```

Error responses:
```json
{
    "success": false,
    "error": {
        "code": "RATE_LIMITED",
        "message": "User has exceeded notification limit for this channel",
        "details": { "retry_after_seconds": 3600 }
    },
    "meta": { "request_id": "req_abc123" }
}
```

---

## Message Queue Design (Kafka)

### Topic Architecture

```
Topics (created at startup with admin client):

  notifyhub.notifications.email     — 6 partitions, replication-factor 1 (dev) / 3 (prod)
  notifyhub.notifications.push      — 6 partitions
  notifyhub.notifications.sms       — 3 partitions
  notifyhub.notifications.inapp     — 6 partitions

  notifyhub.retry                   — 3 partitions (retry with delay)
  notifyhub.dlq                     — 1 partition  (permanently failed messages)

Partition key: recipient_id (ensures ordering per-user within a channel)
```

### Consumer Groups

```
Consumer Group: notifyhub-email-workers
  → reads from: notifyhub.notifications.email
  → instances auto-balance partitions via Kafka consumer group protocol

Consumer Group: notifyhub-push-workers
  → reads from: notifyhub.notifications.push

Consumer Group: notifyhub-sms-workers
  → reads from: notifyhub.notifications.sms

Consumer Group: notifyhub-inapp-workers
  → reads from: notifyhub.notifications.inapp

Consumer Group: notifyhub-retry-processor
  → reads from: notifyhub.retry
  → checks if delay has elapsed, re-publishes to original topic or moves to DLQ
```

### Kafka Producer Config

```go
// Use segmentio/kafka-go
writer := &kafka.Writer{
    Addr:         kafka.TCP("localhost:9092"),
    Balancer:     &kafka.Hash{},          // partition by key (recipient_id)
    RequiredAcks: kafka.RequireAll,       // wait for all ISR replicas
    Async:        false,                  // synchronous writes for reliability
    Compression:  kafka.Snappy,
    BatchTimeout: 10 * time.Millisecond,  // low latency
}
```

### Kafka Consumer Config

```go
// One reader per channel, using consumer groups
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers:        []string{"localhost:9092"},
    Topic:          "notifyhub.notifications.email",
    GroupID:        "notifyhub-email-workers",
    MinBytes:       1,
    MaxBytes:       10e6,                   // 10MB
    MaxWait:        500 * time.Millisecond, // poll interval
    CommitInterval: time.Second,            // auto-commit offset every 1s
    StartOffset:    kafka.LastOffset,
})
```

### Message Format

```json
{
    "notification_id": "uuid",
    "channel": "email",
    "recipient_id": "user_123",
    "recipient_address": "user@example.com",
    "template_id": "uuid",
    "payload": { "claim_id": "CLM-001", "status": "approved" },
    "priority": 5,
    "attempt": 1,
    "max_attempts": 3,
    "enqueued_at": "2025-01-01T00:00:00Z",
    "next_retry_at": null
}
```

Kafka message key = `recipient_id` (for partition affinity and per-user ordering).

### Retry Strategy (Kafka-native)

Since Kafka has no native delayed delivery, retries use a dedicated retry topic with a poller:

```
Failed message:
  1. Increment attempt count
  2. Calculate next_retry_at = now + (base_delay * 2^attempt) + jitter
  3. Publish to notifyhub.retry topic with next_retry_at in payload

Retry processor (polling notifyhub.retry):
  1. Read message
  2. If next_retry_at > now → pause briefly, re-check (or use a time-bucketed approach)
  3. If next_retry_at <= now AND attempt < max_attempts → re-publish to original channel topic
  4. If attempt >= max_attempts → publish to notifyhub.dlq

Alternative approach (simpler): Use multiple retry topics with different consumer poll intervals:
  notifyhub.retry.1m   — consumer polls every 1 minute
  notifyhub.retry.5m   — consumer polls every 5 minutes
  notifyhub.retry.30m  — consumer polls every 30 minutes
  After 30m retry fails → move to DLQ
```

---

## Worker Pool Design

### internal/worker/pool.go

The worker pool should:

1. Accept a configurable concurrency level per channel (e.g., 5 email workers, 3 push workers)
2. Each channel spawns a Kafka consumer group reader in a dedicated goroutine
3. Messages are fanned out to a pool of goroutine workers via a buffered channel (semaphore pattern)
4. Offset commits happen AFTER successful processing (at-least-once delivery)
5. Support graceful shutdown: stop fetching, drain in-flight messages, commit final offsets, then exit
6. Track per-worker metrics (messages processed, errors, latency, consumer lag)

### Processing Pipeline (per message)

```
Kafka consumer fetches message
  -> Deserialize message
  -> Check idempotency (Redis SET NX with TTL)
  -> Check user preferences (is channel enabled? quiet hours?)
  -> Check rate limit (Redis sliding window)
  -> Render template (merge payload into template)
  -> Send via provider (with circuit breaker)
  -> Log delivery attempt
  -> Update notification status in DB
  -> Commit offset (explicit commit after successful processing)
  -> On failure: publish to retry topic (or DLQ if max attempts exceeded)
```

### Offset Management

```
Strategy: Manual commit after processing (at-least-once semantics)

- Do NOT use auto-commit in the processing path
- Commit offset only after the message is fully processed and delivery logged
- On worker crash, uncommitted messages will be re-delivered (idempotency key prevents duplicates)
- Use CommitMessages() per-message for critical notifications, batch commit for bulk
```

---

## Rate Limiting

### Per-User Channel Rate Limiting (Redis)

Use sliding window log algorithm:

```
Key: ratelimit:{user_id}:{channel}
Algorithm: Sorted set with timestamp scores
  - ZREMRANGEBYSCORE to remove expired entries
  - ZCARD to count current window
  - ZADD to record new notification
Window: Configurable per user via preferences (default 60 min)
Limit: Configurable per user (default varies by channel)

Default limits:
  email: 10 per hour
  push: 5 per hour
  sms: 3 per hour
  inapp: 50 per hour
```

### API Request Rate Limiting (per API key)

Use token bucket in Redis:
```
Key: apilimit:{api_key_hash}
Limit: 100 requests per minute
```

---

## Observability

### Prometheus Metrics

```
# Counters
notifyhub_notifications_total{channel, status, type}
notifyhub_delivery_attempts_total{channel, provider, status}
notifyhub_queue_published_total{channel}
notifyhub_rate_limited_total{channel}

# Histograms
notifyhub_delivery_duration_seconds{channel, provider}
notifyhub_api_request_duration_seconds{method, path, status_code}
notifyhub_template_render_duration_seconds{template}

# Gauges
notifyhub_kafka_consumer_lag{topic, partition, group}
notifyhub_workers_active{channel}
notifyhub_circuit_breaker_state{provider}  # 0=closed, 1=half-open, 2=open
```

### Structured Logging (slog)

Every log line includes:
- `request_id` (from middleware)
- `notification_id` (when processing)
- `channel`
- `recipient_id`
- `trace_id` (OpenTelemetry)

### OpenTelemetry Tracing

Trace spans for:
- API request → publish to queue
- Worker consume → preference check → rate limit → render → deliver
- Provider HTTP calls

---

## Docker Compose (Local Development)

```yaml
services:
  api:
    build:
      context: .
      target: api
    ports: ["8080:8080"]
    depends_on: [postgres, redis, kafka]
    environment:
      DATABASE_URL: postgres://notifyhub:notifyhub@postgres:5432/notifyhub?sslmode=disable
      REDIS_URL: redis://redis:6379
      KAFKA_BROKERS: kafka:29092
      LOG_LEVEL: debug

  worker:
    build:
      context: .
      target: worker
    depends_on: [postgres, redis, kafka]
    environment:
      DATABASE_URL: postgres://notifyhub:notifyhub@postgres:5432/notifyhub?sslmode=disable
      REDIS_URL: redis://redis:6379
      KAFKA_BROKERS: kafka:29092
      WORKER_EMAIL_CONCURRENCY: 5
      WORKER_PUSH_CONCURRENCY: 3
      WORKER_SMS_CONCURRENCY: 2
      LOG_LEVEL: debug

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: notifyhub
      POSTGRES_USER: notifyhub
      POSTGRES_PASSWORD: notifyhub
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    ports:
      - "9092:9092"    # host access
      - "29092:29092"  # inter-container access
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes: [kafkadata:/tmp/kraft-combined-logs]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports: ["8090:8080"]
    depends_on: [kafka]
    environment:
      KAFKA_CLUSTERS_0_NAME: notifyhub-local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  prometheus:
    image: prom/prometheus:latest
    ports: ["9090:9090"]
    volumes: [./deployments/prometheus.yml:/etc/prometheus/prometheus.yml]

  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    depends_on: [prometheus]

volumes:
  pgdata:
  kafkadata:
```

---

## Dockerfile (Multi-stage)

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /worker ./cmd/worker

FROM scratch AS api
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /api /api
COPY templates/ /templates/
ENTRYPOINT ["/api"]

FROM scratch AS worker
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /worker /worker
COPY templates/ /templates/
ENTRYPOINT ["/worker"]
```

---

## Makefile

```makefile
.PHONY: all build test lint run-api run-worker migrate docker-up docker-down seed generate

all: lint test build

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

test:
	go test ./... -v -race -cover

test-integration:
	docker compose -f deployments/docker-compose.test.yml up -d
	go test ./... -v -tags=integration -race
	docker compose -f deployments/docker-compose.test.yml down

lint:
	golangci-lint run ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

generate:
	sqlc generate

docker-up:
	docker compose -f deployments/docker-compose.yml up -d

docker-down:
	docker compose -f deployments/docker-compose.yml down

seed:
	go run ./scripts/seed.go
```

---

## Implementation Order

### Phase 1: Foundation
1. Initialize Go module: `go mod init github.com/<your-username>/notifyhub`
2. Create the full directory structure as specified above
3. Set up config loading from environment variables using `internal/config/config.go`
4. Write the database migration file (`000001_init.up.sql` and down)
5. Set up sqlc config and write the query files for all 4 tables
6. Run `sqlc generate` to produce type-safe Go code
7. Implement the repository layer using generated sqlc code
8. Write domain types in `internal/domain/`
9. Set up Docker Compose with Postgres, Redis, Kafka (KRaft mode, no Zookeeper)
10. Verify migrations run cleanly

### Phase 2: API Layer
1. Set up chi router with middleware chain: request ID → logging → recovery → auth → rate limit
2. Implement API key auth middleware (hash incoming key, compare to DB)
3. Build notification handler: POST to send, GET to query
4. Build template CRUD handlers
5. Build preference CRUD handlers
6. Implement standard response envelope and error types
7. Add request validation using `go-playground/validator`
8. Write unit tests for all handlers using httptest
9. Verify full CRUD flow works end-to-end with curl/httpie

### Phase 3: Queue & Workers
1. Implement Kafka connection manager using `segmentio/kafka-go` (writer for producing, reader for consuming)
2. Create topic initialization logic: auto-create topics on startup with correct partition counts using kafka-go admin client
3. Implement producer: serialize notification → write to channel-specific topic with recipient_id as partition key
4. Wire API handler to produce to Kafka after persisting notification
5. Build the worker pool: each channel gets a consumer group reader, fanning messages to a goroutine pool
6. Implement manual offset commit after successful processing (at-least-once delivery)
7. Build email worker first (easiest to verify) with mock provider
8. Add graceful shutdown: stop consumer, drain in-flight goroutines, commit final offsets
9. Implement retry topic flow: failed messages → notifyhub.retry → retry processor re-publishes or moves to DLQ
10. Write integration test: send via API → verify consumed by worker → verify delivery log

### Phase 4: Delivery Providers
1. Define the `Provider` interface in `internal/provider/provider.go`
2. Implement mock provider (always succeeds, records calls)
3. Implement SES email provider
4. Implement FCM push provider
5. Implement Twilio SMS provider
6. Add circuit breaker wrapper around each provider
7. Wire providers into workers via dependency injection
8. Test with real credentials (use environment-based provider selection)

### Phase 5: Intelligence Layer
1. Implement user preference checking in the worker pipeline
2. Implement quiet hours logic (check user timezone, compare current time)
3. Implement rate limiting service using Redis sliding window
4. Add idempotency check (Redis SETNX with notification idempotency key)
5. Implement retry logic with exponential backoff + jitter via retry topic (see Message Queue Design)
6. Set up DLQ topic consumer for permanently failed message inspection and alerting
7. Implement the scheduler: periodic job that queries for due scheduled notifications and enqueues them
8. Write tests for each intelligence feature in isolation

### Phase 6: Observability
1. Add Prometheus metrics registry and instrument all key paths
2. Add `/metrics` endpoint
3. Set up OpenTelemetry tracer provider
4. Add trace spans to API handlers, queue operations, and provider calls
5. Ensure slog is used everywhere with consistent field names
6. Add `/health` and `/ready` endpoints (check DB, Redis, Kafka broker connectivity)
7. Create Prometheus scrape config
8. Create a basic Grafana dashboard JSON (notification throughput, latency, error rates, Kafka consumer lag per group)

### Phase 7: Hardening & Documentation
1. Write integration tests using testcontainers-go (spin up real Postgres, Redis, Kafka)
2. Add golangci-lint config with strict rules
3. Set up GitHub Actions CI: lint → test → build → docker build
4. Write k6 load test script for the send endpoint
5. Write Architecture Decision Records (ADRs) for key choices
6. Write a proper README with: overview, architecture diagram (Mermaid), setup instructions, API examples
7. Generate OpenAPI spec from handler annotations
8. Add a CONTRIBUTING.md with dev setup guide

---

## React Native Integration (FCM + WebSocket)

### Overview

The system supports two channels for mobile clients:

| Channel | Mechanism | Use case |
|---|---|---|
| `push` | FCM → Firebase → device | Background / killed-app alerts |
| `inapp` | WebSocket + Redis pub/sub | Foreground, real-time in-app feed |

```
React Native App
  ├── Push (FCM)
  │     RN calls messaging().getToken()
  │     → POST /api/v1/device-tokens  (register token)
  │     → Notification API → Kafka push topic → push worker → FCM provider → Firebase → device
  │
  └── In-App (WebSocket)
        RN connects  wss://api/ws?token=<short-lived-jwt>
        → Notification API → Kafka inapp topic → inapp worker → Redis pub/sub → WS hub → RN
```

---

### Phase 8: FCM Push Provider

**New table — `device_tokens`**

```sql
CREATE TABLE device_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(255) NOT NULL,
    token TEXT NOT NULL UNIQUE,
    platform VARCHAR(20) NOT NULL, -- 'ios', 'android'
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_device_tokens_user ON device_tokens(user_id);
```

**New API endpoint**

```
POST /api/v1/device-tokens       # Register/refresh FCM token
DELETE /api/v1/device-tokens/:token  # Deregister on logout
```

**FCM provider** (`internal/provider/fcm/fcm.go`)

- Dependency: `firebase.google.com/go/v4`
- Credentials loaded from `FCM_CREDENTIALS_FILE` env var (service account JSON)
- On `messaging.ErrCodeUnregistered` response from Firebase → mark token inactive in DB
- Send `messaging.Message` with `Notification` (title/body) + `Data` (payload for deep linking)
- Wrap in `gobreaker` circuit breaker

**Push worker** (`internal/worker/push.go`)

```
consume from notifyhub.notifications.push
  -> look up active device tokens for recipient_id
  -> for each token: send via FCM provider (fan-out if multi-device)
  -> log delivery per token
  -> if token unregistered: deactivate in DB
  -> commit offset
```

**New environment variables**

```env
FCM_CREDENTIALS_FILE=/etc/notifyhub/fcm-credentials.json
FCM_PROJECT_ID=your-firebase-project-id
```

**React Native side (reference)**

```ts
import messaging from '@react-native-firebase/messaging';

const token = await messaging().getToken();
await api.post('/api/v1/device-tokens', { token, platform: Platform.OS });

// Handle foreground messages (fallback when WS is disconnected)
messaging().onMessage(async remoteMessage => { /* update local state */ });
```

---

### Phase 9: WebSocket Hub (In-App)

**Architecture**

```
inapp worker
  → Redis PUBLISH notifyhub:inapp:{recipient_id} <notification JSON>

WS Hub (goroutine per connection)
  ← Redis SUBSCRIBE notifyhub:inapp:{recipient_id}
  → write to client WebSocket
```

**WS Hub** (`internal/api/ws/hub.go`)

- Manages a `sync.Map` of `recipient_id → []conn`
- On connect: authenticate via short-lived JWT (issued by `POST /api/v1/ws-token`)
- On connect: subscribe to Redis channel for that `recipient_id`
- On disconnect: unsubscribe, remove from map
- Ping/pong keepalive every 30s to detect dead connections
- Library: `nhooyr.io/websocket` (stdlib-friendly, no gorilla global state)

**New API endpoints**

```
GET  /api/v1/ws              # WebSocket upgrade endpoint
POST /api/v1/ws-token        # Issue short-lived JWT for WS auth (60s TTL)
GET  /api/v1/notifications/feed  # REST fallback: paginated in-app feed
```

**Inapp worker** (`internal/worker/inapp.go`)

```
consume from notifyhub.notifications.inapp
  -> persist notification to DB (status = delivered)
  -> Redis PUBLISH notifyhub:inapp:{recipient_id} <notification JSON>
  -> commit offset
  (no external HTTP call — Redis pub/sub is the delivery mechanism)
```

**New environment variables**

```env
WS_JWT_SECRET=your-secret-key
WS_JWT_TTL_SECONDS=60
WS_PING_INTERVAL_SECONDS=30
```

**React Native side (reference)**

```ts
// 1. Get short-lived token
const { token } = await api.post('/api/v1/ws-token');

// 2. Connect
const ws = new WebSocket(`wss://api/api/v1/ws?token=${token}`);

ws.onmessage = (e) => {
  const notification = JSON.parse(e.data);
  // dispatch to local state / notification bell
};

// 3. Reconnect with exponential backoff on close
```

**Message format over WebSocket**

```json
{
  "id": "uuid",
  "type": "claim_status",
  "payload": { "claim_id": "CLM-001", "status": "approved" },
  "created_at": "2025-01-01T00:00:00Z"
}
```

---

### Implementation Order for Phases 8–9

1. **Phase 8 — FCM**
   1. Add `device_tokens` migration
   2. Add sqlc queries for device token CRUD
   3. Add `domain.DeviceToken` type
   4. Add device token repository
   5. Implement `POST /api/v1/device-tokens` handler
   6. Implement FCM provider with `firebase.google.com/go/v4`
   7. Implement push worker with multi-device fan-out
   8. Wire circuit breaker around FCM provider
   9. Integration test: register token → send push notification → verify delivery log

2. **Phase 9 — WebSocket**
   1. Add `POST /api/v1/ws-token` (JWT issuance)
   2. Implement WS hub with Redis pub/sub fan-out
   3. Wire `GET /api/v1/ws` upgrade endpoint into chi router
   4. Implement inapp worker (publishes to Redis, no external provider)
   5. Add `GET /api/v1/notifications/feed` REST fallback for history
   6. Integration test: connect WS → send inapp notification → verify message received

---

## Key Design Principles to Follow

1. **Dependency injection everywhere** — no global state, all dependencies passed via constructors
2. **Interface-based design** — repositories, providers, and queue operations all behind interfaces for testability
3. **Context propagation** — pass `context.Context` through every layer for cancellation and tracing
4. **Graceful shutdown** — handle SIGINT/SIGTERM, drain queues, wait for in-flight work
5. **Idempotency** — every notification can be safely retried without duplicate delivery
6. **Structured errors** — wrap errors with context using `fmt.Errorf("...: %w", err)`
7. **No magic** — explicit configuration, no auto-wiring frameworks
8. **Test at boundaries** — unit test business logic, integration test with real infrastructure via testcontainers

---

## Configuration (Environment Variables)

```env
# Server
PORT=8080
LOG_LEVEL=info  # debug, info, warn, error
ENVIRONMENT=development  # development, staging, production

# Database
DATABASE_URL=postgres://notifyhub:notifyhub@localhost:5432/notifyhub?sslmode=disable
DATABASE_MAX_CONNS=25
DATABASE_MIN_CONNS=5

# Redis
REDIS_URL=redis://localhost:6379
REDIS_PASSWORD=

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_PARTITION_COUNT=6
KAFKA_REPLICATION_FACTOR=1

# Worker
WORKER_EMAIL_CONCURRENCY=5
WORKER_PUSH_CONCURRENCY=3
WORKER_SMS_CONCURRENCY=2
WORKER_INAPP_CONCURRENCY=5
WORKER_RETRY_MAX_ATTEMPTS=3
WORKER_RETRY_BASE_DELAY_MS=1000

# Providers
SES_REGION=ap-south-1
SES_FROM_EMAIL=notifications@yourdomain.com
FCM_CREDENTIALS_FILE=/etc/notifyhub/fcm-credentials.json
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_FROM_NUMBER=

# Rate Limiting
RATELIMIT_API_RPM=100
RATELIMIT_EMAIL_PER_HOUR=10
RATELIMIT_PUSH_PER_HOUR=5
RATELIMIT_SMS_PER_HOUR=3
```
