# Load Testing

NotifyHub includes a k6 script for exercising the authenticated send endpoint:

```bash
make docker-up-infra
make migrate-up
go run ./scripts/seed.go -key loadtest-api-key
make run-api
make run-worker
make loadtest-k6-smoke
make loadtest-k6
```

If `k6` is not installed locally, use the Docker-backed targets:

```bash
docker pull grafana/k6:latest
make loadtest-k6-docker-smoke
make loadtest-k6-docker
```

## Configuration

The script reads these environment variables:

- `NOTIFYHUB_BASE_URL` - API base URL, default `http://localhost:8080`
- `NOTIFYHUB_API_KEY` - API key for `X-API-Key`, default `loadtest-api-key`
- `NOTIFYHUB_CHANNEL` - channel to send, default `inapp`
- `NOTIFYHUB_RECIPIENT_PREFIX` - generated recipient prefix, default `k6-user`
- `NOTIFYHUB_IDEMPOTENCY` - set to `true` to include idempotency keys
- `K6_SCENARIO` - `smoke`, `baseline`, or `stress`, default `baseline`

## Scenarios

- `smoke`: 1 VU for 15 seconds; use this after deploys and config changes.
- `baseline`: ramps from 5 to 25 sends/second; use this for everyday performance checks.
- `stress`: ramps up to 200 sends/second; use this to find saturation points.

The default thresholds fail the run when HTTP failures exceed 1%, p95 latency exceeds 1.5s, or p99 latency exceeds 3s.

## Realtime In-App WebSocket Load

Use `scripts/loadtest/inapp_realtime.js` to test connected clients plus end-to-end delivery:

```bash
make loadtest-inapp-docker-smoke
make loadtest-inapp-docker-prod1000
```

The prod scenario ramps to 1000 connected WebSocket clients, has each client send at least one in-app notification to itself, and measures latency from API acceptance to receipt on `/api/v1/inbox/stream`.

Recommended prod-like settings before the 1000-client run:

```bash
export RATELIMIT_API_RPM=100000
export RATELIMIT_INAPP_PER_HOUR=100000
export DATABASE_MAX_CONNS=100
export WORKER_INAPP_CONCURRENCY=50
export INAPP_WS_BUFFER_SIZE=256
export WS_JWT_TTL_SECONDS=600
```

Realtime script variables:

- `NOTIFYHUB_BASE_URL` - API base URL, default `http://localhost:8080`
- `NOTIFYHUB_WS_URL` - WebSocket base URL, default derived from `NOTIFYHUB_BASE_URL`
- `NOTIFYHUB_API_KEY` - API key for `X-API-Key`, default `loadtest-api-key`
- `NOTIFYHUB_RECIPIENT_PREFIX` - generated recipient prefix, default `k6-inapp-user`
- `NOTIFYHUB_SENDS_PER_CLIENT` - notifications each connected client sends, default `1`
- `NOTIFYHUB_SEND_INTERVAL_MS` - interval between per-client sends, default `5000`
- `NOTIFYHUB_CONNECT_JITTER_MS` - spreads first sends after connect, default `1000`
- `K6_SCENARIO` - `smoke`, `baseline`, or `prod_1000`

Watch these during the run:

- API p95/p99 latency
- `notifyhub_ws_delivery_latency`
- `notifyhub_ws_missed_messages`
- Kafka consumer lag
- Redis CPU/memory and pub/sub throughput
- Postgres connection saturation
- worker `/metrics` for provider errors and dropped in-app frames
