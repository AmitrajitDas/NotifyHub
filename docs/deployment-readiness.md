# Deployment Readiness Checklist

Work required to make NotifyHub production-ready for general app integration. Excludes infra/devops items already tracked in `CLAUDE.md` (CI/CD, k8s/Helm, OpenAPI, load tests, ADRs).

**Scope:** Push (FCM) + In-app (WebSocket) are the supported channels. Existing email (SES) and SMS (Twilio) code is kept as-is — no new work on those fronts.

Grouped by priority. Each item lists scope, files to touch, acceptance criteria.

---

## P0 — Blockers for any real deployment

### 1. Device token API (FCM registration)

Plan Phase 8; not in `CLAUDE.md` Done list. Required for push to work at all.

**Scope:**
- Migration `device_tokens(id, tenant_id, recipient_id, token UNIQUE, platform, is_active, last_seen_at, created_at, updated_at)`
- `POST /api/v1/device-tokens` — upsert by token, mark active
- `DELETE /api/v1/device-tokens/:token` — deregister on logout
- FCM provider rewrite: query active tokens for `recipient_id`, fan-out send, deactivate token on `messaging.ErrCodeUnregistered`/`InvalidArgument`
- Multi-device fan-out logged per-token in `delivery_logs`

**Files:**
- `migrations/00000X_device_tokens.up.sql`
- `queries/device_token.sql`
- `internal/repository/device_token.go`
- `internal/api/handler/device_token.go`
- `internal/provider/fcm/fcm.go` — token lookup + multi-send + deactivation

**Acceptance:** Register token from RN; send push notification; token receives push; revoke token on Firebase console; next send marks token inactive.

### 2. WebSocket auth via short-lived JWT

Current `/inbox/stream` reads `X-Recipient-ID` header — trusts client. Browser/RN `WebSocket` cannot set custom headers reliably; also auth bypass risk (any client claiming any recipient).

**Scope:**
- `POST /api/v1/ws-token` — issues JWT with `tenant_id`, `recipient_id`, 60s TTL, signed with `WS_JWT_SECRET`
- `GET /api/v1/inbox/stream?token=<jwt>` — verify JWT, derive identity from claims, ignore `X-Recipient-ID`
- Reject connections without valid token; close with 4401
- Hub presence registry keyed by JWT-derived `(tenant_id, recipient_id)`

**Files:**
- `internal/auth/jwt.go`
- `internal/api/handler/ws_token.go`
- `internal/api/handler/inbox.go` — `Stream()` auth path
- `internal/realtime/hub.go` — verify identity source

**Env:** `WS_JWT_SECRET`, `WS_JWT_TTL_SECONDS=60`

### 3. Outbound webhooks (delivery events to your app)

Your app needs to know `delivered` / `failed` / `dropped` per notification. Currently only via DB read. Without this, consuming apps poll — bad pattern.

**Scope:**
- New table `webhook_endpoints(id, tenant_id, url, secret, events[], is_active, created_at)`
- New table `webhook_deliveries(id, endpoint_id, payload, status, attempt, next_retry_at, last_error)`
- Worker: on terminal status change, enqueue Kafka topic `notifyhub.webhooks.outbound`
- Webhook worker: HTTP POST with `X-NotifyHub-Signature: sha256=<hmac(body, secret)>`, `X-NotifyHub-Event`, `X-NotifyHub-Delivery-ID`
- Retry with exponential backoff, route to DLQ after N attempts
- Admin endpoints: `POST/GET/DELETE /api/v1/webhooks`
- Events: `notification.delivered`, `notification.failed`, `notification.dropped`, `device_token.deactivated`

**Files:**
- `migrations/00000X_webhooks.up.sql`
- `internal/service/webhook.go`
- `internal/worker/webhook_worker.go`
- `internal/api/handler/webhook_admin.go`

**Acceptance:** Register webhook URL; send notification; receive POST with valid HMAC signature within seconds of delivery.

### 4. Reconnect cursor for inbox

After WS disconnect (mobile network drop, app background→foreground), client must replay missed messages without dupes.

**Scope:**
- `GET /api/v1/inbox?after_id=<uuid>&limit=50` — paginate by `(created_at, id)` cursor
- `GET /api/v1/inbox/stream?token=<jwt>&since=<message_id>` — server reads gap from DB, sends as historical batch, then switches to live pub/sub
- Each WS frame includes monotonic `id` so client can store last-seen
- Document reconnect protocol

**Files:**
- `internal/api/handler/inbox.go`
- `internal/repository/inapp_message.go`
- `internal/realtime/hub.go` — replay-then-live transition

### 5. Secrets management

`FCM_CREDENTIALS_FILE=/etc/notifyhub/fcm-credentials.json` on disk fragile in containers; secrets in env leak via `/proc`, crash dumps, error logs.

**Scope:**
- Abstract `SecretProvider` interface: `Get(ctx, key) (string, error)`
- Impls: `EnvSecretProvider` (dev), `AWSSecretsManagerProvider` (prod), optional `VaultProvider`
- Config: `SECRETS_BACKEND=env|aws|vault`
- FCM service-account JSON loaded as secret blob (not file path) in prod
- `WS_JWT_SECRET`, webhook signing secrets, admin token all routed through provider

**Files:**
- `internal/secrets/secrets.go`
- `cmd/api/main.go`, `cmd/worker/main.go` — wire backend at startup

### 6. Recipient identity hardening

Push and in-app both rely on `recipient_id` (string). Without binding to authenticated user in your app, anyone can register any device token / claim any inbox.

**Scope:**
- Document trust boundary: NotifyHub trusts the API client (`X-API-Key`); the API client (your backend) is responsible for verifying `recipient_id` matches the authenticated end-user before forwarding requests
- Add **HMAC-signed recipient claim** option: `X-Recipient-Token: hmac(api_secret, tenant_id|recipient_id|expiry)` for endpoints called from RN via proxy
- WS JWT token (item 2) already encodes verified identity — same pattern for `/api/v1/device-tokens` if called from app

**Files:**
- `docs/security-model.md` (new)
- `internal/api/middleware/recipient_token.go`

---

## P1 — Strongly recommended before production

### 7. Audit log

Compliance + debugging. Who sent what, when, from which API key.

**Scope:**
- `audit_log(id, tenant_id, api_client_id, action, resource_type, resource_id, payload_hash, ip, user_agent, created_at)`
- Middleware writes on every mutation (`POST/PUT/DELETE`)
- Append-only — no UPDATE/DELETE permitted via app
- Retention: 90 days hot, archive cold (S3/Glacier)

**Files:**
- `migrations/00000X_audit_log.up.sql`
- `internal/api/middleware/audit.go`

### 8. Bulk send endpoint

Plan specifies `POST /api/v1/notifications/bulk` (≤1000 recipients). Common need (broadcast to user segment).

**Scope:**
- Validate `BulkSendRequest`
- Single DB transaction inserting N notification rows
- Batch publish to Kafka (one round-trip per partition via `WriteMessages`)
- Per-recipient payload override
- Returns `[]{recipient_id, notification_id, status, error}` for partial-failure visibility

**Files:**
- `internal/api/handler/notification.go` — `SendBulk()`
- `internal/service/notification.go` — `SendBulk(ctx, tenantID, req)`
- `internal/queue/publisher.go` — batch publish helper

### 9. Localization (i18n) for templates

Apps with non-English users need per-locale templates. Push title/body usually short — but still translated.

**Scope:**
- `templates` table: add `locale VARCHAR(10) DEFAULT 'en'`; constraint `UNIQUE(tenant_id, name, locale)`
- `SendRequest.Locale` — optional override; fallback chain: request → recipient pref → tenant default → `en`
- `preferences`: add `locale` field
- Template service: `Render(name, locale, payload)` walks fallback chain

**Files:**
- `migrations/00000X_template_locale.up.sql`
- `internal/service/template.go`
- `internal/domain/preference.go`

### 10. Push notification options

FCM supports more than title/body. Apps usually need:

**Scope:**
- Domain extension: `PushOptions{ Sound, Badge, ClickAction, DeepLink, ImageURL, Priority, CollapseKey, TimeToLive }`
- Map into `messaging.Message.{APNS,Android}Config` per platform
- Template payload exposes `_push_title`, `_push_body`, plus per-platform overrides via `payload._apns`, `payload._android`
- Silent push support (`content-available: 1`, no notification block) for background data sync

**Files:**
- `internal/domain/notification.go`
- `internal/provider/fcm/fcm.go`

### 11. RN integration guide

Real consumer needs documented path.

**Scope:** new doc `docs/integration-react-native.md`:
- Trust model: never embed `X-API-Key` in app; your backend proxies NotifyHub
- FCM setup (`@react-native-firebase/messaging`); token registration; foreground/background handlers
- WebSocket connect: fetch JWT via your backend → open `wss://api/api/v1/inbox/stream?token=...&since=...`
- Reconnect with exponential backoff; resume from last `id`
- Inbox REST fallback when WS unavailable
- Sample proxy backend snippet (Go and Node)
- Notification permissions request flow (iOS/Android)

### 12. Health check depth

**Scope:**
- `/ready`: Postgres `SELECT 1`, Redis `PING`, Kafka metadata fetch, FCM auth probe (cached, 60s TTL)
- `/health`: shallow (process up, can serve)
- Distinct probes for k8s liveness vs readiness

**Files:** `internal/observability/health.go`

---

## P2 — Nice-to-have

### 13. Per-tenant usage metering
- Redis counter per send, flushed to `usage_daily(tenant_id, channel, date, count)` table
- `GET /internal/tenants/:id/usage?from=&to=`
- Foundation for quotas / billing

### 14. Cost ceiling guardrails
- `tenants.daily_send_limit_push`, `daily_send_limit_inapp`
- Auto-drop notifications past limit with status `quota_exceeded`; alert ops

### 15. Admin UI
- Minimal Next.js: tenants, API keys, templates, DLQ inspection, recent sends, device tokens
- Behind `X-Admin-Token`

### 16. Open/click tracking (push deep links)
- Track when user taps push → opens app at deep link
- RN-side instrumentation; webhook back to NotifyHub `POST /api/v1/notifications/:id/events` with `opened_at`, `clicked_at`
- Stored in `delivery_logs.opened_at`, `clicked_at`

### 17. Template A/B testing
- `templates.variant_group`, `variant_weight`
- Random pick by weight; log `delivery_logs.variant_id`

### 18. Scheduled-in-user-timezone
- Current `scheduled_at TIMESTAMPTZ` is absolute UTC. Add `scheduled_at_local TIME, scheduled_tz` for "send at 9am in user's local zone"
- Scheduler resolves at enqueue time using recipient preference timezone

### 19. Circuit breaker per-provider tuning
- `gobreaker` configs exposed via env: failure threshold, open duration, half-open requests
- Separate breaker for FCM vs in-app Redis pub/sub

### 20. WebSocket scale-out
- Current `Hub` is single-process. For >1 API replica, use Redis pub/sub fan-out across pods (already plan-mentioned, verify implementation)
- Sticky sessions not required if all replicas subscribe to same Redis channel

---

## Operational hardening

### 21. Backup & restore
- Postgres: nightly `pg_dump` to S3; WAL archive for PITR
- Redis: only ephemeral state (rate limits, presence) — no backup needed
- Kafka: replication-factor 3 in prod; topic-level retention reviewed
- Document restore procedure with RTO/RPO targets

### 22. Data retention
- `notifications` older than N days → archive partition or delete
- `delivery_logs` older than N days → S3 cold
- `inapp_messages` per-tenant retention (30/90/forever) via tenant config
- Right-to-delete: `DELETE /api/v1/users/:user_id` cascades preferences, device tokens, inbox, audit log entries (or anonymizes)

### 23. Multi-region (if needed)
- Document active-passive vs active-active
- Kafka MirrorMaker 2 cross-region
- Postgres logical replication
- FCM is global — no region work needed for push
- WebSocket: region-local hubs; cross-region pub/sub via Redis Cluster or skip (users connect to nearest region)

### 24. Security review
- `gosec ./...` and `govulncheck ./...` in CI
- Threat model: `docs/threat-model.md`
- Token leak detection (don't log JWTs, API keys, FCM tokens)
- Pen-test before public launch

### 25. Runbooks
- `docs/runbooks/`: FCM auth failure, Kafka lag spike, Redis OOM, Postgres slow query, DLQ growth, WebSocket connection storm
- Each runbook: symptom → diagnosis → remediation → escalation

---

## Definition of Done

Integration-ready for "any app" when **all P0 (1–6) + P1 items 7, 8, 11, 12** are shipped, plus the remaining DevOps items in `CLAUDE.md` (CI/CD, Helm, OpenAPI, load tests).

P1 remainder + P2 + Operational ship incrementally post-launch.
