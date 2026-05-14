# Production Incident Story — "Silent Kafka Worker"

> Interview-ready narrative. Kafka half and WebSocket half are self-contained — cut either independently.

---

## Situation

Running a notification platform — multi-channel, Kafka-backed, with a WebSocket stream for real-time in-app alerts. Fresh deploy to a new environment. Ops confirmed all containers green. Customers started reporting they weren't receiving in-app notifications.

**What made it hard:** everything *looked* healthy. API returned 202. DB showed `status: queued`. Kafka topic existed. Worker process was running, no crashes, no error logs. Standard monitoring showed nothing wrong.

---

## Act 1 — Kafka Worker Deadlock

### Investigation

Started by checking what the worker was actually *doing*. Grepped logs for any processing events — delivered, dropped, failed. Zero. The worker had been running for 20 minutes and processed nothing.

Checked Kafka consumer group lag. That's where it got interesting: the consumer group had `no active members` according to the broker, even though the worker process was alive and connected.

Traced it back to startup order. In this environment, topics were auto-created by the first producer publish — they didn't exist when the worker started. kafka-go joined the consumer group, got empty partition assignments because there was nothing to assign, and then stayed there. `FetchMessage` blocks forever with no partition assignments. The library never re-triggered a rebalance after topics appeared.

Sequence:
1. Worker starts → joins consumer group
2. Topics don't exist yet → empty partition assignments
3. Producer publishes first message → topics auto-created
4. Worker sits in `FetchMessage` blocked forever
5. Zero deliveries, no errors logged

### Fix #1 — Pre-create topics at startup

Added `EnsureTopics()` at worker startup, before any consumer is created. Uses the Kafka admin API to create all 9 topics idempotently (4 channels + 4 DLQs + webhook topic). Topics always exist before consumers join.

```go
// cmd/worker/main.go — before startPools()
if err := queue.EnsureTopics(context.Background(), cfg.KafkaBrokers, logger); err != nil {
    logger.Error("failed to ensure kafka topics", "error", err)
    os.Exit(1)
}
```

### Bug hiding underneath — `StartOffset: LastOffset`

After fixing startup order, the first two messages published during the incident window were *still* stuck at `queued` even after the worker restarted.

Root cause: `StartOffset: kafka.LastOffset` in the consumer config. When a consumer group has no committed offsets, `LastOffset` means "start from now" — anything published before the group first successfully joined is silently skipped.

**How I found it:** compared the Kafka partition's latest offset (2, meaning 2 messages existed) against the consumer group's committed position after restart (also 2 — skipped straight to the end). The group never read offsets 0 or 1.

### Fix #2 — `FirstOffset`

```go
// internal/queue/consumer.go
StartOffset: kafka.FirstOffset, // new groups replay from earliest uncommitted offset
```

New groups now replay from the beginning if no committed offset exists, catching any messages published before the first successful consumer join.

---

## Act 2 — WebSocket Completely Broken

While investigating, tried to reproduce the issue by connecting a WebSocket client to the real-time notification stream. Got `401 Unauthorized`. Odd — the WS handshake uses a short-lived JWT as a query param, no API key required.

### Bug #3 — Wrong middleware group

Checked the router. `/inbox/stream` was nested inside an auth middleware group that checked for `X-API-Key`. The handler had its own JWT verification, but the middleware fired first and rejected the request.

WebSocket clients — especially browsers — cannot set custom headers on the upgrade handshake. The endpoint was completely inaccessible to any browser client, silently, since day one.

**Fix:** moved the route outside the auth group. Handler already had its own auth.

### Bug #4 — `http.Hijacker` stripped by logging middleware

After the route fix, got `501 Not Implemented`.

`coder/websocket` uses `http.Hijacker` to take over the TCP connection for HTTP/1.1 upgrade. The logging middleware wrapped `ResponseWriter` in a custom `statusWriter` struct to capture status codes — but didn't forward the `Hijacker` interface. `websocket.Accept()` failed the type assertion and returned 501.

```go
// internal/api/middleware/logging.go
func (sw *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
    h, ok := sw.ResponseWriter.(http.Hijacker)
    if !ok {
        return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
    }
    return h.Hijack()
}
```

After that — full end-to-end WebSocket delivery working.

---

## Result

**Kafka:** all new notifications delivered within seconds of worker restart. Old messages from the incident window were permanently undeliverable (committed offsets had advanced past them) — ops manually re-queued ~40 affected notifications from the DB.

**WebSocket:** real-time delivery working for the first time in the product's history.

---

## What I Learned

kafka-go's consumer group does not recover from empty partition assignments when topics appear later. It is not a library bug — it is an assumption violation: the library assumes topics exist when consumers start. The fix is operational, not a workaround.

`LastOffset` for new consumer groups is a footgun that is easy to miss in testing, because in a typical dev workflow you publish *after* starting the consumer. It only bites on fresh deploys or when the worker restarts while messages are in flight.

Any `ResponseWriter` wrapper in middleware must forward every optional interface (`Hijacker`, `Flusher`, `Pusher`) or it will silently break features that depend on them.

---

## Commits

- `fix: pre-create kafka topics at worker startup to prevent consumer deadlock`
- `fix: use FirstOffset for new consumer groups to prevent silent message loss`
- `fix: move WebSocket stream route outside API key auth middleware`
- `fix: forward http.Hijacker in logging middleware statusWriter`
