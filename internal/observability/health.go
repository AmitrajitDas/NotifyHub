package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Probe represents a single dependency health check.
// Each implementation checks one service (Postgres, Redis, Kafka)
// and returns a non-nil error when that service is unavailable.
type Probe interface {
	Name() string
	Check(ctx context.Context) error
}

// Checker runs a set of Probes and exposes three HTTP handlers:
//   - Live  — is the process up? Always 200 while the binary is running.
//   - Ready — are all dependencies reachable? 503 on any probe failure.
//   - Health — verbose JSON listing each probe's status (used by humans).
type Checker struct {
	probes  []Probe
	timeout time.Duration // per-probe deadline
}

// NewChecker returns a Checker that runs each probe with the given timeout.
// Probes run in parallel; the timeout applies to each individual probe.
func NewChecker(timeout time.Duration, probes ...Probe) *Checker {
	return &Checker{probes: probes, timeout: timeout}
}

// Live always returns 200 with {"status":"ok"} as long as the process is running.
// k8s uses this for liveness; a 200 means "don't restart me".
func (c *Checker) Live(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// Ready returns 200 when all probes pass, 503 when any probe fails.
// k8s uses this for readiness; a 503 pulls the pod from the load balancer
// without restarting it.
func (c *Checker) Ready(w http.ResponseWriter, r *http.Request) {
	results := c.runProbes(r.Context())

	w.Header().Set("Content-Type", "application/json")
	for _, res := range results {
		if res.err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "degraded"})
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// Health returns a verbose JSON body listing every probe's name and status.
// Clients: ops dashboards, manual curl checks. k8s should use /livez or /readyz.
func (c *Checker) Health(w http.ResponseWriter, r *http.Request) {
	results := c.runProbes(r.Context())

	checks := make(map[string]string, len(results))
	healthy := true
	for _, res := range results {
		if res.err != nil {
			checks[res.name] = "unavailable: " + res.err.Error()
			healthy = false
		} else {
			checks[res.name] = "ok"
		}
	}

	status := "ok"
	httpStatus := http.StatusOK
	if !healthy {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks,omitempty"`
	}{Status: status, Checks: checks})
}

// probeResult is the outcome of a single Probe.Check call.
type probeResult struct {
	name string
	err  error
}

// runProbes executes all probes concurrently and returns their results.
// Each probe gets its own context bounded by c.timeout.
func (c *Checker) runProbes(ctx context.Context) []probeResult {
	results := make([]probeResult, len(c.probes))

	var wg sync.WaitGroup
	for i, p := range c.probes {
		wg.Add(1)
		go func(idx int, probe Probe) {
			defer wg.Done()
			pCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			results[idx] = probeResult{name: probe.Name(), err: probe.Check(pCtx)}
		}(i, p)
	}
	wg.Wait()

	return results
}

// ── Built-in probes ───────────────────────────────────────────────────────────

// postgresProbe pings the pgxpool.
type postgresProbe struct{ pool *pgxpool.Pool }

// PostgresProbe returns a Probe that pings the given pgxpool.Pool.
func PostgresProbe(pool *pgxpool.Pool) Probe { return &postgresProbe{pool: pool} }

func (p *postgresProbe) Name() string { return "postgres" }
func (p *postgresProbe) Check(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// redisProbe pings the Redis client.
type redisProbe struct{ client *redis.Client }

// RedisProbe returns a Probe that sends PING to Redis.
func RedisProbe(client *redis.Client) Probe { return &redisProbe{client: client} }

func (p *redisProbe) Name() string { return "redis" }
func (p *redisProbe) Check(ctx context.Context) error {
	return p.client.Ping(ctx).Err()
}

// kafkaProbe dials the first broker and fetches partition metadata.
type kafkaProbe struct{ brokers []string }

// KafkaProbe returns a Probe that dials the first broker and reads metadata.
// It uses the caller-supplied context for the dial timeout; callers should
// wrap the context with a short deadline (the Checker does this automatically).
func KafkaProbe(brokers []string) Probe { return &kafkaProbe{brokers: brokers} }

func (p *kafkaProbe) Name() string { return "kafka" }
func (p *kafkaProbe) Check(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return nil
	}
	conn, err := kafka.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ReadPartitions()
	return err
}
