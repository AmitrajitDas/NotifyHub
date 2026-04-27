// Package observability owns NotifyHub's metrics, tracing, health, and
// log-correlation primitives. It has no dependencies on the rest of the app
// other than domain types — the dependency arrow points one way: every
// other package may import observability, but observability imports none of
// them.
//
// The Metrics struct exposed here is the single source of truth for
// Prometheus collectors. Construct it once in main(), inject it into every
// service/handler that emits, and serve its Handler at /metrics.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Outcome label values. Centralised so producers and dashboards stay aligned.
const (
	OutcomeOK             = "ok"
	OutcomeTempFail       = "temp_fail"
	OutcomePermFail       = "perm_fail"
	OutcomeAllowed        = "allowed"
	OutcomeDenied         = "denied"
	StatusDelivered       = "delivered"
	StatusFailed          = "failed"
	StatusDroppedPref     = "dropped_pref"
	StatusDroppedRateLim  = "dropped_ratelimit"
	ScopeAPI              = "api"
	ScopeWorker           = "worker"
)

// Metrics aggregates every Prometheus collector NotifyHub emits.
// Construct it once in main() — collectors register on their own private
// registry, isolated from the global default registry so unit tests don't
// trip on duplicate registration.
type Metrics struct {
	reg *prometheus.Registry

	// HTTP — labelled by the chi route pattern (low cardinality), method, status.
	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec

	// Notification lifecycle.
	NotifEnqueued  *prometheus.CounterVec // labels: channel
	NotifProcessed *prometheus.CounterVec // labels: channel, status

	// Provider call telemetry.
	ProviderSendTotal *prometheus.CounterVec   // labels: channel, provider, outcome
	ProviderSendDur   *prometheus.HistogramVec // labels: channel, provider, outcome

	// Worker internals.
	WorkerInflight  *prometheus.GaugeVec // labels: channel
	QueueLagSeconds *prometheus.GaugeVec // labels: channel

	// Templating.
	TemplateRenderDur prometheus.Histogram

	// Rate-limit decisions across the API and worker.
	RateLimitDecisions *prometheus.CounterVec // labels: scope, outcome

	// Build / runtime info — set once at startup, never changes.
	BuildInfo *prometheus.GaugeVec

	// DLQ telemetry.
	DLQPublished    *prometheus.CounterVec // labels: channel, reason
	DLQPublishErrors *prometheus.CounterVec // labels: channel
	DLQPersisted    *prometheus.CounterVec // labels: channel
	DLQReplayed     *prometheus.CounterVec // labels: channel
	DLQDepth        *prometheus.GaugeVec  // labels: channel
}

// NewMetrics builds a Metrics struct backed by a fresh registry.
// Call once per process. version/commit come from -ldflags; env from config.
func NewMetrics(version, commit, env string) *Metrics {
	reg := prometheus.NewRegistry()

	// Standard runtime collectors — Go GC, goroutines, file descriptors, etc.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,

		HTTPRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total HTTP requests handled, labelled by method, route pattern, and status.",
			},
			[]string{"method", "route", "status"},
		),
		HTTPDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "notifyhub",
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "HTTP request latency in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method", "route"},
		),

		NotifEnqueued: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "notifications",
				Name:      "enqueued_total",
				Help:      "Notifications successfully published to Kafka.",
			},
			[]string{"channel"},
		),
		NotifProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "notifications",
				Name:      "processed_total",
				Help:      "Notifications fully processed by the worker (terminal status reached).",
			},
			[]string{"channel", "status"},
		),

		ProviderSendTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "provider",
				Name:      "send_total",
				Help:      "Provider Send() calls.",
			},
			[]string{"channel", "provider", "outcome"},
		),
		ProviderSendDur: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "notifyhub",
				Subsystem: "provider",
				Name:      "send_duration_seconds",
				Help:      "Provider Send() latency in seconds.",
				Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
			},
			[]string{"channel", "provider", "outcome"},
		),

		WorkerInflight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "notifyhub",
				Subsystem: "worker",
				Name:      "inflight",
				Help:      "Messages currently being processed per channel.",
			},
			[]string{"channel"},
		),
		QueueLagSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "notifyhub",
				Subsystem: "queue",
				Name:      "consume_lag_seconds",
				Help:      "Age of the most recently fetched message at consume time.",
			},
			[]string{"channel"},
		),

		TemplateRenderDur: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "notifyhub",
				Subsystem: "template",
				Name:      "render_duration_seconds",
				Help:      "Template render latency in seconds.",
				Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
			},
		),

		RateLimitDecisions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "ratelimit",
				Name:      "decisions_total",
				Help:      "Rate-limit decisions: scope=api|worker, outcome=allowed|denied.",
			},
			[]string{"scope", "outcome"},
		),

		BuildInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "notifyhub",
				Name:      "build_info",
				Help:      "Build metadata. Always 1; labels carry version, commit, env.",
			},
			[]string{"version", "commit", "env"},
		),

		DLQPublished: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "dlq",
				Name:      "published_total",
				Help:      "Messages successfully published to a DLQ topic.",
			},
			[]string{"channel", "reason"},
		),
		DLQPublishErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "dlq",
				Name:      "publish_errors_total",
				Help:      "Failed DLQ publish attempts (message still marked failed).",
			},
			[]string{"channel"},
		),
		DLQPersisted: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "dlq",
				Name:      "persisted_total",
				Help:      "DLQ messages persisted to dead_letter_messages table.",
			},
			[]string{"channel"},
		),
		DLQReplayed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notifyhub",
				Subsystem: "dlq",
				Name:      "replayed_total",
				Help:      "DLQ messages replayed via admin API.",
			},
			[]string{"channel"},
		),
		DLQDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "notifyhub",
				Subsystem: "dlq",
				Name:      "depth",
				Help:      "Current number of unreplayed dead-letter messages per channel.",
			},
			[]string{"channel"},
		),
	}

	reg.MustRegister(
		m.HTTPRequests, m.HTTPDuration,
		m.NotifEnqueued, m.NotifProcessed,
		m.ProviderSendTotal, m.ProviderSendDur,
		m.WorkerInflight, m.QueueLagSeconds,
		m.TemplateRenderDur,
		m.RateLimitDecisions,
		m.BuildInfo,
		m.DLQPublished, m.DLQPublishErrors,
		m.DLQPersisted, m.DLQReplayed, m.DLQDepth,
	)

	m.BuildInfo.WithLabelValues(version, commit, env).Set(1)

	return m
}

// Handler returns the http.Handler that serves the Prometheus exposition
// format from this Metrics' private registry. Mount at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// Registry returns the underlying Prometheus registry. Useful for tests
// that want to scrape and assert on emitted samples.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
