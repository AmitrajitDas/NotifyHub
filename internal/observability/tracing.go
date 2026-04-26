package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TracingConfig holds all parameters needed to initialise the tracer provider.
type TracingConfig struct {
	// Endpoint is the OTLP gRPC endpoint of the OTel Collector, e.g. "otel-collector:4317".
	// When empty, tracing is disabled and a no-op provider is installed — safe for local dev.
	Endpoint string

	// Insecure disables TLS verification on the gRPC connection.
	// Should be true for in-cluster collector communication; false when exporting externally.
	Insecure bool

	// ServiceName identifies this process in the collector (e.g. "notifyhub-api").
	ServiceName string

	// Version and Environment are attached to every span as resource attributes.
	Version     string
	Environment string

	// SampleRatio is the fraction of traces to sample (0.0–1.0).
	// 0.1 = 10 %. The sampler is ParentBased so downstream spans honour the
	// upstream decision, preventing broken trace trees.
	SampleRatio float64
}

// InitTracer configures the global OTel TracerProvider.
//
// When cfg.Endpoint is empty a no-op provider is installed; all otel.Tracer()
// calls succeed but produce no spans. This keeps local dev free of collector
// dependencies.
//
// The returned shutdown func must be deferred in main() to flush in-flight
// spans before the process exits. Callers should supply a context with a
// short deadline (≤5 s) to avoid blocking shutdown.
func InitTracer(ctx context.Context, cfg TracingConfig) (shutdown func(context.Context) error, err error) {
	if cfg.Endpoint == "" {
		// No collector configured — install noop and return immediately.
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	// ── Resource attributes attached to every span ────────────────────────────
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.Version),
			semconv.DeploymentEnvironment(cfg.Environment),
		),
		resource.WithHost(),
		resource.WithProcess(),
	)
	// resource.New returns a partial resource on detection errors (e.g.
	// user.Current() failing in CGO-free containers without $USER set).
	// Treat as non-fatal — the partial resource is still usable.
	if err != nil && res == nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	// ── gRPC connection to the OTel Collector ─────────────────────────────────
	grpcOpts := []grpc.DialOption{grpc.WithBlock()}
	if cfg.Insecure {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(connCtx, cfg.Endpoint, grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial otel collector %s: %w", cfg.Endpoint, err)
	}

	// ── OTLP trace exporter ───────────────────────────────────────────────────
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	// ── Trace provider with parent-based ratio sampling ───────────────────────
	ratio := cfg.SampleRatio
	if ratio <= 0 {
		ratio = 0.1 // sane default: 10 %
	}
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// ── Install globally so otel.Tracer() works anywhere ─────────────────────
	otel.SetTracerProvider(tp)

	// W3C traceparent + tracestate propagation — used for Kafka header injection.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	shutdown = func(shutCtx context.Context) error {
		if err := tp.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("flush tracer provider: %w", err)
		}
		return conn.Close()
	}

	return shutdown, nil
}

// Tracer returns an otel.Tracer scoped to the given instrumentation name.
// Callers use this to start spans; the underlying provider is whatever
// InitTracer installed (real or noop).
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
