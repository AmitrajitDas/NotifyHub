package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// TraceHandler wraps a slog.Handler and injects the active span's trace_id
// and span_id into every log record emitted while a span is active.
// This creates the log ↔ trace correlation needed to jump from a Loki log
// line directly to the corresponding Tempo trace in Grafana.
type TraceHandler struct {
	inner slog.Handler
}

// NewTraceHandler wraps inner so that log records include trace context attrs
// when a span is present in the context.
func NewTraceHandler(inner slog.Handler) *TraceHandler {
	return &TraceHandler{inner: inner}
}

// Enabled delegates to the inner handler.
func (h *TraceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

// Handle injects trace_id and span_id from the active span then delegates.
func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

// WithAttrs delegates to the inner handler.
func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{inner: h.inner.WithAttrs(attrs)}
}

// WithGroup delegates to the inner handler.
func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{inner: h.inner.WithGroup(name)}
}

// NewLogger returns a slog.Logger backed by a JSON handler that automatically
// injects trace_id / span_id whenever a span is active in the context.
// Pass this to all services and handlers. Use logger.InfoContext(ctx, ...) —
// the ctx is what triggers the trace injection.
func NewLogger(level slog.Level) *slog.Logger {
	jsonHandler := slog.NewJSONHandler(
		// os.Stdout is the write target; the container runtime captures it and
		// Grafana Alloy ships the JSON lines to Loki.
		logOutput(),
		&slog.HandlerOptions{Level: level},
	)
	return slog.New(NewTraceHandler(jsonHandler))
}
