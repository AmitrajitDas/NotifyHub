package observability

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// WithSpanAttrs is a convenience wrapper around trace.WithAttributes.
// It keeps call-sites readable by reducing the import burden on callers —
// they only need to import observability instead of both observability and
// go.opentelemetry.io/otel/trace.
func WithSpanAttrs(attrs ...attribute.KeyValue) trace.SpanStartOption {
	return trace.WithAttributes(attrs...)
}
