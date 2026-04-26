package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/amitrajitdas31/notifyhub/internal/observability"
)

// MetricsMiddleware records HTTP RED metrics (Requests, Errors, Duration) for
// every request that passes through the chi router.
//
// Labels use the chi route pattern (e.g. /api/v1/notifications/{id}) rather
// than the raw URL, so label cardinality stays bounded regardless of how many
// unique IDs are requested. The pattern is resolved after the handler runs,
// which is why we capture it in a deferred closure.
func MetricsMiddleware(m *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// WrapResponseWriter captures the status code written by the handler.
			ww := chiMiddleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			// RoutePattern is only populated after the handler returns, so read
			// it here rather than before calling next.ServeHTTP.
			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}

			status := strconv.Itoa(ww.Status())
			dur := time.Since(start).Seconds()

			m.HTTPRequests.WithLabelValues(r.Method, route, status).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur)
		})
	}
}
