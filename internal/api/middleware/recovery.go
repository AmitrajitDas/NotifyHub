package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// Recovery catches panics, logs the stack trace, and returns a 500 error response.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic recovered",
						"panic", rec,
						"stack", string(debug.Stack()),
						"request_id", RequestIDFromContext(r.Context()),
					)
					response.JSONError(w,
						domain.NewInternalError("an unexpected error occurred", nil),
						RequestIDFromContext(r.Context()),
					)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
