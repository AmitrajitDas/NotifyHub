package middleware

import (
	"log/slog"
	"net/http"

	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// AdminAuth returns middleware that validates the X-Admin-Token header against
// a static token loaded from config. It does not use the database.
func AdminAuth(token string, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := RequestIDFromContext(r.Context())

			provided := r.Header.Get("X-Admin-Token")
			if provided == "" {
				response.JSONError(w, domain.NewUnauthorizedError("X-Admin-Token header is required"), reqID)
				return
			}

			if token == "" || provided != token {
				logger.Warn("invalid admin token attempt", "request_id", reqID)
				response.JSONError(w, domain.NewForbiddenError("invalid admin token"), reqID)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
