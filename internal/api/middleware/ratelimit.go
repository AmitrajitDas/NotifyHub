package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// RateLimit returns middleware that enforces a per-API-client sliding-window
// request cap of rpm requests per minute. It must run after Auth so that the
// API client is already present in the context.
//
// On Redis failure the middleware fails open — the request is allowed through
// and the error is logged. Blocking all API traffic on Redis flaps is worse
// than temporarily losing rate-limit enforcement.
func RateLimit(svc service.RateLimitService, rpm int, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := RequestIDFromContext(r.Context())

			client := ClientFromContext(r.Context())
			if client == nil {
				// Auth middleware didn't run or failed; let the request proceed.
				next.ServeHTTP(w, r)
				return
			}

			key := fmt.Sprintf("ratelimit:api:%s", client.ID)
			ok, err := svc.AllowKey(r.Context(), key, time.Minute, rpm)
			if err != nil {
				logger.Error("rate limit check failed, failing open", "error", err, "request_id", reqID, "client_id", client.ID)
				next.ServeHTTP(w, r)
				return
			}

			if !ok {
				w.Header().Set("Retry-After", strconv.Itoa(60))
				response.JSONError(w, domain.NewRateLimitedError("API rate limit exceeded"), reqID)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
