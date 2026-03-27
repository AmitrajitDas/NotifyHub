package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"

	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

type clientContextKey string

const clientKey clientContextKey = "api_client"

// APIClientLookup is the minimal interface the auth middleware needs.
// The concrete repository implements this; tests can use a stub.
type APIClientLookup interface {
	GetByKeyHash(ctx context.Context, hash string) (*domain.APIClient, error)
}

// Auth returns middleware that validates X-API-Key against the database.
// It SHA-256 hashes the raw key before the lookup (the DB stores hashes, never raw keys).
func Auth(repo APIClientLookup, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rawKey := r.Header.Get("X-API-Key")
			if rawKey == "" {
				response.JSONError(w, domain.NewUnauthorizedError("X-API-Key header is required"), RequestIDFromContext(r.Context()))
				return
			}

			hash := hashKey(rawKey)
			client, err := repo.GetByKeyHash(r.Context(), hash)
			if err != nil {
				var appErr *domain.AppError
				if ok := isAppError(err, &appErr); ok {
					response.JSONError(w, appErr, RequestIDFromContext(r.Context()))
				} else {
					logger.Error("auth lookup failed", "error", err, "request_id", RequestIDFromContext(r.Context()))
					response.JSONError(w, domain.NewUnauthorizedError("invalid API key"), RequestIDFromContext(r.Context()))
				}
				return
			}

			if !client.IsActive {
				response.JSONError(w, domain.NewForbiddenError("API key is inactive"), RequestIDFromContext(r.Context()))
				return
			}

			ctx := context.WithValue(r.Context(), clientKey, client)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClientFromContext retrieves the authenticated API client stored by Auth middleware.
func ClientFromContext(ctx context.Context) *domain.APIClient {
	c, _ := ctx.Value(clientKey).(*domain.APIClient)
	return c
}

func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func isAppError(err error, target **domain.AppError) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*domain.AppError); ok {
		*target = e
		return true
	}
	return false
}
