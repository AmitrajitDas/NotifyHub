package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/amitrajitdas31/notifyhub/internal/api/handler"
	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// RouterDeps holds all handlers and cross-cutting dependencies needed to build
// the router. Passed as a struct rather than individual args to keep the call
// site readable as the surface grows.
type RouterDeps struct {
	Auth         middleware.APIClientLookup
	AdminToken   string
	Logger       *slog.Logger
	RateLimit    service.RateLimitService
	RateLimitRPM int
	Metrics      *observability.Metrics
	Health       *observability.Checker
	Notification *handler.NotificationHandler
	Template     *handler.TemplateHandler
	Preference   *handler.PreferenceHandler
	Tenant       *handler.TenantHandler
	DLQ         *handler.DLQHandler
	Inbox       *handler.InboxHandler
	DeviceToken *handler.DeviceTokenHandler
}

func NewRouter(deps RouterDeps) http.Handler {
	r := chi.NewRouter()

	// ── Global middleware (runs on every request) ─────────────────────────────
	r.Use(chiMiddleware.RealIP)
	r.Use(middleware.RequestID)
	// otelhttp creates a server span per request and propagates W3C traceparent.
	// It must run before Logging so the trace_id is available when we log.
	r.Use(otelhttp.NewMiddleware("notifyhub-api",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	))
	r.Use(middleware.Logging(deps.Logger))
	r.Use(middleware.Recovery(deps.Logger))
	r.Use(middleware.MetricsMiddleware(deps.Metrics))

	// ── Observability endpoints (no auth, no rate-limit) ──────────────────────
	r.Handle("/metrics", deps.Metrics.Handler())
	r.Get("/livez", deps.Health.Live)
	r.Get("/readyz", deps.Health.Ready)
	r.Get("/health", deps.Health.Health)

	// ── Internal admin routes (X-Admin-Token, no API key auth) ───────────────
	r.Group(func(r chi.Router) {
		r.Use(middleware.AdminAuth(deps.AdminToken, deps.Logger))
		r.Route("/internal/tenants", func(r chi.Router) {
			r.Post("/", deps.Tenant.CreateTenant)
			r.Post("/{id}/api-keys", deps.Tenant.CreateAPIKey)
		})
		if deps.DLQ != nil {
			r.Route("/internal/dlq", func(r chi.Router) {
				r.Get("/", deps.DLQ.List)
				r.Get("/{id}", deps.DLQ.GetByID)
				r.Post("/{id}/replay", deps.DLQ.Replay)
				r.Delete("/{id}", deps.DLQ.Delete)
			})
		}
	})

	// ── Authenticated API routes ──────────────────────────────────────────────
	r.Group(func(r chi.Router) {
		r.Use(middleware.Auth(deps.Auth, deps.Logger))
		r.Use(middleware.RateLimit(deps.RateLimit, deps.RateLimitRPM, deps.Metrics, deps.Logger))

		r.Route("/api/v1", func(r chi.Router) {

			// Notifications
			r.Route("/notifications", func(r chi.Router) {
				r.Post("/", deps.Notification.Send)
				r.Post("/bulk", deps.Notification.SendBulk)
				r.Get("/", deps.Notification.List)
				r.Get("/{id}", deps.Notification.GetByID)
				r.Delete("/{id}", deps.Notification.Cancel)
			})

			// Templates
			r.Route("/templates", func(r chi.Router) {
				r.Post("/", deps.Template.Create)
				r.Get("/", deps.Template.List)
				r.Get("/{id}", deps.Template.GetByID)
				r.Put("/{id}", deps.Template.Update)
				r.Delete("/{id}", deps.Template.Delete)
				r.Post("/{id}/preview", deps.Template.Preview)
			})

			// User preferences
			r.Route("/users/{user_id}/preferences", func(r chi.Router) {
				r.Get("/", deps.Preference.ListByUser)
				r.Put("/{channel}", deps.Preference.Upsert)
				r.Delete("/{channel}", deps.Preference.Delete)
			})

			// Device tokens (FCM push)
			if deps.DeviceToken != nil {
				r.Route("/device-tokens", func(r chi.Router) {
					r.Post("/", deps.DeviceToken.Register)
					r.Delete("/{token}", deps.DeviceToken.Deregister)
				})
			}

			// In-app inbox
			if deps.Inbox != nil {
				r.Route("/inbox", func(r chi.Router) {
					r.Get("/", deps.Inbox.List)
					r.Get("/unread-count", deps.Inbox.UnreadCount)
					r.Post("/read-all", deps.Inbox.MarkAllRead)
					r.Post("/{id}/read", deps.Inbox.MarkRead)
					r.Get("/stream", deps.Inbox.Stream)
				})
			}
		})
	})

	return r
}
