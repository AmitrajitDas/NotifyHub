package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/amitrajitdas31/notifyhub/internal/api/handler"
	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// RouterDeps holds all handlers needed to build the router.
type RouterDeps struct {
	Auth            middleware.APIClientLookup
	AdminToken      string
	Logger          *slog.Logger
	RateLimit       service.RateLimitService
	RateLimitRPM    int
	Notification    *handler.NotificationHandler
	Template        *handler.TemplateHandler
	Preference      *handler.PreferenceHandler
	Health          *handler.HealthHandler
	Tenant          *handler.TenantHandler
}

func NewRouter(deps RouterDeps) http.Handler {
	r := chi.NewRouter()

	// Global middleware
	r.Use(chiMiddleware.RealIP)
	r.Use(middleware.RequestID)
	r.Use(middleware.Logging(deps.Logger))
	r.Use(middleware.Recovery(deps.Logger))

	// Health — no auth required
	r.Get("/health", deps.Health.Health)

	// Internal admin routes — protected by X-Admin-Token (no API key auth)
	r.Group(func(r chi.Router) {
		r.Use(middleware.AdminAuth(deps.AdminToken, deps.Logger))
		r.Route("/internal/tenants", func(r chi.Router) {
			r.Post("/", deps.Tenant.CreateTenant)
			r.Post("/{id}/api-keys", deps.Tenant.CreateAPIKey)
		})
	})

	// Authenticated API routes
	r.Group(func(r chi.Router) {
		r.Use(middleware.Auth(deps.Auth, deps.Logger))
		r.Use(middleware.RateLimit(deps.RateLimit, deps.RateLimitRPM, deps.Logger))

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
		})
	})

	return r
}
