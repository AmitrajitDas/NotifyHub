package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// WebhookHandler serves CRUD endpoints for webhook endpoint management.
type WebhookHandler struct {
	svc service.WebhookService
}

func NewWebhookHandler(svc service.WebhookService) *WebhookHandler {
	return &WebhookHandler{svc: svc}
}

// POST /api/v1/webhooks
func (h *WebhookHandler) Create(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	var req domain.CreateWebhookEndpointRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	ep, err := h.svc.Create(r.Context(), tenantID, req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusCreated, ep, reqID)
}

// GET /api/v1/webhooks
func (h *WebhookHandler) List(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	eps, err := h.svc.List(r.Context(), tenantID)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, eps, reqID)
}

// GET /api/v1/webhooks/{id}
func (h *WebhookHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	ep, err := h.svc.GetByID(r.Context(), tenantID, id)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, ep, reqID)
}

// PUT /api/v1/webhooks/{id}
func (h *WebhookHandler) Update(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	var req domain.UpdateWebhookEndpointRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	ep, err := h.svc.Update(r.Context(), tenantID, id, req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, ep, reqID)
}

// DELETE /api/v1/webhooks/{id}
func (h *WebhookHandler) Delete(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	if err := h.svc.Delete(r.Context(), tenantID, id); err != nil {
		if appErr := toAppError(err); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
