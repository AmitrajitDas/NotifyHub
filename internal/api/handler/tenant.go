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

type TenantHandler struct {
	svc service.TenantService
}

func NewTenantHandler(svc service.TenantService) *TenantHandler {
	return &TenantHandler{svc: svc}
}

// POST /internal/tenants
func (h *TenantHandler) CreateTenant(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	var req domain.CreateTenantRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	tenant, err := h.svc.CreateTenant(r.Context(), req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusCreated, tenant, reqID)
}

// POST /internal/tenants/:id/api-keys
func (h *TenantHandler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	tenantID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid tenant id", nil), reqID)
		return
	}

	var req domain.CreateAPIKeyRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	result, svcErr := h.svc.CreateAPIKey(r.Context(), tenantID, req)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusCreated, result, reqID)
}
