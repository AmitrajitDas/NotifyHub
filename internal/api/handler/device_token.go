package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

type DeviceTokenHandler struct {
	svc service.DeviceTokenService
}

func NewDeviceTokenHandler(svc service.DeviceTokenService) *DeviceTokenHandler {
	return &DeviceTokenHandler{svc: svc}
}

// POST /api/v1/device-tokens
func (h *DeviceTokenHandler) Register(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	var req domain.RegisterDeviceTokenRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	dt, err := h.svc.Register(r.Context(), tenantID, req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusCreated, dt, reqID)
}

// DELETE /api/v1/device-tokens/:token
func (h *DeviceTokenHandler) Deregister(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID
	token := chi.URLParam(r, "token")

	if err := h.svc.Deregister(r.Context(), tenantID, token); err != nil {
		if appErr := toAppError(err); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
