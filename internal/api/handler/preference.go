package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

type PreferenceHandler struct {
	svc service.PreferenceService
}

func NewPreferenceHandler(svc service.PreferenceService) *PreferenceHandler {
	return &PreferenceHandler{svc: svc}
}

// GET /api/v1/users/:user_id/preferences
func (h *PreferenceHandler) ListByUser(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	userID := chi.URLParam(r, "user_id")

	prefs, err := h.svc.ListByUser(r.Context(), userID)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, prefs, reqID)
}

// PUT /api/v1/users/:user_id/preferences/:channel
func (h *PreferenceHandler) Upsert(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	userID := chi.URLParam(r, "user_id")
	channel := domain.Channel(chi.URLParam(r, "channel"))

	if !channel.IsValid() {
		response.JSONError(w, domain.NewValidationError("invalid channel", map[string]any{
			"channel": "must be one of: email, push, sms, inapp",
		}), reqID)
		return
	}

	var req domain.UpsertPreferenceRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	pref, err := h.svc.Upsert(r.Context(), userID, channel, req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, pref, reqID)
}

// DELETE /api/v1/users/:user_id/preferences/:channel
func (h *PreferenceHandler) Delete(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	userID := chi.URLParam(r, "user_id")
	channel := domain.Channel(chi.URLParam(r, "channel"))

	if !channel.IsValid() {
		response.JSONError(w, domain.NewValidationError("invalid channel", map[string]any{
			"channel": "must be one of: email, push, sms, inapp",
		}), reqID)
		return
	}

	if err := h.svc.Delete(r.Context(), userID, channel); err != nil {
		if appErr := toAppError(err); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
