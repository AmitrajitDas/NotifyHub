package handler

import (
	"net/http"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/auth"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// WSTokenHandler issues short-lived JWTs for WebSocket authentication.
type WSTokenHandler struct {
	svc *auth.WSTokenService
}

func NewWSTokenHandler(svc *auth.WSTokenService) *WSTokenHandler {
	return &WSTokenHandler{svc: svc}
}

// POST /api/v1/ws-token
// Body: { "recipient_id": "..." }
// Returns: { "token": "<jwt>", "expires_in": 60 }
func (h *WSTokenHandler) Issue(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	var body struct {
		RecipientID string `json:"recipient_id"`
	}
	if appErr := response.DecodeJSON(r, &body); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}
	if body.RecipientID == "" {
		response.JSONError(w, domain.NewValidationError("recipient_id required", nil), reqID)
		return
	}

	token, err := h.svc.Issue(tenantID, body.RecipientID)
	if err != nil {
		response.JSONError(w, domain.NewInternalError("failed to issue token", err), reqID)
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"expires_in": 60,
	}, reqID)
}
