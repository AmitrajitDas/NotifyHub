package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// DLQPublisher is the subset of queue.Producer needed by the DLQ handler for replay.
type DLQPublisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// DLQHandler serves admin endpoints for dead-letter message inspection and replay.
type DLQHandler struct {
	repo           repository.DeadLetterRepository
	publisher      DLQPublisher
	maxAttempts    int
}

// NewDLQHandler wires up a DLQHandler.
func NewDLQHandler(
	repo repository.DeadLetterRepository,
	publisher DLQPublisher,
	maxAttempts int,
) *DLQHandler {
	return &DLQHandler{
		repo:        repo,
		publisher:   publisher,
		maxAttempts: maxAttempts,
	}
}

// GET /internal/dlq
// Query params: channel (optional), unreplayed=true (optional), limit, offset
func (h *DLQHandler) List(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	tenantID, appErr := tenantIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	params := domain.DeadLetterListParams{
		TenantID: tenantID,
		Channel:  r.URL.Query().Get("channel"),
		Limit:    50,
	}

	if v := r.URL.Query().Get("unreplayed"); v == "true" {
		t := true
		params.Unreplayed = &t
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			params.Limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			params.Offset = n
		}
	}

	msgs, err := h.repo.List(r.Context(), params)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, msgs, reqID)
}

// GET /internal/dlq/{id}
func (h *DLQHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	tenantID, appErr := tenantIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	msg, err := h.repo.GetByID(r.Context(), tenantID, id)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, msg, reqID)
}

// POST /internal/dlq/{id}/replay
// Re-publishes the original queue.Message to its original channel topic with
// Attempt reset to 1 so the worker processes it fresh.
func (h *DLQHandler) Replay(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	tenantID, appErr := tenantIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	msg, err := h.repo.GetByID(r.Context(), tenantID, id)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	// Deserialise original queue.Message from the DLQ envelope.
	var dlqMsg queue.DLQMessage
	if err := json.Unmarshal(msg.Payload, &dlqMsg); err != nil {
		response.JSONError(w, domain.NewInternalError("failed to parse dlq payload", err), reqID)
		return
	}

	// Reset attempt counter so the worker retries from scratch.
	original := dlqMsg.Original
	original.Attempt = 1
	original.MaxAttempts = h.maxAttempts
	original.EnqueuedAt = time.Now().UTC()

	payload, marshalErr := json.Marshal(original)
	if marshalErr != nil {
		response.JSONError(w, domain.NewInternalError("failed to marshal replay message", marshalErr), reqID)
		return
	}

	if err := h.publisher.Publish(r.Context(), dlqMsg.OriginalTopic, original.RecipientID, payload); err != nil {
		response.JSONError(w, domain.NewInternalError("failed to publish replay", err), reqID)
		return
	}

	if err := h.repo.MarkReplayed(r.Context(), tenantID, id); err != nil {
		// Non-fatal — message is already re-enqueued; log via response metadata only.
		response.JSON(w, http.StatusOK, map[string]string{
			"status":  "replayed",
			"warning": "mark_replayed failed: " + err.Error(),
		}, reqID)
		return
	}

	response.JSON(w, http.StatusOK, map[string]string{"status": "replayed"}, reqID)
}

// DELETE /internal/dlq/{id}
func (h *DLQHandler) Delete(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	tenantID, appErr := tenantIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	if err := h.repo.Delete(r.Context(), tenantID, id); err != nil {
		if appErr := toAppError(err); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// tenantIDFromHeader parses X-Tenant-ID from the request header.
// Admin endpoints are not API-key authenticated, so tenant ID is passed explicitly.
func tenantIDFromHeader(r *http.Request) (uuid.UUID, *domain.AppError) {
	raw := r.Header.Get("X-Tenant-ID")
	if raw == "" {
		return uuid.UUID{}, domain.NewValidationError("X-Tenant-ID header required", nil)
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, domain.NewValidationError("invalid X-Tenant-ID", nil)
	}
	return id, nil
}
