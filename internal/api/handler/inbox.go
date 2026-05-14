package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/auth"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/realtime"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// InboxHandler serves inbox REST endpoints and the WebSocket stream.
type InboxHandler struct {
	repo        repository.InAppRepository
	hub         *realtime.Hub
	wsToken     *auth.WSTokenService
	metrics     *observability.Metrics
	logger      *slog.Logger
	heartbeat   time.Duration
	readTimeout time.Duration
}

// InboxHandlerConfig holds tunable parameters for InboxHandler.
type InboxHandlerConfig struct {
	HeartbeatSeconds   int
	ReadTimeoutSeconds int
}

func NewInboxHandler(
	repo repository.InAppRepository,
	hub *realtime.Hub,
	wsToken *auth.WSTokenService,
	metrics *observability.Metrics,
	logger *slog.Logger,
	cfg InboxHandlerConfig,
) *InboxHandler {
	hb := time.Duration(cfg.HeartbeatSeconds) * time.Second
	if hb <= 0 {
		hb = 25 * time.Second
	}
	rt := time.Duration(cfg.ReadTimeoutSeconds) * time.Second
	if rt <= 0 {
		rt = 60 * time.Second
	}
	return &InboxHandler{
		repo:        repo,
		hub:         hub,
		wsToken:     wsToken,
		metrics:     metrics,
		logger:      logger,
		heartbeat:   hb,
		readTimeout: rt,
	}
}

// recipientIDFromHeader extracts X-Recipient-ID header; returns 400 if absent.
func recipientIDFromHeader(r *http.Request) (string, *domain.AppError) {
	id := r.Header.Get("X-Recipient-ID")
	if id == "" {
		return "", domain.NewValidationError("X-Recipient-ID header required", nil)
	}
	return id, nil
}

// GET /api/v1/inbox
func (h *InboxHandler) List(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	recipientID, appErr := recipientIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	q := domain.InboxQuery{
		TenantID:    tenantID,
		RecipientID: recipientID,
		Limit:       50,
	}
	if v := r.URL.Query().Get("unread"); v == "true" {
		q.UnreadOnly = true
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 200 {
			q.Limit = n
		}
	}
	if v := r.URL.Query().Get("after_id"); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			q.AfterID = &id
		}
	}
	if q.AfterID == nil {
		if v := r.URL.Query().Get("offset"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				q.Offset = n
			}
		}
	}

	msgs, err := h.repo.List(r.Context(), q)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, msgs, reqID)
}

// GET /api/v1/inbox/unread-count
func (h *InboxHandler) UnreadCount(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	recipientID, appErr := recipientIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	count, err := h.repo.CountUnread(r.Context(), tenantID, recipientID)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, map[string]int64{"unread": count}, reqID)
}

// POST /api/v1/inbox/{id}/read
func (h *InboxHandler) MarkRead(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	recipientID, appErr := recipientIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid id", nil), reqID)
		return
	}

	if err := h.repo.MarkRead(r.Context(), tenantID, id, recipientID); err != nil {
		if appErr := toAppError(err); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// POST /api/v1/inbox/read-all
func (h *InboxHandler) MarkAllRead(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	recipientID, appErr := recipientIDFromHeader(r)
	if appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	if err := h.repo.MarkAllRead(r.Context(), tenantID, recipientID); err != nil {
		if appErr := toAppError(err); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/inbox/stream?token=<jwt>[&since=<message_id>]
//
// WebSocket stream for real-time in-app messages. Identity comes from the
// signed JWT; X-Recipient-ID is ignored.
//
// Reconnect protocol:
//  1. Client stores the `id` of the last received message frame.
//  2. On reconnect, client passes ?since=<last_id>.
//  3. Server subscribes to live pub/sub first (no-miss window), then fetches
//     the gap from DB and sends it as historical frames (type "history").
//  4. Live frames follow (type "message"). Client deduplicates by `id`.
//  5. Heartbeat pings (type "ping") keep the connection alive every ~25 s.
func (h *InboxHandler) Stream(w http.ResponseWriter, r *http.Request) {
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		http.Error(w, "token required", http.StatusUnauthorized)
		return
	}

	claims, err := h.wsToken.Verify(tokenStr)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	tenantID := claims.TenantID
	recipientID := claims.RecipientID

	var sinceID *uuid.UUID
	if v := r.URL.Query().Get("since"); v != "" {
		if id, err := uuid.Parse(v); err == nil {
			sinceID = &id
		}
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // allow cross-origin from same cluster / dev
	})
	if err != nil {
		h.logger.Warn("inapp ws: accept failed", slog.Any("error", err))
		return
	}
	defer conn.CloseNow()

	h.metrics.InAppWSConnections.Inc()
	defer h.metrics.InAppWSConnections.Dec()

	// Subscribe before fetching historical so no messages fall through the gap.
	ch, cancel := h.hub.Subscribe(tenantID.String(), recipientID)
	defer cancel()

	ctx := r.Context()

	// Replay gap: send missed messages as historical frames, then go live.
	// Client deduplicates by id in case a message arrives via both paths.
	if sinceID != nil {
		missed, err := h.repo.ListSince(ctx, tenantID, recipientID, *sinceID)
		if err != nil {
			h.logger.Warn("inapp ws: history fetch failed", slog.Any("error", err))
		} else {
			for _, msg := range missed {
				frame := struct {
					Type string            `json:"type"`
					Data domain.InAppMessage `json:"data"`
				}{"history", msg}
				b, _ := json.Marshal(frame)
				if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
					return
				}
			}
		}
	}

	ticker := time.NewTicker(h.heartbeat)
	defer ticker.Stop()

	// read pump in background — keeps connection alive and detects client disconnect
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		conn.SetReadLimit(512)
		for {
			_, _, err := conn.Read(ctx)
			if err != nil {
				return
			}
		}
	}()

	ping := json.RawMessage(`{"type":"ping"}`)

	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "server shutting down")
			return
		case <-readDone:
			return
		case <-ticker.C:
			if err := conn.Write(ctx, websocket.MessageText, ping); err != nil {
				return
			}
		case raw, ok := <-ch:
			if !ok {
				conn.Close(websocket.StatusNormalClosure, "")
				return
			}
			// Wrap raw InAppMessage JSON with envelope so clients can distinguish
			// live frames (type "message") from historical replays (type "history").
			wrapped := make([]byte, 0, len(raw)+20)
			wrapped = append(wrapped, `{"type":"message","data":`...)
			wrapped = append(wrapped, raw...)
			wrapped = append(wrapped, '}')
			if err := conn.Write(ctx, websocket.MessageText, wrapped); err != nil {
				h.metrics.InAppWSDropped.Inc()
				return
			}
		}
	}
}
