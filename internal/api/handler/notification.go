package handler

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/api/middleware"
	"github.com/amitrajitdas31/notifyhub/internal/api/response"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

type NotificationHandler struct {
	svc service.NotificationService
}

func NewNotificationHandler(svc service.NotificationService) *NotificationHandler {
	return &NotificationHandler{svc: svc}
}

// POST /api/v1/notifications
func (h *NotificationHandler) Send(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	var req domain.SendRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	n, err := h.svc.Send(r.Context(), req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusAccepted, n, reqID)
}

// POST /api/v1/notifications/bulk
func (h *NotificationHandler) SendBulk(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	var req domain.BulkSendRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	results, errs := h.svc.SendBulk(r.Context(), req)

	// If everything failed and the first error is a known AppError, return 4xx.
	if len(results) == 0 && len(errs) > 0 {
		if appErr := toAppError(errs[0]); appErr != nil {
			response.JSONError(w, appErr, reqID)
			return
		}
	}

	type bulkResponse struct {
		Accepted []*domain.Notification `json:"accepted"`
		Errors   []string               `json:"errors,omitempty"`
	}

	out := bulkResponse{Accepted: results}
	for _, e := range errs {
		out.Errors = append(out.Errors, e.Error())
	}

	response.JSON(w, http.StatusAccepted, out, reqID)
}

// GET /api/v1/notifications/:id
func (h *NotificationHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid notification id", nil), reqID)
		return
	}

	n, svcErr := h.svc.GetByID(r.Context(), id)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, n, reqID)
}

// GET /api/v1/notifications
func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 20
	}

	filter := domain.ListNotificationsFilter{
		RecipientID: q.Get("recipient_id"),
		Channel:     domain.Channel(q.Get("channel")),
		Status:      domain.NotificationStatus(q.Get("status")),
		Page:        page,
		PerPage:     perPage,
	}

	notifications, total, svcErr := h.svc.List(r.Context(), filter)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSONList(w, http.StatusOK, notifications, page, perPage, total, reqID)
}

// DELETE /api/v1/notifications/:id
func (h *NotificationHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid notification id", nil), reqID)
		return
	}

	n, svcErr := h.svc.Cancel(r.Context(), id)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, n, reqID)
}
