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

type TemplateHandler struct {
	svc service.TemplateService
}

func NewTemplateHandler(svc service.TemplateService) *TemplateHandler {
	return &TemplateHandler{svc: svc}
}

// POST /api/v1/templates
func (h *TemplateHandler) Create(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	var req domain.CreateTemplateRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	t, err := h.svc.Create(r.Context(), tenantID, req)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusCreated, t, reqID)
}

// GET /api/v1/templates
func (h *TemplateHandler) List(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 20
	}

	templates, total, err := h.svc.List(r.Context(), tenantID, domain.Channel(q.Get("channel")), page, perPage)
	if appErr := toAppError(err); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSONList(w, http.StatusOK, templates, page, perPage, total, reqID)
}

// GET /api/v1/templates/:id
func (h *TemplateHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid template id", nil), reqID)
		return
	}

	t, svcErr := h.svc.GetByID(r.Context(), tenantID, id)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, t, reqID)
}

// PUT /api/v1/templates/:id
func (h *TemplateHandler) Update(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid template id", nil), reqID)
		return
	}

	var req domain.UpdateTemplateRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	t, svcErr := h.svc.Update(r.Context(), tenantID, id, req)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, t, reqID)
}

// DELETE /api/v1/templates/:id
func (h *TemplateHandler) Delete(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid template id", nil), reqID)
		return
	}

	t, svcErr := h.svc.Delete(r.Context(), tenantID, id)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, t, reqID)
}

// POST /api/v1/templates/:id/preview
func (h *TemplateHandler) Preview(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.RequestIDFromContext(r.Context())
	tenantID := middleware.ClientFromContext(r.Context()).TenantID

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		response.JSONError(w, domain.NewValidationError("invalid template id", nil), reqID)
		return
	}

	var req domain.PreviewTemplateRequest
	if appErr := response.DecodeJSON(r, &req); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	preview, svcErr := h.svc.Preview(r.Context(), tenantID, id, req)
	if appErr := toAppError(svcErr); appErr != nil {
		response.JSONError(w, appErr, reqID)
		return
	}

	response.JSON(w, http.StatusOK, preview, reqID)
}
