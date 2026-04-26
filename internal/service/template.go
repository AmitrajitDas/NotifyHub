package service

import (
	"bytes"
	"context"
	"fmt"
	"text/template"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// TemplateService defines business logic for notification templates.
type TemplateService interface {
	Create(ctx context.Context, tenantID uuid.UUID, req domain.CreateTemplateRequest) (*domain.Template, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*domain.Template, error)
	GetByName(ctx context.Context, tenantID uuid.UUID, name string) (*domain.Template, error)
	List(ctx context.Context, tenantID uuid.UUID, channel domain.Channel, page, perPage int) ([]domain.Template, int64, error)
	Update(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, req domain.UpdateTemplateRequest) (*domain.Template, error)
	Delete(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*domain.Template, error)
	Preview(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, req domain.PreviewTemplateRequest) (*domain.PreviewTemplateResponse, error)
	Render(ctx context.Context, tenantID uuid.UUID, templateID uuid.UUID, payload map[string]any) (subject, body string, err error)
}

type templateService struct {
	repo      repository.TemplateRepository
	validator *validator.Validate
	metrics   *observability.Metrics
}

// NewTemplateService wires up the template service.
// metrics is used to record render latency via TemplateRenderDur.
func NewTemplateService(repo repository.TemplateRepository, v *validator.Validate, metrics *observability.Metrics) TemplateService {
	return &templateService{repo: repo, validator: v, metrics: metrics}
}

func (s *templateService) Create(ctx context.Context, tenantID uuid.UUID, req domain.CreateTemplateRequest) (*domain.Template, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}
	return s.repo.Create(ctx, tenantID, req)
}

func (s *templateService) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*domain.Template, error) {
	return s.repo.GetByID(ctx, tenantID, id)
}

func (s *templateService) GetByName(ctx context.Context, tenantID uuid.UUID, name string) (*domain.Template, error) {
	return s.repo.GetByName(ctx, tenantID, name)
}

func (s *templateService) List(ctx context.Context, tenantID uuid.UUID, channel domain.Channel, page, perPage int) ([]domain.Template, int64, error) {
	return s.repo.List(ctx, tenantID, channel, page, perPage)
}

func (s *templateService) Update(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, req domain.UpdateTemplateRequest) (*domain.Template, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}
	return s.repo.Update(ctx, tenantID, id, req)
}

func (s *templateService) Delete(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*domain.Template, error) {
	return s.repo.Delete(ctx, tenantID, id)
}

func (s *templateService) Preview(ctx context.Context, tenantID uuid.UUID, id uuid.UUID, req domain.PreviewTemplateRequest) (*domain.PreviewTemplateResponse, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}

	tmpl, err := s.repo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	body, err := renderTemplate("body", tmpl.BodyTemplate, req.Payload)
	if err != nil {
		return nil, domain.NewValidationError("failed to render body template", map[string]any{"error": err.Error()})
	}

	resp := &domain.PreviewTemplateResponse{Body: body}

	if tmpl.SubjectTemplate != nil {
		subject, err := renderTemplate("subject", *tmpl.SubjectTemplate, req.Payload)
		if err != nil {
			return nil, domain.NewValidationError("failed to render subject template", map[string]any{"error": err.Error()})
		}
		resp.Subject = subject
	}

	return resp, nil
}

// Render fetches and executes the template, recording render latency.
func (s *templateService) Render(ctx context.Context, tenantID uuid.UUID, templateID uuid.UUID, payload map[string]any) (subject, body string, err error) {
	start := time.Now()
	defer func() {
		s.metrics.TemplateRenderDur.Observe(time.Since(start).Seconds())
	}()

	tmpl, err := s.repo.GetByID(ctx, tenantID, templateID)
	if err != nil {
		return "", "", err
	}

	body, err = renderTemplate("body", tmpl.BodyTemplate, payload)
	if err != nil {
		return "", "", domain.NewInternalError("failed to render body template", err)
	}

	if tmpl.SubjectTemplate != nil {
		subject, err = renderTemplate("subject", *tmpl.SubjectTemplate, payload)
		if err != nil {
			return "", "", domain.NewInternalError("failed to render subject template", err)
		}
	}

	return subject, body, nil
}

// renderTemplate executes a Go text/template string against the given data map.
func renderTemplate(name, tmplStr string, data map[string]any) (string, error) {
	t, err := template.New(name).Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("parse template %q: %w", name, err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template %q: %w", name, err)
	}
	return buf.String(), nil
}
