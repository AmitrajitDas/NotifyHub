package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// TemplateRepository defines DB operations for templates.
type TemplateRepository interface {
	Create(ctx context.Context, req domain.CreateTemplateRequest) (*domain.Template, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error)
	GetByName(ctx context.Context, name string) (*domain.Template, error)
	List(ctx context.Context, channel domain.Channel, page, perPage int) ([]domain.Template, int64, error)
	Update(ctx context.Context, id uuid.UUID, req domain.UpdateTemplateRequest) (*domain.Template, error)
	Delete(ctx context.Context, id uuid.UUID) (*domain.Template, error)
}

type postgresTemplateRepo struct {
	q *db.Queries
}

func NewTemplateRepository(q *db.Queries) TemplateRepository {
	return &postgresTemplateRepo{q: q}
}

func (r *postgresTemplateRepo) Create(ctx context.Context, req domain.CreateTemplateRequest) (*domain.Template, error) {
	row, err := r.q.InsertTemplate(ctx, db.InsertTemplateParams{
		Name:            req.Name,
		Channel:         string(req.Channel),
		SubjectTemplate: toNullString(req.SubjectTemplate),
		BodyTemplate:    req.BodyTemplate,
		Metadata:        toNullRawMessage(req.Metadata),
	})
	if err != nil {
		return nil, domain.NewInternalError("failed to insert template", err)
	}
	t := dbTemplateToDomain(row)
	return &t, nil
}

func (r *postgresTemplateRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	row, err := r.q.GetTemplateByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("template not found")
		}
		return nil, domain.NewInternalError("failed to get template", err)
	}
	t := dbTemplateToDomain(row)
	return &t, nil
}

func (r *postgresTemplateRepo) GetByName(ctx context.Context, name string) (*domain.Template, error) {
	row, err := r.q.GetTemplateByName(ctx, name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("template not found")
		}
		return nil, domain.NewInternalError("failed to get template by name", err)
	}
	t := dbTemplateToDomain(row)
	return &t, nil
}

func (r *postgresTemplateRepo) List(ctx context.Context, channel domain.Channel, page, perPage int) ([]domain.Template, int64, error) {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 20
	}

	channelStr := string(channel)
	rows, err := r.q.ListTemplates(ctx, db.ListTemplatesParams{
		Channel: channelStr,
		Offset:  int32((page - 1) * perPage),
		Limit:   int32(perPage),
	})
	if err != nil {
		return nil, 0, domain.NewInternalError("failed to list templates", err)
	}
	total, err := r.q.CountTemplates(ctx, channelStr)
	if err != nil {
		return nil, 0, domain.NewInternalError("failed to count templates", err)
	}

	out := make([]domain.Template, len(rows))
	for i, row := range rows {
		out[i] = dbTemplateToDomain(row)
	}
	return out, total, nil
}

// Update applies a partial update. Fields that are nil in req are left unchanged.
// Because the generated UpdateTemplateParams uses non-nullable types for BodyTemplate
// and IsActive, we fetch the existing record first to fill any omitted fields.
func (r *postgresTemplateRepo) Update(ctx context.Context, id uuid.UUID, req domain.UpdateTemplateRequest) (*domain.Template, error) {
	existing, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	bodyTemplate := existing.BodyTemplate
	if req.BodyTemplate != nil {
		bodyTemplate = *req.BodyTemplate
	}

	isActive := existing.IsActive
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	subjectTemplate := existing.SubjectTemplate
	if req.SubjectTemplate != nil {
		subjectTemplate = req.SubjectTemplate
	}

	metadata := existing.Metadata
	if req.Metadata != nil {
		metadata = req.Metadata
	}

	row, err := r.q.UpdateTemplate(ctx, db.UpdateTemplateParams{
		ID:              id,
		SubjectTemplate: toNullString(subjectTemplate),
		BodyTemplate:    bodyTemplate,
		Metadata:        toNullRawMessage(metadata),
		IsActive:        isActive,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("template not found")
		}
		return nil, domain.NewInternalError("failed to update template", err)
	}
	t := dbTemplateToDomain(row)
	return &t, nil
}

func (r *postgresTemplateRepo) Delete(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	row, err := r.q.DeleteTemplate(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("template not found")
		}
		return nil, domain.NewInternalError("failed to delete template", err)
	}
	t := dbTemplateToDomain(row)
	return &t, nil
}

func dbTemplateToDomain(t db.Template) domain.Template {
	return domain.Template{
		ID:              t.ID,
		Name:            t.Name,
		Channel:         domain.Channel(t.Channel),
		SubjectTemplate: fromNullString(t.SubjectTemplate),
		BodyTemplate:    t.BodyTemplate,
		Metadata:        fromNullRawMessage(t.Metadata),
		Version:         int(t.Version),
		IsActive:        t.IsActive,
		CreatedAt:       t.CreatedAt,
		UpdatedAt:       t.UpdatedAt,
	}
}
