package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// TenantRepository defines DB operations for tenants.
type TenantRepository interface {
	Create(ctx context.Context, req domain.CreateTenantRequest) (*domain.Tenant, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error)
}

type postgresTenantRepo struct {
	q *db.Queries
}

func NewTenantRepository(q *db.Queries) TenantRepository {
	return &postgresTenantRepo{q: q}
}

func (r *postgresTenantRepo) Create(ctx context.Context, req domain.CreateTenantRequest) (*domain.Tenant, error) {
	row, err := r.q.InsertTenant(ctx, req.Name)
	if err != nil {
		return nil, domain.NewInternalError("failed to insert tenant", err)
	}
	t := dbTenantToDomain(row)
	return &t, nil
}

func (r *postgresTenantRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Tenant, error) {
	row, err := r.q.GetTenantByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("tenant not found")
		}
		return nil, domain.NewInternalError("failed to get tenant", err)
	}
	t := dbTenantToDomain(row)
	return &t, nil
}

func dbTenantToDomain(t db.Tenant) domain.Tenant {
	return domain.Tenant{
		ID:        t.ID,
		Name:      t.Name,
		IsActive:  t.IsActive,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
}
