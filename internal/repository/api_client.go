package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// APIClientRepository is the interface used by the auth middleware and tenant service.
type APIClientRepository interface {
	GetByKeyHash(ctx context.Context, hash string) (*domain.APIClient, error)
	CreateForTenant(ctx context.Context, tenantID uuid.UUID, name, keyHash string) (*domain.APIClient, error)
}

type postgresAPIClientRepo struct {
	q *db.Queries
}

func NewAPIClientRepository(q *db.Queries) APIClientRepository {
	return &postgresAPIClientRepo{q: q}
}

func (r *postgresAPIClientRepo) GetByKeyHash(ctx context.Context, hash string) (*domain.APIClient, error) {
	row, err := r.q.GetAPIClientByKeyHash(ctx, hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewUnauthorizedError("invalid API key")
		}
		return nil, domain.NewInternalError("api client lookup failed", err)
	}
	client := &domain.APIClient{
		ID:       row.ID,
		Name:     row.Name,
		IsActive: row.IsActive,
	}
	if row.TenantID.Valid {
		client.TenantID = row.TenantID.UUID
	}
	return client, nil
}

func (r *postgresAPIClientRepo) CreateForTenant(ctx context.Context, tenantID uuid.UUID, name, keyHash string) (*domain.APIClient, error) {
	row, err := r.q.InsertAPIClient(ctx, db.InsertAPIClientParams{
		TenantID:   uuidToNullUUID(tenantID),
		Name:       name,
		ApiKeyHash: keyHash,
	})
	if err != nil {
		return nil, domain.NewInternalError("failed to create api client", err)
	}
	return &domain.APIClient{
		ID:       row.ID,
		TenantID: tenantID,
		Name:     row.Name,
		IsActive: row.IsActive,
	}, nil
}
