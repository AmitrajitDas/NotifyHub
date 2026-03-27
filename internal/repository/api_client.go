package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// APIClientRepository is the interface used by the auth middleware.
type APIClientRepository interface {
	GetByKeyHash(ctx context.Context, hash string) (*domain.APIClient, error)
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
	return &domain.APIClient{
		ID:       row.ID,
		Name:     row.Name,
		IsActive: row.IsActive,
	}, nil
}
