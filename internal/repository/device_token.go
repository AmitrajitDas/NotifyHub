package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

type DeviceTokenRepository interface {
	Upsert(ctx context.Context, tenantID uuid.UUID, req domain.RegisterDeviceTokenRequest) (*domain.DeviceToken, error)
	Deactivate(ctx context.Context, tenantID uuid.UUID, token string) error
	ListActiveByUser(ctx context.Context, tenantID uuid.UUID, userID string) ([]domain.DeviceToken, error)
}

type postgresDeviceTokenRepo struct {
	q *db.Queries
}

func NewDeviceTokenRepository(q *db.Queries) DeviceTokenRepository {
	return &postgresDeviceTokenRepo{q: q}
}

func (r *postgresDeviceTokenRepo) Upsert(ctx context.Context, tenantID uuid.UUID, req domain.RegisterDeviceTokenRequest) (*domain.DeviceToken, error) {
	row, err := r.q.UpsertDeviceToken(ctx, db.UpsertDeviceTokenParams{
		TenantID: tenantID,
		UserID:   req.UserID,
		Token:    req.Token,
		Platform: req.Platform,
	})
	if err != nil {
		return nil, err
	}
	return toDeviceToken(row), nil
}

func (r *postgresDeviceTokenRepo) Deactivate(ctx context.Context, tenantID uuid.UUID, token string) error {
	err := r.q.DeactivateDeviceToken(ctx, db.DeactivateDeviceTokenParams{
		TenantID: tenantID,
		Token:    token,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.NewNotFoundError("device token not found")
		}
		return err
	}
	return nil
}

func (r *postgresDeviceTokenRepo) ListActiveByUser(ctx context.Context, tenantID uuid.UUID, userID string) ([]domain.DeviceToken, error) {
	rows, err := r.q.ListActiveDeviceTokensByUser(ctx, db.ListActiveDeviceTokensByUserParams{
		TenantID: tenantID,
		UserID:   userID,
	})
	if err != nil {
		return nil, err
	}
	tokens := make([]domain.DeviceToken, len(rows))
	for i, row := range rows {
		tokens[i] = *toDeviceToken(row)
	}
	return tokens, nil
}

func toDeviceToken(row db.DeviceToken) *domain.DeviceToken {
	return &domain.DeviceToken{
		ID:        row.ID,
		TenantID:  row.TenantID,
		UserID:    row.UserID,
		Token:     row.Token,
		Platform:  row.Platform,
		IsActive:  row.IsActive,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}
}
