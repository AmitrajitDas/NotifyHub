package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// DeliveryRepository defines DB operations for delivery logs.
type DeliveryRepository interface {
	Create(ctx context.Context, log domain.DeliveryLog) (*domain.DeliveryLog, error)
	GetLatest(ctx context.Context, notificationID uuid.UUID) (*domain.DeliveryLog, error)
	ListByNotification(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryLog, error)
}

type postgresDeliveryRepo struct {
	q *db.Queries
}

func NewDeliveryRepository(q *db.Queries) DeliveryRepository {
	return &postgresDeliveryRepo{q: q}
}

func (r *postgresDeliveryRepo) Create(ctx context.Context, log domain.DeliveryLog) (*domain.DeliveryLog, error) {
	row, err := r.q.InsertDeliveryLog(ctx, db.InsertDeliveryLogParams{
		NotificationID:    log.NotificationID,
		Channel:           string(log.Channel),
		Provider:          log.Provider,
		Status:            string(log.Status),
		ProviderMessageID: toNullString(log.ProviderMessageID),
		ErrorMessage:      toNullString(log.ErrorMessage),
		ErrorCode:         toNullString(log.ErrorCode),
		RetryCount:        int32(log.RetryCount),
		DeliveredAt:       toNullTime(log.DeliveredAt),
	})
	if err != nil {
		return nil, domain.NewInternalError("failed to insert delivery log", err)
	}
	d := dbDeliveryLogToDomain(row)
	return &d, nil
}

func (r *postgresDeliveryRepo) GetLatest(ctx context.Context, notificationID uuid.UUID) (*domain.DeliveryLog, error) {
	row, err := r.q.GetLatestDeliveryLog(ctx, notificationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("delivery log not found")
		}
		return nil, domain.NewInternalError("failed to get delivery log", err)
	}
	d := dbDeliveryLogToDomain(row)
	return &d, nil
}

func (r *postgresDeliveryRepo) ListByNotification(ctx context.Context, notificationID uuid.UUID) ([]domain.DeliveryLog, error) {
	rows, err := r.q.ListDeliveryLogsByNotification(ctx, notificationID)
	if err != nil {
		return nil, domain.NewInternalError("failed to list delivery logs", err)
	}
	out := make([]domain.DeliveryLog, len(rows))
	for i, row := range rows {
		out[i] = dbDeliveryLogToDomain(row)
	}
	return out, nil
}

func dbDeliveryLogToDomain(d db.DeliveryLog) domain.DeliveryLog {
	return domain.DeliveryLog{
		ID:                d.ID,
		NotificationID:    d.NotificationID,
		Channel:           domain.Channel(d.Channel),
		Provider:          d.Provider,
		Status:            domain.DeliveryStatus(d.Status),
		ProviderMessageID: fromNullString(d.ProviderMessageID),
		ErrorMessage:      fromNullString(d.ErrorMessage),
		ErrorCode:         fromNullString(d.ErrorCode),
		RetryCount:        int(d.RetryCount),
		AttemptedAt:       d.AttemptedAt,
		DeliveredAt:       fromNullTime(d.DeliveredAt),
	}
}
