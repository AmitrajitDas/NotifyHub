package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// NotificationRepository defines DB operations for notifications.
type NotificationRepository interface {
	Create(ctx context.Context, req domain.SendRequest) (*domain.Notification, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error)
	List(ctx context.Context, filter domain.ListNotificationsFilter) ([]domain.Notification, int64, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus) (*domain.Notification, error)
	// ListDueScheduled returns up to limit pending notifications whose scheduled_at <= NOW().
	ListDueScheduled(ctx context.Context, limit int) ([]domain.Notification, error)
}

type postgresNotificationRepo struct {
	q *db.Queries
}

func NewNotificationRepository(q *db.Queries) NotificationRepository {
	return &postgresNotificationRepo{q: q}
}

func (r *postgresNotificationRepo) Create(ctx context.Context, req domain.SendRequest) (*domain.Notification, error) {
	priority := req.Priority
	if priority == 0 {
		priority = domain.PriorityNormal
	}

	row, err := r.q.InsertNotification(ctx, db.InsertNotificationParams{
		IdempotencyKey:   toNullString(req.IdempotencyKey),
		Type:             req.Type,
		Channel:          string(req.Channel),
		RecipientID:      req.RecipientID,
		RecipientAddress: toNullString(&req.RecipientAddress),
		TemplateID:       toNullUUID(req.TemplateID),
		Payload:          toRawMessage(req.Payload),
		Priority:         int32(priority),
		Status:           string(domain.StatusPending),
		ScheduledAt:      toNullTime(req.ScheduledAt),
	})
	if err != nil {
		return nil, domain.NewInternalError("failed to insert notification", err)
	}
	n := dbNotificationToDomain(row)
	return &n, nil
}

func (r *postgresNotificationRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	row, err := r.q.GetNotificationByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification not found")
		}
		return nil, domain.NewInternalError("failed to get notification", err)
	}
	n := dbNotificationToDomain(row)
	return &n, nil
}

func (r *postgresNotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	row, err := r.q.GetNotificationByIdempotencyKey(ctx, toNullString(&key))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil // not found = no duplicate, safe to proceed
		}
		return nil, domain.NewInternalError("idempotency check failed", err)
	}
	n := dbNotificationToDomain(row)
	return &n, nil
}

func (r *postgresNotificationRepo) List(ctx context.Context, filter domain.ListNotificationsFilter) ([]domain.Notification, int64, error) {
	if filter.Page < 1 {
		filter.Page = 1
	}
	if filter.PerPage < 1 {
		filter.PerPage = 20
	}

	rows, err := r.q.ListNotifications(ctx, db.ListNotificationsParams{
		RecipientID: filter.RecipientID,
		Channel:     string(filter.Channel),
		Status:      string(filter.Status),
		Limit:       int32(filter.PerPage),
		Offset:      int32((filter.Page - 1) * filter.PerPage),
	})
	if err != nil {
		return nil, 0, domain.NewInternalError("failed to list notifications", err)
	}
	total, err := r.q.CountNotifications(ctx, db.CountNotificationsParams{
		RecipientID: filter.RecipientID,
		Channel:     string(filter.Channel),
		Status:      string(filter.Status),
	})
	if err != nil {
		return nil, 0, domain.NewInternalError("failed to count notifications", err)
	}

	out := make([]domain.Notification, len(rows))
	for i, row := range rows {
		out[i] = dbNotificationToDomain(row)
	}
	return out, total, nil
}

func (r *postgresNotificationRepo) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	row, err := r.q.CancelNotification(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification not found or not cancellable")
		}
		return nil, domain.NewInternalError("failed to cancel notification", err)
	}
	n := dbNotificationToDomain(row)
	return &n, nil
}

func (r *postgresNotificationRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus) (*domain.Notification, error) {
	row, err := r.q.UpdateNotificationStatus(ctx, db.UpdateNotificationStatusParams{
		ID:     id,
		Status: string(status),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("notification not found")
		}
		return nil, domain.NewInternalError("failed to update notification status", err)
	}
	n := dbNotificationToDomain(row)
	return &n, nil
}

func (r *postgresNotificationRepo) ListDueScheduled(ctx context.Context, limit int) ([]domain.Notification, error) {
	rows, err := r.q.ListDueScheduledNotifications(ctx, int32(limit))
	if err != nil {
		return nil, domain.NewInternalError("failed to list due scheduled notifications", err)
	}
	out := make([]domain.Notification, len(rows))
	for i, row := range rows {
		out[i] = dbNotificationToDomain(row)
	}
	return out, nil
}

func dbNotificationToDomain(n db.Notification) domain.Notification {
	return domain.Notification{
		ID:               n.ID,
		IdempotencyKey:   fromNullString(n.IdempotencyKey),
		Type:             n.Type,
		Channel:          domain.Channel(n.Channel),
		RecipientID:      n.RecipientID,
		RecipientAddress: n.RecipientAddress.String,
		TemplateID:       fromNullUUID(n.TemplateID),
		Payload:          fromRawMessage(n.Payload),
		Priority:         domain.Priority(n.Priority),
		Status:           domain.NotificationStatus(n.Status),
		ScheduledAt:      fromNullTime(n.ScheduledAt),
		CreatedAt:        n.CreatedAt,
		UpdatedAt:        n.UpdatedAt,
	}
}
