package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

type InAppRepository interface {
	Insert(ctx context.Context, msg domain.InAppMessage) (*domain.InAppMessage, error)
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.InAppMessage, error)
	List(ctx context.Context, q domain.InboxQuery) ([]domain.InAppMessage, error)
	CountUnread(ctx context.Context, tenantID uuid.UUID, recipientID string) (int64, error)
	MarkRead(ctx context.Context, tenantID, id uuid.UUID, recipientID string) error
	MarkAllRead(ctx context.Context, tenantID uuid.UUID, recipientID string) error
}

type postgresInAppRepo struct {
	q *db.Queries
}

func NewInAppRepository(q *db.Queries) InAppRepository {
	return &postgresInAppRepo{q: q}
}

func (r *postgresInAppRepo) Insert(ctx context.Context, msg domain.InAppMessage) (*domain.InAppMessage, error) {
	row, err := r.q.InsertInAppMessage(ctx, db.InsertInAppMessageParams{
		TenantID:       msg.TenantID,
		NotificationID: toNullUUID(msg.NotificationID),
		RecipientID:    msg.RecipientID,
		Title:          msg.Title,
		Body:           msg.Body,
		Payload:        toRawMessage(msg.Payload),
	})
	if err != nil {
		return nil, fmt.Errorf("insert inapp message: %w", err)
	}
	out := fromDBInApp(row)
	return &out, nil
}

func (r *postgresInAppRepo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.InAppMessage, error) {
	row, err := r.q.GetInAppMessage(ctx, db.GetInAppMessageParams{
		ID:       id,
		TenantID: tenantID,
	})
	if err != nil {
		return nil, fmt.Errorf("get inapp message: %w", err)
	}
	out := fromDBInApp(row)
	return &out, nil
}

func (r *postgresInAppRepo) List(ctx context.Context, q domain.InboxQuery) ([]domain.InAppMessage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.q.ListInbox(ctx, db.ListInboxParams{
		TenantID:    q.TenantID,
		RecipientID: q.RecipientID,
		UnreadOnly:  q.UnreadOnly,
		Limit:       int32(limit),
		Offset:      int32(q.Offset),
	})
	if err != nil {
		return nil, fmt.Errorf("list inbox: %w", err)
	}
	out := make([]domain.InAppMessage, len(rows))
	for i, row := range rows {
		out[i] = fromDBInApp(row)
	}
	return out, nil
}

func (r *postgresInAppRepo) CountUnread(ctx context.Context, tenantID uuid.UUID, recipientID string) (int64, error) {
	count, err := r.q.CountUnread(ctx, db.CountUnreadParams{
		TenantID:    tenantID,
		RecipientID: recipientID,
	})
	if err != nil {
		return 0, fmt.Errorf("count unread: %w", err)
	}
	return count, nil
}

func (r *postgresInAppRepo) MarkRead(ctx context.Context, tenantID, id uuid.UUID, recipientID string) error {
	if err := r.q.MarkInAppMessageRead(ctx, db.MarkInAppMessageReadParams{
		ID:          id,
		TenantID:    tenantID,
		RecipientID: recipientID,
	}); err != nil {
		return fmt.Errorf("mark inapp read: %w", err)
	}
	return nil
}

func (r *postgresInAppRepo) MarkAllRead(ctx context.Context, tenantID uuid.UUID, recipientID string) error {
	if err := r.q.MarkAllInAppMessagesRead(ctx, db.MarkAllInAppMessagesReadParams{
		TenantID:    tenantID,
		RecipientID: recipientID,
	}); err != nil {
		return fmt.Errorf("mark all inapp read: %w", err)
	}
	return nil
}

func fromDBInApp(row db.InappMessage) domain.InAppMessage {
	return domain.InAppMessage{
		ID:             row.ID,
		TenantID:       row.TenantID,
		NotificationID: fromNullUUID(row.NotificationID),
		RecipientID:    row.RecipientID,
		Title:          row.Title,
		Body:           row.Body,
		Payload:        fromRawMessage(row.Payload),
		ReadAt:         fromNullTime(row.ReadAt),
		CreatedAt:      row.CreatedAt,
	}
}
