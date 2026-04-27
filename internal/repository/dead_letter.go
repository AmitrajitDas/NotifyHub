package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// DeadLetterRepository persists and queries dead-letter messages.
type DeadLetterRepository interface {
	Create(ctx context.Context, msg domain.DeadLetterMessage) (*domain.DeadLetterMessage, error)
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.DeadLetterMessage, error)
	List(ctx context.Context, params domain.DeadLetterListParams) ([]domain.DeadLetterMessage, error)
	MarkReplayed(ctx context.Context, tenantID, id uuid.UUID) error
	Delete(ctx context.Context, tenantID, id uuid.UUID) error
	CountByChannel(ctx context.Context, tenantID uuid.UUID) (map[string]int64, error)
}

type postgresDeadLetterRepo struct {
	q *db.Queries
}

func NewDeadLetterRepository(q *db.Queries) DeadLetterRepository {
	return &postgresDeadLetterRepo{q: q}
}

func (r *postgresDeadLetterRepo) Create(ctx context.Context, msg domain.DeadLetterMessage) (*domain.DeadLetterMessage, error) {
	row, err := r.q.CreateDeadLetter(ctx, db.CreateDeadLetterParams{
		TenantID:       msg.TenantID,
		NotificationID: msg.NotificationID,
		Channel:        string(msg.Channel),
		OriginalTopic:  msg.OriginalTopic,
		Payload:        json.RawMessage(msg.Payload),
		FailureReason:  msg.FailureReason,
		ErrorMessage:   msg.ErrorMessage,
		Attempts:       int32(msg.Attempts),
	})
	if err != nil {
		return nil, fmt.Errorf("create dead letter: %w", err)
	}
	out := fromDBDeadLetter(row)
	return &out, nil
}

func (r *postgresDeadLetterRepo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.DeadLetterMessage, error) {
	row, err := r.q.GetDeadLetterByID(ctx, db.GetDeadLetterByIDParams{
		ID:       id,
		TenantID: tenantID,
	})
	if err != nil {
		return nil, fmt.Errorf("get dead letter: %w", err)
	}
	out := fromDBDeadLetter(row)
	return &out, nil
}

func (r *postgresDeadLetterRepo) List(ctx context.Context, params domain.DeadLetterListParams) ([]domain.DeadLetterMessage, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 50
	}
	unreplayedOnly := false
	if params.Unreplayed != nil && *params.Unreplayed {
		unreplayedOnly = true
	}
	rows, err := r.q.ListDeadLetters(ctx, db.ListDeadLettersParams{
		TenantID:       params.TenantID,
		Channel:        params.Channel,
		UnreplayedOnly: unreplayedOnly,
		Limit:          int32(limit),
		Offset:         int32(params.Offset),
	})
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	out := make([]domain.DeadLetterMessage, len(rows))
	for i, row := range rows {
		out[i] = fromDBDeadLetter(row)
	}
	return out, nil
}

func (r *postgresDeadLetterRepo) MarkReplayed(ctx context.Context, tenantID, id uuid.UUID) error {
	if err := r.q.MarkDeadLetterReplayed(ctx, db.MarkDeadLetterReplayedParams{
		ID:       id,
		TenantID: tenantID,
	}); err != nil {
		return fmt.Errorf("mark dead letter replayed: %w", err)
	}
	return nil
}

func (r *postgresDeadLetterRepo) Delete(ctx context.Context, tenantID, id uuid.UUID) error {
	if err := r.q.DeleteDeadLetter(ctx, db.DeleteDeadLetterParams{
		ID:       id,
		TenantID: tenantID,
	}); err != nil {
		return fmt.Errorf("delete dead letter: %w", err)
	}
	return nil
}

func (r *postgresDeadLetterRepo) CountByChannel(ctx context.Context, tenantID uuid.UUID) (map[string]int64, error) {
	rows, err := r.q.CountDeadLettersByChannel(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("count dead letters by channel: %w", err)
	}
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		out[row.Channel] = row.Count
	}
	return out, nil
}

func fromDBDeadLetter(row db.DeadLetterMessage) domain.DeadLetterMessage {
	return domain.DeadLetterMessage{
		ID:             row.ID,
		TenantID:       row.TenantID,
		NotificationID: row.NotificationID,
		Channel:        domain.Channel(row.Channel),
		OriginalTopic:  row.OriginalTopic,
		Payload:        json.RawMessage(row.Payload),
		FailureReason:  row.FailureReason,
		ErrorMessage:   row.ErrorMessage,
		Attempts:       int(row.Attempts),
		ReplayedAt:     fromNullTime(row.ReplayedAt),
		CreatedAt:      row.CreatedAt,
	}
}
