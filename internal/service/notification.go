package service

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// Publisher is the queue abstraction the notification service depends on.
// Implemented by the queue package; a no-op stub is used in tests.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

//NotificationService defines business logic for sending and querying notifications.
type NotificationService interface {
	Send(ctx context.Context, req domain.SendRequest) (*domain.Notification, error)
	SendBulk(ctx context.Context, req domain.BulkSendRequest) ([]*domain.Notification, []error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	List(ctx context.Context, filter domain.ListNotificationsFilter) ([]domain.Notification, int64, error)
	Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
}

type notificationService struct {
	repo      repository.NotificationRepository
	publisher Publisher
	validator *validator.Validate
	maxRetries int
}

func NewNotificationService(
	repo repository.NotificationRepository,
	publisher Publisher,
	v *validator.Validate,
	maxRetries int,
) NotificationService {
	return &notificationService{
		repo:       repo,
		publisher:  publisher,
		validator:  v,
		maxRetries: maxRetries,
	}
}

func (s *notificationService) Send(ctx context.Context, req domain.SendRequest) (*domain.Notification, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}

	// Idempotency: return existing notification if this key was already processed.
	if req.IdempotencyKey != nil && *req.IdempotencyKey != "" {
		existing, err := s.repo.GetByIdempotencyKey(ctx, *req.IdempotencyKey)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
	}

	// Persist with status=pending.
	n, err := s.repo.Create(ctx, req)
	if err != nil {
		return nil, err
	}

	// Skip queuing for scheduled notifications — the scheduler will enqueue them later.
	if req.ScheduledAt != nil && req.ScheduledAt.After(time.Now()) {
		return n, nil
	}

	// Publish to channel topic.
	if err := s.enqueue(ctx, n); err != nil {
		// Non-fatal: notification is persisted; the scheduler/reconciler can retry.
		// Log the error but don't fail the API response.
		_ = err
		return n, nil
	}

	// Mark as queued.
	n, err = s.repo.UpdateStatus(ctx, n.ID, domain.StatusQueued)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (s *notificationService) SendBulk(ctx context.Context, req domain.BulkSendRequest) ([]*domain.Notification, []error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, []error{toValidationError(err)}
	}

	results := make([]*domain.Notification, 0, len(req.Recipients))
	errs := make([]error, 0)

	for _, recipient := range req.Recipients {
		// Merge base payload with per-recipient overrides.
		merged := make(map[string]any, len(req.Payload)+len(recipient.Payload))
		maps.Copy(merged, req.Payload)
		maps.Copy(merged, recipient.Payload)

		single := domain.SendRequest{
			Type:             req.Type,
			Channel:          req.Channel,
			RecipientID:      recipient.RecipientID,
			RecipientAddress: recipient.RecipientAddress,
			TemplateID:       req.TemplateID,
			Payload:          merged,
			Priority:         req.Priority,
		}

		n, err := s.Send(ctx, single)
		if err != nil {
			errs = append(errs, fmt.Errorf("recipient %s: %w", recipient.RecipientID, err))
			continue
		}
		results = append(results, n)
	}

	return results, errs
}

func (s *notificationService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *notificationService) List(ctx context.Context, filter domain.ListNotificationsFilter) ([]domain.Notification, int64, error) {
	return s.repo.List(ctx, filter)
}

func (s *notificationService) Cancel(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.repo.Cancel(ctx, id)
}

func (s *notificationService) enqueue(ctx context.Context, n *domain.Notification) error {
	msg := queue.Message{
		NotificationID:   n.ID,
		Channel:          n.Channel,
		RecipientID:      n.RecipientID,
		RecipientAddress: n.RecipientAddress,
		TemplateID:       n.TemplateID,
		Payload:          n.Payload,
		Priority:         n.Priority,
		Attempt:          1,
		MaxAttempts:      s.maxRetries,
		EnqueuedAt:       time.Now().UTC(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal notification message: %w", err)
	}

	return s.publisher.Publish(ctx, queue.TopicForChannel(n.Channel), n.RecipientID, data)
}
