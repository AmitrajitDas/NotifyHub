package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// WebhookService manages webhook endpoint registration and fanout dispatch.
type WebhookService interface {
	Create(ctx context.Context, tenantID uuid.UUID, req domain.CreateWebhookEndpointRequest) (*domain.WebhookEndpoint, error)
	List(ctx context.Context, tenantID uuid.UUID) ([]domain.WebhookEndpoint, error)
	GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.WebhookEndpoint, error)
	Update(ctx context.Context, tenantID, id uuid.UUID, req domain.UpdateWebhookEndpointRequest) (*domain.WebhookEndpoint, error)
	Delete(ctx context.Context, tenantID, id uuid.UUID) error
	// Dispatch fans out a webhook event for a notification to all active subscribed endpoints.
	// Called by the worker processor after each terminal notification outcome.
	Dispatch(ctx context.Context, n *domain.Notification, event string) error
}

// webhookPublisher is the subset of queue.Producer used by WebhookService.
type webhookPublisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

type webhookService struct {
	repo        repository.WebhookRepository
	publisher   webhookPublisher
	maxAttempts int
	validate    *validator.Validate
	logger      *slog.Logger
}

func NewWebhookService(
	repo repository.WebhookRepository,
	publisher webhookPublisher,
	maxAttempts int,
	validate *validator.Validate,
	logger *slog.Logger,
) WebhookService {
	return &webhookService{
		repo:        repo,
		publisher:   publisher,
		maxAttempts: maxAttempts,
		validate:    validate,
		logger:      logger,
	}
}

func (s *webhookService) Create(ctx context.Context, tenantID uuid.UUID, req domain.CreateWebhookEndpointRequest) (*domain.WebhookEndpoint, error) {
	if err := s.validate.Struct(req); err != nil {
		return nil, domain.NewValidationError("invalid webhook endpoint", nil)
	}
	if err := validateEvents(req.Events); err != nil {
		return nil, err
	}
	ep := domain.WebhookEndpoint{
		TenantID: tenantID,
		URL:      req.URL,
		Secret:   req.Secret,
		Events:   req.Events,
		IsActive: true,
	}
	return s.repo.CreateEndpoint(ctx, ep)
}

func (s *webhookService) List(ctx context.Context, tenantID uuid.UUID) ([]domain.WebhookEndpoint, error) {
	return s.repo.ListEndpoints(ctx, tenantID)
}

func (s *webhookService) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.WebhookEndpoint, error) {
	return s.repo.GetEndpoint(ctx, tenantID, id)
}

func (s *webhookService) Update(ctx context.Context, tenantID, id uuid.UUID, req domain.UpdateWebhookEndpointRequest) (*domain.WebhookEndpoint, error) {
	if err := s.validate.Struct(req); err != nil {
		return nil, domain.NewValidationError("invalid webhook endpoint", nil)
	}
	if err := validateEvents(req.Events); err != nil {
		return nil, err
	}
	ep := domain.WebhookEndpoint{
		ID:       id,
		TenantID: tenantID,
		URL:      req.URL,
		Events:   req.Events,
		IsActive: req.IsActive,
	}
	return s.repo.UpdateEndpoint(ctx, ep)
}

func (s *webhookService) Delete(ctx context.Context, tenantID, id uuid.UUID) error {
	return s.repo.DeleteEndpoint(ctx, tenantID, id)
}

// Dispatch looks up active endpoints subscribed to event, creates a delivery
// record for each, and publishes a WebhookMessage to the outbound Kafka topic.
// Errors per endpoint are logged and skipped — other endpoints still receive the event.
func (s *webhookService) Dispatch(ctx context.Context, n *domain.Notification, event string) error {
	endpoints, err := s.repo.ListActiveForEvent(ctx, n.TenantID, event)
	if err != nil {
		return fmt.Errorf("list active webhook endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return nil
	}

	eventPayload := buildEventPayload(n, event)

	for _, ep := range endpoints {
		delivery, err := s.repo.CreateDelivery(ctx, domain.WebhookDelivery{
			EndpointID:     ep.ID,
			NotificationID: &n.ID,
			Event:          event,
			Payload:        eventPayload,
			Status:         domain.WebhookDeliveryPending,
			Attempt:        0,
		})
		if err != nil {
			s.logger.ErrorContext(ctx, "webhook: failed to create delivery record",
				slog.String("endpoint_id", ep.ID.String()),
				slog.String("event", event),
				slog.Any("error", err),
			)
			continue
		}

		msg := queue.WebhookMessage{
			TenantID:       n.TenantID,
			EndpointID:     ep.ID,
			DeliveryID:     delivery.ID,
			NotificationID: &n.ID,
			Event:          event,
			Payload:        eventPayload,
			EndpointURL:    ep.URL,
			Secret:         ep.Secret,
			Attempt:        1,
			MaxAttempts:    s.maxAttempts,
			EnqueuedAt:     time.Now().UTC(),
		}

		msgBytes, err := json.Marshal(msg)
		if err != nil {
			s.logger.ErrorContext(ctx, "webhook: failed to marshal message",
				slog.String("delivery_id", delivery.ID.String()),
				slog.Any("error", err),
			)
			continue
		}

		if err := s.publisher.Publish(ctx, queue.WebhookTopic, ep.ID.String(), msgBytes); err != nil {
			s.logger.ErrorContext(ctx, "webhook: failed to publish message",
				slog.String("delivery_id", delivery.ID.String()),
				slog.Any("error", err),
			)
		}
	}
	return nil
}

// buildEventPayload constructs the JSON body sent to the webhook endpoint.
func buildEventPayload(n *domain.Notification, event string) map[string]any {
	return map[string]any{
		"event":           event,
		"notification_id": n.ID.String(),
		"tenant_id":       n.TenantID.String(),
		"channel":         string(n.Channel),
		"recipient_id":    n.RecipientID,
		"status":          string(n.Status),
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	}
}

// validateEvents rejects unknown event names.
func validateEvents(events []string) *domain.AppError {
	valid := make(map[string]bool, len(domain.AllWebhookEvents))
	for _, e := range domain.AllWebhookEvents {
		valid[e] = true
	}
	for _, e := range events {
		if !valid[e] {
			return domain.NewValidationError("unknown event: "+e, map[string]any{
				"valid_events": domain.AllWebhookEvents,
			})
		}
	}
	return nil
}
