package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// Processor runs the full delivery pipeline for a single queue.Message.
// It is stateless across calls — safe to invoke concurrently from the pool.
type Processor struct {
	notifRepo  repository.NotificationRepository
	delivRepo  repository.DeliveryRepository
	prefs      service.PreferenceService
	rateLimit  service.RateLimitService
	templates  service.TemplateService
	registry   *provider.Registry
	logger     *slog.Logger
	rateCaps   map[domain.Channel]int // per-channel hourly cap from config
	retryDelay time.Duration          // base delay for exponential backoff
}

// NewProcessor wires up a Processor with all its dependencies.
// rateCaps maps each channel to its per-hour delivery cap (e.g. email→10).
// retryDelay is the base backoff delay; actual delay = retryDelay * 2^(attempt-1).
func NewProcessor(
	notifRepo repository.NotificationRepository,
	delivRepo repository.DeliveryRepository,
	prefs service.PreferenceService,
	rateLimit service.RateLimitService,
	templates service.TemplateService,
	registry *provider.Registry,
	rateCaps map[domain.Channel]int,
	retryDelay time.Duration,
	logger *slog.Logger,
) *Processor {
	return &Processor{
		notifRepo:  notifRepo,
		delivRepo:  delivRepo,
		prefs:      prefs,
		rateLimit:  rateLimit,
		templates:  templates,
		registry:   registry,
		rateCaps:   rateCaps,
		retryDelay: retryDelay,
		logger:     logger,
	}
}

// Process runs the delivery pipeline for msg. It returns:
//   - nil      → processing complete (delivered, dropped, or permanently failed).
//     The pool MUST commit the Kafka offset.
//   - non-nil  → infrastructure failure (DB/Redis down).
//     The pool must NOT commit; Kafka will redeliver when the worker recovers.
func (p *Processor) Process(ctx context.Context, msg queue.Message) error {
	log := p.logger.With(
		slog.String("notification_id", msg.NotificationID.String()),
		slog.String("channel", string(msg.Channel)),
		slog.String("recipient_id", msg.RecipientID),
		slog.Int("attempt", msg.Attempt),
	)

	// ── Step 1: fetch canonical notification from DB ──────────────────────────
	n, err := p.notifRepo.GetByID(ctx, msg.NotificationID)
	if err != nil {
		return fmt.Errorf("fetch notification: %w", err)
	}

	// ── Step 2: mark as processing ────────────────────────────────────────────
	if _, err := p.notifRepo.UpdateStatus(ctx, n.ID, domain.StatusProcessing); err != nil {
		return fmt.Errorf("mark processing: %w", err)
	}

	// ── Step 3: preference check ──────────────────────────────────────────────
	allowed, reason, err := p.prefs.IsAllowed(ctx, n.RecipientID, n.Channel)
	if err != nil {
		return fmt.Errorf("preference check: %w", err)
	}
	if !allowed {
		log.InfoContext(ctx, "notification dropped: preference", slog.String("reason", reason))
		p.drop(ctx, msg, n, reason)
		return nil
	}

	// ── Step 4: rate limit check ──────────────────────────────────────────────
	if cap := p.rateCaps[n.Channel]; cap > 0 {
		ok, err := p.rateLimit.Allow(ctx, n.RecipientID, n.Channel, 60, cap)
		if err != nil {
			return fmt.Errorf("rate limit check: %w", err)
		}
		if !ok {
			log.InfoContext(ctx, "notification dropped: rate limit")
			p.drop(ctx, msg, n, "rate limit exceeded")
			return nil
		}
	}

	// ── Step 5: template render ───────────────────────────────────────────────
	// Clone payload so we don't mutate the original.
	payload := make(map[string]any, len(n.Payload)+2)
	for k, v := range n.Payload {
		payload[k] = v
	}

	if n.TemplateID != nil {
		subject, body, err := p.templates.Render(ctx, *n.TemplateID, n.Payload)
		if err != nil {
			// Render failure is permanent — a bad template can never succeed on retry.
			log.ErrorContext(ctx, "template render failed", slog.Any("error", err))
			errMsg := err.Error()
			p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &errMsg, nil, nil)
			p.updateStatus(ctx, n.ID, domain.StatusFailed)
			return nil
		}
		// Expose rendered content to the provider via reserved payload keys.
		payload["_subject"] = subject
		payload["_body"] = body
	}

	// Build the notification the provider will receive, with rendered payload.
	notification := *n
	notification.Payload = payload

	// ── Step 6: provider send (with inline retry for transient errors) ────────
	prov, err := p.registry.Get(n.Channel)
	if err != nil {
		log.ErrorContext(ctx, "no provider for channel")
		errMsg := err.Error()
		p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &errMsg, nil, nil)
		p.updateStatus(ctx, n.ID, domain.StatusFailed)
		return nil
	}

	sendErr := p.sendWithRetry(ctx, prov, notification, msg, log)

	// ── Step 7: write delivery log + final status ─────────────────────────────
	if sendErr != nil {
		errMsg := sendErr.Error()
		p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &errMsg, nil, nil)
		p.updateStatus(ctx, n.ID, domain.StatusFailed)
		log.ErrorContext(ctx, "notification failed", slog.Any("error", sendErr))
		return nil
	}

	now := time.Now().UTC()
	p.logDelivery(ctx, msg, n, domain.DeliverySuccess, nil, nil, &now)
	p.updateStatus(ctx, n.ID, domain.StatusDelivered)
	log.InfoContext(ctx, "notification delivered")

	return nil
}

// sendWithRetry calls provider.Send and retries on temporary errors using
// exponential backoff up to msg.MaxAttempts. Returns nil on success, the
// final error if all attempts are exhausted or the error is permanent.
func (p *Processor) sendWithRetry(
	ctx context.Context,
	prov provider.Provider,
	n domain.Notification,
	msg queue.Message,
	log *slog.Logger,
) error {
	attempt := msg.Attempt

	for {
		err := prov.Send(ctx, n)
		if err == nil {
			return nil
		}

		if provider.IsPermanent(err) || attempt >= msg.MaxAttempts {
			return err
		}

		// Temporary error with retries remaining — exponential backoff.
		delay := p.retryDelay * time.Duration(math.Pow(2, float64(attempt-1)))
		log.WarnContext(ctx, "temporary send error, retrying",
			slog.Any("error", err),
			slog.Int("attempt", attempt),
			slog.Duration("backoff", delay),
		)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
		case <-time.After(delay):
		}

		attempt++
	}
}

// drop marks the notification as dropped and writes a delivery log entry.
// Errors are best-effort — logged, never propagated.
func (p *Processor) drop(ctx context.Context, msg queue.Message, n *domain.Notification, reason string) {
	p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &reason, nil, nil)
	p.updateStatus(ctx, n.ID, domain.StatusDropped)
}

// logDelivery persists a delivery attempt record. Non-fatal: errors are logged only.
func (p *Processor) logDelivery(
	ctx context.Context,
	msg queue.Message,
	n *domain.Notification,
	status domain.DeliveryStatus,
	errMsg *string,
	errCode *string,
	deliveredAt *time.Time,
) {
	provName := string(n.Channel) // fallback label if no provider is registered
	if prov, err := p.registry.Get(n.Channel); err == nil {
		provName = string(prov.Channel())
	}

	entry := domain.DeliveryLog{
		NotificationID: n.ID,
		Channel:        n.Channel,
		Provider:       provName,
		Status:         status,
		ErrorMessage:   errMsg,
		ErrorCode:      errCode,
		RetryCount:     msg.Attempt - 1,
		AttemptedAt:    time.Now().UTC(),
		DeliveredAt:    deliveredAt,
	}

	if _, err := p.delivRepo.Create(ctx, entry); err != nil {
		p.logger.ErrorContext(ctx, "failed to write delivery log",
			slog.String("notification_id", n.ID.String()),
			slog.Any("error", err),
		)
	}
}

// updateStatus updates the notification status. Non-fatal: errors are logged only.
func (p *Processor) updateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus) {
	if _, err := p.notifRepo.UpdateStatus(ctx, id, status); err != nil {
		p.logger.ErrorContext(ctx, "failed to update notification status",
			slog.String("notification_id", id.String()),
			slog.String("status", string(status)),
			slog.Any("error", err),
		)
	}
}
