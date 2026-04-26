package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// ProcessorDeps groups all dependencies required to build a Processor.
// Using a struct keeps the constructor signature within linter limits and
// makes the call site self-documenting.
type ProcessorDeps struct {
	NotificationRepo repository.NotificationRepository
	DeliveryRepo     repository.DeliveryRepository
	Preferences      service.PreferenceService
	RateLimit        service.RateLimitService
	Templates        service.TemplateService
	Registry         *provider.Registry
	Metrics          *observability.Metrics
	RateCaps         map[domain.Channel]int // per-channel hourly cap from config
	RetryDelay       time.Duration          // base delay; actual = RetryDelay * 2^(attempt-1)
	Logger           *slog.Logger
}

// Processor runs the full delivery pipeline for a single queue.Message.
// It is stateless across calls — safe to invoke concurrently from the pool.
type Processor struct {
	deps ProcessorDeps
}

// NewProcessor wires up a Processor from a ProcessorDeps bundle.
func NewProcessor(deps ProcessorDeps) *Processor {
	return &Processor{deps: deps}
}

// Process runs the delivery pipeline for msg. Returns:
//   - nil      → processing complete (delivered, dropped, or permanently failed).
//     The pool MUST commit the Kafka offset.
//   - non-nil  → infrastructure failure (DB/Redis down).
//     The pool must NOT commit; Kafka will redeliver when the worker recovers.
func (p *Processor) Process(ctx context.Context, msg queue.Message) error {
	ctx, span := observability.Tracer("worker.processor").Start(ctx, "worker.Process",
		observability.WithSpanAttrs(
			attribute.String("notifyhub.tenant_id", msg.TenantID.String()),
			attribute.String("notifyhub.channel", string(msg.Channel)),
			attribute.String("notifyhub.notification_id", msg.NotificationID.String()),
			attribute.String("notifyhub.recipient_id", msg.RecipientID),
			attribute.Int("notifyhub.attempt", msg.Attempt),
		),
	)
	defer span.End()

	log := p.deps.Logger.With(
		slog.String("notification_id", msg.NotificationID.String()),
		slog.String("channel", string(msg.Channel)),
		slog.String("recipient_id", msg.RecipientID),
		slog.Int("attempt", msg.Attempt),
	)

	p.deps.Metrics.WorkerInflight.WithLabelValues(string(msg.Channel)).Inc()
	defer p.deps.Metrics.WorkerInflight.WithLabelValues(string(msg.Channel)).Dec()

	if !msg.EnqueuedAt.IsZero() {
		p.deps.Metrics.QueueLagSeconds.WithLabelValues(string(msg.Channel)).
			Set(time.Since(msg.EnqueuedAt).Seconds())
	}

	// outcome is the terminal status label written by the deferred counter.
	outcome := observability.StatusDelivered
	defer func() {
		p.deps.Metrics.NotifProcessed.WithLabelValues(string(msg.Channel), outcome).Inc()
	}()

	// ── Step 1: fetch ─────────────────────────────────────────────────────────
	n, err := p.deps.NotificationRepo.GetByID(ctx, msg.TenantID, msg.NotificationID)
	if err != nil {
		setSpanErr(span, "fetch notification", err)
		return fmt.Errorf("fetch notification: %w", err)
	}

	// ── Step 2: mark processing ───────────────────────────────────────────────
	if _, err := p.deps.NotificationRepo.UpdateStatus(ctx, n.ID, domain.StatusProcessing); err != nil {
		setSpanErr(span, "mark processing", err)
		return fmt.Errorf("mark processing: %w", err)
	}

	// ── Step 3: preference check ──────────────────────────────────────────────
	allowed, reason, err := p.deps.Preferences.IsAllowed(ctx, msg.TenantID, n.RecipientID, n.Channel)
	if err != nil {
		setSpanErr(span, "preference check", err)
		return fmt.Errorf("preference check: %w", err)
	}
	if !allowed {
		log.InfoContext(ctx, "notification dropped: preference", slog.String("reason", reason))
		outcome = observability.StatusDroppedPref
		p.drop(ctx, msg, n, reason)
		return nil
	}

	// ── Step 4: worker-side rate limit ────────────────────────────────────────
	if cap := p.deps.RateCaps[n.Channel]; cap > 0 {
		ok, err := p.deps.RateLimit.Allow(ctx, n.RecipientID, n.Channel, time.Hour, cap)
		if err != nil {
			setSpanErr(span, "rate limit check", err)
			return fmt.Errorf("rate limit check: %w", err)
		}
		rlLabel := observability.OutcomeAllowed
		if !ok {
			rlLabel = observability.OutcomeDenied
		}
		p.deps.Metrics.RateLimitDecisions.WithLabelValues(observability.ScopeWorker, rlLabel).Inc()
		if !ok {
			log.InfoContext(ctx, "notification dropped: rate limit")
			outcome = observability.StatusDroppedRateLim
			p.drop(ctx, msg, n, "rate limit exceeded")
			return nil
		}
	}

	// ── Steps 5-7: render + send + log ───────────────────────────────────────
	// deliver returns the terminal outcome label so the deferred counter is correct.
	termOutcome, infraErr := p.deliver(ctx, msg, n, log)
	if infraErr != nil {
		setSpanErr(span, "deliver", infraErr)
		return infraErr
	}
	outcome = termOutcome
	return nil
}

// deliver executes steps 5 (template render), 6 (provider send), and 7 (delivery log).
// It returns (outcomeLabel, nil) for all terminal outcomes — including permanent
// provider failures. It returns ("", non-nil) only for infrastructure errors
// that must block the Kafka offset commit.
func (p *Processor) deliver(ctx context.Context, msg queue.Message, n *domain.Notification, log *slog.Logger) (string, error) {
	// ── Step 5: template render ───────────────────────────────────────────────
	payload := make(map[string]any, len(n.Payload)+2)
	for k, v := range n.Payload {
		payload[k] = v
	}

	if n.TemplateID != nil {
		subject, body, err := p.deps.Templates.Render(ctx, msg.TenantID, *n.TemplateID, n.Payload)
		if err != nil {
			log.ErrorContext(ctx, "template render failed", slog.Any("error", err))
			errMsg := err.Error()
			p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &errMsg, nil, nil)
			p.updateStatus(ctx, n.ID, domain.StatusFailed)
			return observability.StatusFailed, nil
		}
		payload["_subject"] = subject
		payload["_body"] = body
	}

	notification := *n
	notification.Payload = payload

	// ── Step 6: provider send ─────────────────────────────────────────────────
	prov, err := p.deps.Registry.Get(n.Channel)
	if err != nil {
		log.ErrorContext(ctx, "no provider for channel")
		errMsg := err.Error()
		p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &errMsg, nil, nil)
		p.updateStatus(ctx, n.ID, domain.StatusFailed)
		return observability.StatusFailed, nil
	}

	sendErr := p.sendWithRetry(ctx, prov, notification, msg, log)

	// ── Step 7: delivery log + final status ───────────────────────────────────
	if sendErr != nil {
		errMsg := sendErr.Error()
		p.logDelivery(ctx, msg, n, domain.DeliveryFailed, &errMsg, nil, nil)
		p.updateStatus(ctx, n.ID, domain.StatusFailed)
		log.ErrorContext(ctx, "notification failed", slog.Any("error", sendErr))
		return observability.StatusFailed, nil
	}

	now := time.Now().UTC()
	p.logDelivery(ctx, msg, n, domain.DeliverySuccess, nil, nil, &now)
	p.updateStatus(ctx, n.ID, domain.StatusDelivered)
	log.InfoContext(ctx, "notification delivered")
	return observability.StatusDelivered, nil
}

// sendWithRetry calls provider.Send and retries on temporary errors using
// exponential backoff up to msg.MaxAttempts. Records per-call provider metrics.
func (p *Processor) sendWithRetry(
	ctx context.Context,
	prov provider.Provider,
	n domain.Notification,
	msg queue.Message,
	log *slog.Logger,
) error {
	attempt := msg.Attempt
	ch := string(n.Channel)
	provName := prov.Name()

	for {
		start := time.Now()
		err := prov.Send(ctx, n)
		dur := time.Since(start).Seconds()

		outcomeLabel := observability.OutcomeOK
		if err != nil {
			if provider.IsPermanent(err) {
				outcomeLabel = observability.OutcomePermFail
			} else {
				outcomeLabel = observability.OutcomeTempFail
			}
		}
		p.deps.Metrics.ProviderSendTotal.WithLabelValues(ch, provName, outcomeLabel).Inc()
		p.deps.Metrics.ProviderSendDur.WithLabelValues(ch, provName, outcomeLabel).Observe(dur)

		if err == nil {
			return nil
		}
		if provider.IsPermanent(err) || attempt >= msg.MaxAttempts {
			return err
		}

		delay := p.deps.RetryDelay * time.Duration(math.Pow(2, float64(attempt-1)))
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

// setSpanErr records the error and sets an error status on the span.
func setSpanErr(span trace.Span, op string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, op+" failed")
}

// drop marks the notification as dropped and writes a delivery log entry.
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
	provName := string(n.Channel)
	if prov, err := p.deps.Registry.Get(n.Channel); err == nil {
		provName = prov.Name()
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

	if _, err := p.deps.DeliveryRepo.Create(ctx, entry); err != nil {
		p.deps.Logger.ErrorContext(ctx, "failed to write delivery log",
			slog.String("notification_id", n.ID.String()),
			slog.Any("error", err),
		)
	}
}

// updateStatus updates the notification status. Non-fatal: errors are logged only.
func (p *Processor) updateStatus(ctx context.Context, id uuid.UUID, status domain.NotificationStatus) {
	if _, err := p.deps.NotificationRepo.UpdateStatus(ctx, id, status); err != nil {
		p.deps.Logger.ErrorContext(ctx, "failed to update notification status",
			slog.String("notification_id", id.String()),
			slog.String("status", string(status)),
			slog.Any("error", err),
		)
	}
}
