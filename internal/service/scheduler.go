package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// Scheduler polls the DB for due scheduled notifications and enqueues them.
// It also recovers immediate notifications that failed to enqueue at send time
// (they remain status=pending with scheduled_at=NULL until the next tick picks them up via the same query... actually those are handled by a separate concern).
// Runs in the worker process as a goroutine alongside the channel pools.
type Scheduler struct {
	repo      repository.NotificationRepository
	publisher Publisher
	logger    *slog.Logger
	interval  time.Duration
	batchSize int
	maxRetries int
}

// NewScheduler creates a Scheduler.
// interval: how often to poll (e.g. 15s).
// batchSize: max notifications to process per tick (e.g. 100).
// maxRetries: passed into the queue.Message envelope as MaxAttempts.
func NewScheduler(
	repo repository.NotificationRepository,
	publisher Publisher,
	interval time.Duration,
	batchSize int,
	maxRetries int,
	logger *slog.Logger,
) *Scheduler {
	return &Scheduler{
		repo:       repo,
		publisher:  publisher,
		interval:   interval,
		batchSize:  batchSize,
		maxRetries: maxRetries,
		logger:     logger,
	}
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.logger.InfoContext(ctx, "scheduler started",
		slog.Duration("interval", s.interval),
		slog.Int("batch_size", s.batchSize),
	)

	for {
		select {
		case <-ctx.Done():
			s.logger.InfoContext(ctx, "scheduler stopped")
			return nil
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick fetches one batch of due notifications and enqueues each one.
func (s *Scheduler) tick(ctx context.Context) {
	notifications, err := s.repo.ListDueScheduled(ctx, s.batchSize)
	if err != nil {
		s.logger.ErrorContext(ctx, "scheduler: failed to list due notifications", slog.Any("error", err))
		return
	}

	if len(notifications) == 0 {
		return
	}

	s.logger.InfoContext(ctx, "scheduler: enqueuing due notifications", slog.Int("count", len(notifications)))

	for i := range notifications {
		n := &notifications[i]
		if err := s.enqueue(ctx, n); err != nil {
			s.logger.ErrorContext(ctx, "scheduler: failed to enqueue notification",
				slog.String("notification_id", n.ID.String()),
				slog.Any("error", err),
			)
			continue
		}
		if _, err := s.repo.UpdateStatus(ctx, n.ID, domain.StatusQueued); err != nil {
			s.logger.ErrorContext(ctx, "scheduler: failed to mark notification queued",
				slog.String("notification_id", n.ID.String()),
				slog.Any("error", err),
			)
		}
	}

	// Warn if we hit the batch ceiling — more rows may be waiting for the next tick.
	if len(notifications) == s.batchSize {
		s.logger.WarnContext(ctx, "scheduler: batch full, more notifications may be pending",
			slog.Int("batch_size", s.batchSize),
		)
	}
}

// enqueue marshals n into a queue.Message and publishes it to the correct Kafka topic.
func (s *Scheduler) enqueue(ctx context.Context, n *domain.Notification) error {
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
		return fmt.Errorf("marshal message: %w", err)
	}

	return s.publisher.Publish(ctx, queue.TopicForChannel(n.Channel), n.RecipientID, data)
}
