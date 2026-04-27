package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// DLQConsumer reads from a channel's DLQ Kafka topic and persists each entry
// to dead_letter_messages for operator inspection and replay.
// Offset is committed only after a successful DB insert — infrastructure
// errors leave the message unconsumed for reprocessing, matching the main pool.
type DLQConsumer struct {
	consumer *queue.Consumer
	repo     repository.DeadLetterRepository
	metrics  *observability.Metrics
	logger   *slog.Logger
}

// NewDLQConsumer wires up a DLQConsumer.
func NewDLQConsumer(
	consumer *queue.Consumer,
	repo repository.DeadLetterRepository,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *DLQConsumer {
	return &DLQConsumer{
		consumer: consumer,
		repo:     repo,
		metrics:  metrics,
		logger:   logger,
	}
}

// Run blocks, consuming the DLQ topic until ctx is cancelled.
func (c *DLQConsumer) Run(ctx context.Context) error {
	for {
		raw, err := c.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // clean shutdown
			}
			c.logger.ErrorContext(ctx, "dlq fetch error", slog.Any("error", err))
			continue
		}

		var dlq queue.DLQMessage
		if err := json.Unmarshal(raw.Value, &dlq); err != nil {
			c.logger.ErrorContext(ctx, "dlq message decode failed, skipping",
				slog.Any("error", err),
				slog.Int("bytes", len(raw.Value)),
			)
			_ = c.consumer.Commit(ctx, raw)
			continue
		}

		domainMsg := domain.DeadLetterMessage{
			TenantID:       dlq.Original.TenantID,
			NotificationID: dlq.Original.NotificationID,
			Channel:        dlq.Original.Channel,
			OriginalTopic:  dlq.OriginalTopic,
			Payload:        mustMarshal(dlq),
			FailureReason:  dlq.FailureReason,
			ErrorMessage:   dlq.ErrorMessage,
			Attempts:       dlq.LastAttempt,
		}

		if _, err := c.repo.Create(ctx, domainMsg); err != nil {
			// DB down — do not commit; Kafka will redeliver after recovery.
			c.logger.ErrorContext(ctx, "dlq persist failed, not committing",
				slog.String("channel", string(dlq.Original.Channel)),
				slog.Any("error", err),
			)
			continue
		}

		c.metrics.DLQPersisted.WithLabelValues(string(dlq.Original.Channel)).Inc()

		if err := c.consumer.Commit(ctx, raw); err != nil {
			c.logger.ErrorContext(ctx, "dlq offset commit failed", slog.Any("error", err))
		}
	}

	if err := c.consumer.Close(); err != nil {
		c.logger.Error("dlq consumer close error", slog.Any("error", err))
	}
	return nil
}

// mustMarshal serialises v to JSON; falls back to an error payload so the DB
// row is never left with an empty payload column.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{"error":"marshal failed","ts":"` + time.Now().UTC().Format(time.RFC3339) + `"}`)
	}
	return b
}
