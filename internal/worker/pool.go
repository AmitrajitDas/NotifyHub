package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/amitrajitdas31/notifyhub/internal/queue"
)

// workItem pairs the raw Kafka message (needed for offset commit) with the
// decoded queue.Message (needed by the processor).
type workItem struct {
	raw     kafka.Message
	message queue.Message
}

// Pool manages a fixed set of worker goroutines for a single channel.
// One Pool is created per channel (email, sms, push, inapp).
type Pool struct {
	consumer    *queue.Consumer
	processor   *Processor
	concurrency int
	logger      *slog.Logger
}

// NewPool creates a Pool. Goroutines are not started until Run is called.
func NewPool(consumer *queue.Consumer, processor *Processor, concurrency int, logger *slog.Logger) *Pool {
	return &Pool{
		consumer:    consumer,
		processor:   processor,
		concurrency: concurrency,
		logger:      logger,
	}
}

// Run starts the worker goroutines and the fetch loop. It blocks until ctx is
// cancelled, all in-flight messages are processed, and the consumer is closed.
// Safe to call in a goroutine alongside other Pool instances.
func (p *Pool) Run(ctx context.Context) error {
	work := make(chan workItem, p.concurrency)

	var wg sync.WaitGroup

	// ── Spin up worker goroutines ─────────────────────────────────────────────
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				err := p.processor.Process(ctx, item.message)
				if err != nil {
					// Infrastructure failure — skip commit so Kafka redelivers.
					p.logger.ErrorContext(ctx, "process failed, skipping commit",
						slog.String("notification_id", item.message.NotificationID.String()),
						slog.Any("error", err),
					)
					continue
				}
				if err := p.consumer.Commit(ctx, item.raw); err != nil {
					p.logger.ErrorContext(ctx, "offset commit failed",
						slog.String("notification_id", item.message.NotificationID.String()),
						slog.Any("error", err),
					)
				}
			}
		}()
	}

	// ── Fetch loop (runs on calling goroutine) ────────────────────────────────
	for {
		raw, err := p.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled — clean shutdown.
				break
			}
			// Transient consumer error — log and continue.
			p.logger.ErrorContext(ctx, "kafka fetch error", slog.Any("error", err))
			continue
		}

		var msg queue.Message
		if err := json.Unmarshal(raw.Value, &msg); err != nil {
			// Poison pill — undecodeable message will never succeed; commit and skip.
			p.logger.ErrorContext(ctx, "failed to decode message, skipping",
				slog.Any("error", err),
				slog.Int("bytes", len(raw.Value)),
			)
			if cerr := p.consumer.Commit(ctx, raw); cerr != nil {
				p.logger.ErrorContext(ctx, "commit of poison pill failed", slog.Any("error", cerr))
			}
			continue
		}

		work <- workItem{raw: raw, message: msg}
	}

	// ── Drain: close work channel and wait for all goroutines to finish ───────
	close(work)
	wg.Wait()

	if err := p.consumer.Close(); err != nil {
		p.logger.Error("consumer close error", slog.Any("error", err))
	}

	return nil
}
