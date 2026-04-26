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
// decoded queue.Message (needed by the processor) and the trace-enriched context
// restored from the message's W3C traceparent header.
type workItem struct {
	raw     kafka.Message
	message queue.Message
	ctx     context.Context // carries the span parent extracted from Kafka headers
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
	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go p.runWorker(ctx, &wg, work)
	}

	p.fetchLoop(ctx, work)

	close(work)
	wg.Wait()

	if err := p.consumer.Close(); err != nil {
		p.logger.Error("consumer close error", slog.Any("error", err))
	}

	return nil
}

// runWorker processes items from the work channel until it is closed.
func (p *Pool) runWorker(ctx context.Context, wg *sync.WaitGroup, work <-chan workItem) {
	defer wg.Done()
	for item := range work {
		// Use the trace-enriched context so the processor span links to the
		// originating API request span across the Kafka boundary.
		if err := p.processor.Process(item.ctx, item.message); err != nil {
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
}

// fetchLoop reads messages from Kafka until ctx is cancelled, decoding each
// message and dispatching it to the work channel.
func (p *Pool) fetchLoop(ctx context.Context, work chan<- workItem) {
	for {
		raw, err := p.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // clean shutdown
			}
			p.logger.ErrorContext(ctx, "kafka fetch error", slog.Any("error", err))
			continue
		}

		var msg queue.Message
		if err := json.Unmarshal(raw.Value, &msg); err != nil {
			// Poison pill — will never succeed; commit and skip.
			p.logger.ErrorContext(ctx, "failed to decode message, skipping",
				slog.Any("error", err),
				slog.Int("bytes", len(raw.Value)),
			)
			if commitErr := p.consumer.Commit(ctx, raw); commitErr != nil {
				p.logger.ErrorContext(ctx, "commit of bad message failed", slog.Any("error", commitErr))
			}
			continue
		}

		// Restore the parent trace context from the message's W3C headers.
		msgCtx := queue.ExtractTraceContext(ctx, raw)
		work <- workItem{raw: raw, message: msg, ctx: msgCtx}
	}
}
