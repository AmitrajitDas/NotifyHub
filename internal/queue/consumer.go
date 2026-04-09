package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer wraps kafka-go's Reader for a single topic + consumer group.
// One Consumer instance is created per channel (email, sms, push, inapp).
type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
}

// NewConsumer creates a Consumer that reads from topic as part of groupID.
// brokers: list of bootstrap broker addresses, e.g. ["localhost:9092"].
func NewConsumer(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,                // fetch as soon as any data is available
		MaxBytes:       10 << 20,         // 10 MB cap per fetch response
		MaxWait:        1 * time.Second,  // max idle wait; keeps ctx cancellation responsive
		CommitInterval: 0,                // disable auto-commit; we commit manually
		StartOffset:    kafka.LastOffset, // new groups start from latest, not beginning
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Debug(fmt.Sprintf(msg, args...))
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Error(fmt.Sprintf(msg, args...))
		}),
	})

	return &Consumer{reader: r, logger: logger}
}

// Fetch blocks until a message is available on the topic or ctx is cancelled.
// Returns the raw kafka.Message so the caller retains the offset metadata
// needed for a subsequent Commit call.
func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("kafka fetch: %w", err)
	}

	c.logger.DebugContext(ctx, "message fetched",
		slog.String("topic", msg.Topic),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
		slog.Int("bytes", len(msg.Value)),
	)

	return msg, nil
}

// Commit marks msg as processed in the consumer group.
// Must be called only after the message has been fully handled (delivered,
// dropped, or dead-lettered) to prevent re-delivery on restart.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka commit offset %d: %w", msg.Offset, err)
	}

	c.logger.DebugContext(ctx, "offset committed",
		slog.String("topic", msg.Topic),
		slog.Int("partition", msg.Partition),
		slog.Int64("offset", msg.Offset),
	)

	return nil
}

// Close triggers a consumer group rebalance so other replicas can pick up
// this instance's partitions. Must be called during graceful shutdown.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("kafka consumer close: %w", err)
	}
	return nil
}
