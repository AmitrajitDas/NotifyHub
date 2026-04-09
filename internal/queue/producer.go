package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer wraps kafka-go's Writer and implements service.Publisher.
type Producer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewProducer creates a ready-to-use Kafka producer.
// brokers: list of bootstrap broker addresses, e.g. ["localhost:9092"].
func NewProducer(brokers []string, logger *slog.Logger) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{}, // same key → same partition (per-recipient ordering)
		RequiredAcks: kafka.RequireAll,
		Async:        false,        // synchronous: caller gets a real error on failure
		WriteTimeout: 10 * time.Second,
		Logger:       kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Debug(fmt.Sprintf(msg, args...))
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Error(fmt.Sprintf(msg, args...))
		}),
	}

	return &Producer{writer: w, logger: logger}
}

// Publish writes a single pre-serialised message to the given Kafka topic.
// key is used as the Kafka message key (RecipientID) to guarantee per-recipient ordering.
// Implements service.Publisher.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}

	p.logger.InfoContext(ctx, "message published",
		slog.String("topic", topic),
		slog.String("key", key),
		slog.Int("bytes", len(payload)),
	)

	return nil
}

// Close flushes buffered messages and closes the underlying TCP connection.
// Must be called during graceful shutdown.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("kafka producer close: %w", err)
	}
	return nil
}
