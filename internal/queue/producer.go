package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// headerCarrier adapts []kafka.Header to the OTel TextMapCarrier interface
// so W3C traceparent / tracestate can be injected into and extracted from
// Kafka message headers, linking the API span to the worker span.
type headerCarrier struct {
	headers *[]kafka.Header
}

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(value)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

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
		Async:        false, // synchronous: caller gets a real error on failure
		WriteTimeout: 10 * time.Second,
		Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Debug(fmt.Sprintf(msg, args...))
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Error(fmt.Sprintf(msg, args...))
		}),
	}

	return &Producer{writer: w, logger: logger}
}

// Publish writes a single pre-serialised message to the given Kafka topic.
// The active span's trace context is injected as W3C traceparent headers so
// the worker can resume the trace chain across the queue boundary.
// Implements service.Publisher.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload []byte) error {
	headers := make([]kafka.Header, 0, 2)
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{headers: &headers})

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   payload,
		Headers: headers,
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

// ExtractTraceContext reads W3C traceparent headers from a Kafka message and
// returns a context that is a child of the originating request span.
// Called by the pool before handing each message to the processor.
func ExtractTraceContext(ctx context.Context, msg kafka.Message) context.Context {
	m := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		m[h.Key] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m))
}

// PublishDLQ serialises a DLQMessage and writes it to the channel's DLQ topic.
// The recipient ID is used as the partition key to preserve per-recipient ordering.
func (p *Producer) PublishDLQ(ctx context.Context, msg DLQMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dlq message: %w", err)
	}
	return p.Publish(ctx, DLQTopicForChannel(msg.Original.Channel), msg.Original.RecipientID, payload)
}

// Close flushes buffered messages and closes the underlying TCP connection.
// Must be called during graceful shutdown.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("kafka producer close: %w", err)
	}
	return nil
}
