package queue

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// allTopics returns every topic the worker reads from.
// Called at startup to pre-create topics before consumers join,
// preventing the empty-assignment deadlock that occurs when consumers
// join a group before topics exist and kafka-go never re-triggers rebalance.
func allTopics() []string {
	channels := []domain.Channel{
		domain.ChannelEmail,
		domain.ChannelPush,
		domain.ChannelSMS,
		domain.ChannelInApp,
	}
	topics := make([]string, 0, len(channels)*2+1)
	for _, ch := range channels {
		topics = append(topics, TopicForChannel(ch), DLQTopicForChannel(ch))
	}
	topics = append(topics, WebhookTopic)
	return topics
}

// EnsureTopics creates all worker topics on the broker if they do not already
// exist. The operation is idempotent — calling it on every startup is safe.
// Uses a 30-second timeout so a slow broker doesn't hang indefinitely.
func EnsureTopics(ctx context.Context, brokers []string, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Resolve a broker address to connect to.
	addr, err := net.ResolveTCPAddr("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("resolve broker address: %w", err)
	}

	conn, err := kafka.DialLeader(ctx, "tcp", addr.String(), "__consumer_offsets", 0)
	if err != nil {
		// Fallback: use plain Dial if DialLeader fails (broker may not have the
		// internal topic yet on a completely fresh cluster).
		conn, err = kafka.Dial("tcp", addr.String())
		if err != nil {
			return fmt.Errorf("connect to kafka broker: %w", err)
		}
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("connect to controller: %w", err)
	}
	defer controllerConn.Close()

	topics := allTopics()
	specs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		specs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}

	err = controllerConn.CreateTopics(specs...)
	if err != nil {
		// TopicAlreadyExists (error code 36) is not fatal — log and continue.
		logger.Info("kafka topics already exist or partially created", slog.Any("error", err))
	} else {
		logger.Info("kafka topics ensured", slog.Int("count", len(topics)))
	}

	return nil
}
