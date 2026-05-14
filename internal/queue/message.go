package queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// Message is the canonical Kafka envelope for one notification dispatch attempt.
// Both the service (producer) and worker (consumer) import this type — keeping it
// here prevents an import cycle between those two packages.
type Message struct {
	NotificationID   uuid.UUID       `json:"notification_id"`
	TenantID         uuid.UUID       `json:"tenant_id"`
	Channel          domain.Channel  `json:"channel"`
	RecipientID      string          `json:"recipient_id"`
	RecipientAddress string          `json:"recipient_address"`
	TemplateID       *uuid.UUID      `json:"template_id,omitempty"`
	Payload          map[string]any  `json:"payload"`
	Priority         domain.Priority `json:"priority"`
	Attempt          int             `json:"attempt"`
	MaxAttempts      int             `json:"max_attempts"`
	EnqueuedAt       time.Time       `json:"enqueued_at"`
}

// TopicForChannel returns the Kafka topic name for the given channel.
// Centralised here so producer and consumer always agree on naming.
func TopicForChannel(ch domain.Channel) string {
	return fmt.Sprintf("notifyhub.notifications.%s", ch)
}

// DLQTopicForChannel returns the dead-letter topic for the given channel.
func DLQTopicForChannel(ch domain.Channel) string {
	return TopicForChannel(ch) + ".dlq"
}

// DLQMessage is the Kafka envelope written to a DLQ topic when a notification
// exhausts all delivery attempts or encounters a permanent provider failure.
type DLQMessage struct {
	Original       Message   `json:"original"`
	OriginalTopic  string    `json:"original_topic"`
	FailureReason  string    `json:"failure_reason"`
	ErrorMessage   string    `json:"error_message"`
	LastAttempt    int       `json:"last_attempt"`
	DeadLetteredAt time.Time `json:"dead_lettered_at"`
}

// WebhookTopic is the Kafka topic for outbound webhook dispatch.
const WebhookTopic = "notifyhub.webhooks.outbound"

// WebhookMessage is the Kafka envelope for one outbound webhook dispatch attempt.
type WebhookMessage struct {
	TenantID       uuid.UUID      `json:"tenant_id"`
	EndpointID     uuid.UUID      `json:"endpoint_id"`
	DeliveryID     uuid.UUID      `json:"delivery_id"`
	NotificationID *uuid.UUID     `json:"notification_id,omitempty"`
	Event          string         `json:"event"`
	Payload        map[string]any `json:"payload"`
	EndpointURL    string         `json:"endpoint_url"`
	Secret         string         `json:"secret"`
	Attempt        int            `json:"attempt"`
	MaxAttempts    int            `json:"max_attempts"`
	EnqueuedAt     time.Time      `json:"enqueued_at"`
}
