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
