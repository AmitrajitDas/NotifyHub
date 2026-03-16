package domain

import (
	"time"

	"github.com/google/uuid"
)

type DeliveryStatus string

const (
	DeliverySuccess  DeliveryStatus = "success"
	DeliveryFailed   DeliveryStatus = "failed"
	DeliveryBounced  DeliveryStatus = "bounced"
	DeliveryRejected DeliveryStatus = "rejected"
)

type DeliveryLog struct {
	ID                uuid.UUID      `json:"id"`
	NotificationID    uuid.UUID      `json:"notification_id"`
	Channel           Channel        `json:"channel"`
	Provider          string         `json:"provider"` // "ses", "fcm", "twilio", "mock"
	Status            DeliveryStatus `json:"status"`
	ProviderMessageID *string        `json:"provider_message_id,omitempty"`
	ErrorMessage      *string        `json:"error_message,omitempty"`
	ErrorCode         *string        `json:"error_code,omitempty"`
	RetryCount        int            `json:"retry_count"`
	AttemptedAt       time.Time      `json:"attempted_at"`
	DeliveredAt       *time.Time     `json:"delivered_at,omitempty"`
}
