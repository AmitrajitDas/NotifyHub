package domain

import (
	"time"

	"github.com/google/uuid"
)

type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
	ChannelSMS   Channel = "sms"
	ChannelInApp Channel = "inapp"
)

func (c Channel) IsValid() bool {
	switch c {
	case ChannelEmail, ChannelPush, ChannelSMS, ChannelInApp:
		return true
	}
	return false
}

type NotificationStatus string

const (
	StatusPending    NotificationStatus = "pending"
	StatusQueued     NotificationStatus = "queued"
	StatusProcessing NotificationStatus = "processing"
	StatusDelivered  NotificationStatus = "delivered"
	StatusFailed     NotificationStatus = "failed"
	StatusDropped    NotificationStatus = "dropped" // rate-limited or preference-blocked
)

type Priority int

const (
	PriorityCritical Priority = 1
	PriorityHigh     Priority = 3
	PriorityNormal   Priority = 5
	PriorityLow      Priority = 7
	PriorityBulk     Priority = 10
)

type Notification struct {
	ID               uuid.UUID          `json:"id"`
	IdempotencyKey   *string            `json:"idempotency_key,omitempty"`
	Type             string             `json:"type"`
	Channel          Channel            `json:"channel"`
	RecipientID      string             `json:"recipient_id"`
	RecipientAddress string             `json:"recipient_address"`
	TemplateID       *uuid.UUID         `json:"template_id,omitempty"`
	Payload          map[string]any     `json:"payload"`
	Priority         Priority           `json:"priority"`
	Status           NotificationStatus `json:"status"`
	ScheduledAt      *time.Time         `json:"scheduled_at,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at"`
}

// SendRequest is the inbound API request for a single notification.
type SendRequest struct {
	Type             string         `json:"type"              validate:"required"`
	Channel          Channel        `json:"channel"           validate:"required,oneof=email push sms inapp"`
	RecipientID      string         `json:"recipient_id"      validate:"required"`
	RecipientAddress string         `json:"recipient_address" validate:"required"`
	TemplateID       *uuid.UUID     `json:"template_id,omitempty"`
	Payload          map[string]any `json:"payload"`
	Priority         Priority       `json:"priority"          validate:"min=1,max=10"`
	IdempotencyKey   *string        `json:"idempotency_key,omitempty"`
	ScheduledAt      *time.Time     `json:"scheduled_at,omitempty"`
}

// BulkSendRequest allows sending to multiple recipients in one call.
type BulkSendRequest struct {
	Type       string         `json:"type"        validate:"required"`
	Channel    Channel        `json:"channel"     validate:"required,oneof=email push sms inapp"`
	TemplateID *uuid.UUID     `json:"template_id,omitempty"`
	Payload    map[string]any `json:"payload"`
	Priority   Priority       `json:"priority"    validate:"min=1,max=10"`
	Recipients []Recipient    `json:"recipients"  validate:"required,min=1,max=1000"`
}

type Recipient struct {
	RecipientID      string         `json:"recipient_id"      validate:"required"`
	RecipientAddress string         `json:"recipient_address" validate:"required"`
	Payload          map[string]any `json:"payload,omitempty"` // per-recipient overrides
}

// ListNotificationsFilter holds query params for listing notifications.
type ListNotificationsFilter struct {
	RecipientID string
	Channel     Channel
	Status      NotificationStatus
	Page        int
	PerPage     int
}
