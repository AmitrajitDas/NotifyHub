package domain

import (
	"time"

	"github.com/google/uuid"
)

// Webhook event types fired after a notification reaches a terminal status.
const (
	WebhookEventDelivered        = "notification.delivered"
	WebhookEventFailed           = "notification.failed"
	WebhookEventDropped          = "notification.dropped"
	WebhookEventTokenDeactivated = "device_token.deactivated"
)

// AllWebhookEvents is the exhaustive list of valid event names for validation.
var AllWebhookEvents = []string{
	WebhookEventDelivered,
	WebhookEventFailed,
	WebhookEventDropped,
	WebhookEventTokenDeactivated,
}

type WebhookDeliveryStatus string

const (
	WebhookDeliveryPending   WebhookDeliveryStatus = "pending"
	WebhookDeliveryDelivered WebhookDeliveryStatus = "delivered"
	WebhookDeliveryFailed    WebhookDeliveryStatus = "failed"
)

// WebhookEndpoint is a tenant-registered URL that receives HTTP POST callbacks
// for subscribed notification lifecycle events.
type WebhookEndpoint struct {
	ID        uuid.UUID `json:"id"`
	TenantID  uuid.UUID `json:"tenant_id"`
	URL       string    `json:"url"`
	Secret    string    `json:"-"`
	Events    []string  `json:"events"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// WebhookDelivery tracks a single HTTP dispatch attempt to an endpoint.
type WebhookDelivery struct {
	ID             uuid.UUID             `json:"id"`
	EndpointID     uuid.UUID             `json:"endpoint_id"`
	NotificationID *uuid.UUID            `json:"notification_id,omitempty"`
	Event          string                `json:"event"`
	Payload        map[string]any        `json:"payload"`
	Status         WebhookDeliveryStatus `json:"status"`
	Attempt        int                   `json:"attempt"`
	NextRetryAt    *time.Time            `json:"next_retry_at,omitempty"`
	LastError      *string               `json:"last_error,omitempty"`
	ResponseStatus *int                  `json:"response_status,omitempty"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
}

type CreateWebhookEndpointRequest struct {
	URL    string   `json:"url"    validate:"required,url"`
	Secret string   `json:"secret" validate:"required,min=16"`
	Events []string `json:"events" validate:"required,min=1"`
}

type UpdateWebhookEndpointRequest struct {
	URL      string   `json:"url"       validate:"required,url"`
	Events   []string `json:"events"    validate:"required,min=1"`
	IsActive bool     `json:"is_active"`
}
