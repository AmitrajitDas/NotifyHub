package domain

import (
	"time"

	"github.com/google/uuid"
)

type InAppMessage struct {
	ID             uuid.UUID      `json:"id"`
	TenantID       uuid.UUID      `json:"tenant_id"`
	NotificationID *uuid.UUID     `json:"notification_id,omitempty"`
	RecipientID    string         `json:"recipient_id"`
	Title          string         `json:"title"`
	Body           string         `json:"body"`
	Payload        map[string]any `json:"payload,omitempty"`
	ReadAt         *time.Time     `json:"read_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
}

type InboxQuery struct {
	TenantID    uuid.UUID
	RecipientID string
	UnreadOnly  bool
	Limit       int
	Offset      int
	AfterID     *uuid.UUID // keyset cursor; if set, Offset is ignored
}
