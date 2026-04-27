package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// DLQ failure reason constants.
const (
	DLQReasonRetriesExhausted = "retries_exhausted"
	DLQReasonPermanent        = "permanent"
	DLQReasonTemplateRender   = "template_render"
	DLQReasonNoProvider       = "no_provider"
)

// DeadLetterMessage represents a notification that exhausted all delivery
// attempts or encountered a permanent failure. Persisted for operator
// inspection and replay.
type DeadLetterMessage struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	NotificationID uuid.UUID
	Channel        Channel
	OriginalTopic  string
	Payload        json.RawMessage // serialized queue.DLQMessage envelope
	FailureReason  string
	ErrorMessage   string
	Attempts       int
	ReplayedAt     *time.Time
	CreatedAt      time.Time
}

// DeadLetterListParams controls pagination and filtering for list queries.
type DeadLetterListParams struct {
	TenantID  uuid.UUID
	Channel   string // empty = all channels
	Unreplayed *bool  // nil = all, true = only unreplayed, false = only replayed
	Limit     int
	Offset    int
}
