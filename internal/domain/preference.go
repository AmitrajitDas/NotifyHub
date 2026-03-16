package domain

import (
	"time"

	"github.com/google/uuid"
)

type Preference struct {
	ID                    uuid.UUID `json:"id"`
	UserID                string    `json:"user_id"`
	Channel               Channel   `json:"channel"`
	Enabled               bool      `json:"enabled"`
	QuietHoursStart       *string   `json:"quiet_hours_start,omitempty"` // "22:00"
	QuietHoursEnd         *string   `json:"quiet_hours_end,omitempty"`   // "08:00"
	FrequencyCap          *int      `json:"frequency_cap,omitempty"`
	FrequencyWindowMinutes int      `json:"frequency_window_minutes"`
	Timezone              string    `json:"timezone"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}

type UpsertPreferenceRequest struct {
	Enabled                bool    `json:"enabled"`
	QuietHoursStart        *string `json:"quiet_hours_start,omitempty"  validate:"omitempty,datetime=15:04"`
	QuietHoursEnd          *string `json:"quiet_hours_end,omitempty"    validate:"omitempty,datetime=15:04"`
	FrequencyCap           *int    `json:"frequency_cap,omitempty"      validate:"omitempty,min=1"`
	FrequencyWindowMinutes *int    `json:"frequency_window_minutes,omitempty" validate:"omitempty,min=1"`
	Timezone               *string `json:"timezone,omitempty"`
}
