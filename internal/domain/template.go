package domain

import (
	"time"

	"github.com/google/uuid"
)

type Template struct {
	ID              uuid.UUID      `json:"id"`
	Name            string         `json:"name"`
	Channel         Channel        `json:"channel"`
	SubjectTemplate *string        `json:"subject_template,omitempty"` // email only
	BodyTemplate    string         `json:"body_template"`
	Metadata        map[string]any `json:"metadata"`
	Version         int            `json:"version"`
	IsActive        bool           `json:"is_active"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

type CreateTemplateRequest struct {
	Name            string         `json:"name"          validate:"required,max=255"`
	Channel         Channel        `json:"channel"       validate:"required,oneof=email push sms inapp"`
	SubjectTemplate *string        `json:"subject_template,omitempty"`
	BodyTemplate    string         `json:"body_template" validate:"required"`
	Metadata        map[string]any `json:"metadata"`
}

type UpdateTemplateRequest struct {
	SubjectTemplate *string        `json:"subject_template,omitempty"`
	BodyTemplate    *string        `json:"body_template,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	IsActive        *bool          `json:"is_active,omitempty"`
}

type PreviewTemplateRequest struct {
	Payload map[string]any `json:"payload" validate:"required"`
}

type PreviewTemplateResponse struct {
	Subject string `json:"subject,omitempty"`
	Body    string `json:"body"`
}
