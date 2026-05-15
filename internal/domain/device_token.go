package domain

import (
	"time"

	"github.com/google/uuid"
)

type DeviceToken struct {
	ID        uuid.UUID `json:"id"`
	TenantID  uuid.UUID `json:"tenant_id"`
	UserID    string    `json:"user_id"`
	Token     string    `json:"token"`
	Platform  string    `json:"platform"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type RegisterDeviceTokenRequest struct {
	UserID   string `json:"user_id"  validate:"required"`
	Token    string `json:"token"    validate:"required"`
	Platform string `json:"platform" validate:"required,oneof=ios android web"`
}
