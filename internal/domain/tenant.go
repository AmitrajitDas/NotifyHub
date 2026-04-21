package domain

import (
	"time"

	"github.com/google/uuid"
)

// Tenant represents a top-level organization or deployment unit.
type Tenant struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateTenantRequest struct {
	Name string `json:"name" validate:"required,max=255"`
}

type CreateAPIKeyRequest struct {
	Name string `json:"name" validate:"required,max=255"`
}

type CreateAPIKeyResponse struct {
	ID       uuid.UUID `json:"id"`
	TenantID uuid.UUID `json:"tenant_id"`
	Name     string    `json:"name"`
	APIKey   string    `json:"api_key"` // raw key — shown only once
}
