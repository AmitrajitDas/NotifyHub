package domain

import "github.com/google/uuid"

// APIClient represents an authenticated caller of the API.
// Stored in context by auth middleware after key validation.
type APIClient struct {
	ID       uuid.UUID
	Name     string
	IsActive bool
}
