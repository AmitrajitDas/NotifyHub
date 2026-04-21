package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// TenantService manages tenant provisioning and API key issuance.
type TenantService interface {
	CreateTenant(ctx context.Context, req domain.CreateTenantRequest) (*domain.Tenant, error)
	CreateAPIKey(ctx context.Context, tenantID uuid.UUID, req domain.CreateAPIKeyRequest) (*domain.CreateAPIKeyResponse, error)
}

type tenantService struct {
	tenantRepo    repository.TenantRepository
	apiClientRepo repository.APIClientRepository
	validator     *validator.Validate
}

func NewTenantService(
	tenantRepo repository.TenantRepository,
	apiClientRepo repository.APIClientRepository,
	v *validator.Validate,
) TenantService {
	return &tenantService{
		tenantRepo:    tenantRepo,
		apiClientRepo: apiClientRepo,
		validator:     v,
	}
}

func (s *tenantService) CreateTenant(ctx context.Context, req domain.CreateTenantRequest) (*domain.Tenant, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}
	return s.tenantRepo.Create(ctx, req)
}

func (s *tenantService) CreateAPIKey(ctx context.Context, tenantID uuid.UUID, req domain.CreateAPIKeyRequest) (*domain.CreateAPIKeyResponse, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}

	// Verify tenant exists.
	if _, err := s.tenantRepo.GetByID(ctx, tenantID); err != nil {
		return nil, err
	}

	// Generate a cryptographically random 32-byte key.
	rawKey, err := generateKey()
	if err != nil {
		return nil, domain.NewInternalError("failed to generate api key", err)
	}
	keyHash := hashAPIKey(rawKey)

	client, err := s.apiClientRepo.CreateForTenant(ctx, tenantID, req.Name, keyHash)
	if err != nil {
		return nil, err
	}

	return &domain.CreateAPIKeyResponse{
		ID:       client.ID,
		TenantID: tenantID,
		Name:     client.Name,
		APIKey:   rawKey,
	}, nil
}

func generateKey() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func hashAPIKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
