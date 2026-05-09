package service

import (
	"context"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

type DeviceTokenService interface {
	Register(ctx context.Context, tenantID uuid.UUID, req domain.RegisterDeviceTokenRequest) (*domain.DeviceToken, error)
	Deregister(ctx context.Context, tenantID uuid.UUID, token string) error
	ListActiveByUser(ctx context.Context, tenantID uuid.UUID, userID string) ([]domain.DeviceToken, error)
}

type deviceTokenService struct {
	repo     repository.DeviceTokenRepository
	validate *validator.Validate
}

func NewDeviceTokenService(repo repository.DeviceTokenRepository, validate *validator.Validate) DeviceTokenService {
	return &deviceTokenService{repo: repo, validate: validate}
}

func (s *deviceTokenService) Register(ctx context.Context, tenantID uuid.UUID, req domain.RegisterDeviceTokenRequest) (*domain.DeviceToken, error) {
	if err := s.validate.Struct(req); err != nil {
		return nil, toValidationError(err)
	}
	dt, err := s.repo.Upsert(ctx, tenantID, req)
	if err != nil {
		return nil, domain.NewInternalError("failed to register device token", err)
	}
	return dt, nil
}

func (s *deviceTokenService) Deregister(ctx context.Context, tenantID uuid.UUID, token string) error {
	if token == "" {
		return domain.NewValidationError("token required", nil)
	}
	return s.repo.Deactivate(ctx, tenantID, token)
}

func (s *deviceTokenService) ListActiveByUser(ctx context.Context, tenantID uuid.UUID, userID string) ([]domain.DeviceToken, error) {
	return s.repo.ListActiveByUser(ctx, tenantID, userID)
}
