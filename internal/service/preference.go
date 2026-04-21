package service

import (
	"context"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// PreferenceService defines business logic for user notification preferences.
type PreferenceService interface {
	Get(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel) (*domain.Preference, error)
	ListByUser(ctx context.Context, tenantID uuid.UUID, userID string) ([]domain.Preference, error)
	Upsert(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel, req domain.UpsertPreferenceRequest) (*domain.Preference, error)
	Delete(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel) error
	// IsAllowed checks whether a notification may be delivered to a user on a given channel.
	// Returns (true, "") if allowed, or (false, reason) if blocked.
	IsAllowed(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel) (bool, string, error)
}

type preferenceService struct {
	repo      repository.PreferenceRepository
	validator *validator.Validate
}

func NewPreferenceService(repo repository.PreferenceRepository, v *validator.Validate) PreferenceService {
	return &preferenceService{repo: repo, validator: v}
}

func (s *preferenceService) Get(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel) (*domain.Preference, error) {
	return s.repo.Get(ctx, tenantID, userID, channel)
}

func (s *preferenceService) ListByUser(ctx context.Context, tenantID uuid.UUID, userID string) ([]domain.Preference, error) {
	return s.repo.ListByUser(ctx, tenantID, userID)
}

func (s *preferenceService) Upsert(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel, req domain.UpsertPreferenceRequest) (*domain.Preference, error) {
	if err := s.validator.StructCtx(ctx, req); err != nil {
		return nil, toValidationError(err)
	}
	return s.repo.Upsert(ctx, tenantID, userID, channel, req)
}

func (s *preferenceService) Delete(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel) error {
	return s.repo.Delete(ctx, tenantID, userID, channel)
}

func (s *preferenceService) IsAllowed(ctx context.Context, tenantID uuid.UUID, userID string, channel domain.Channel) (bool, string, error) {
	pref, err := s.repo.Get(ctx, tenantID, userID, channel)
	if err != nil {
		// No preference record = use defaults (allow everything).
		appErr, ok := err.(*domain.AppError)
		if ok && appErr.Code == domain.ErrCodeNotFound {
			return true, "", nil
		}
		return false, "", err
	}

	if !pref.Enabled {
		return false, "channel disabled by user", nil
	}

	if pref.QuietHoursStart != nil && pref.QuietHoursEnd != nil {
		if inQuietHours(pref.QuietHoursStart, pref.QuietHoursEnd, pref.Timezone) {
			return false, "quiet hours active", nil
		}
	}

	return true, "", nil
}

// inQuietHours returns true if the current time in the given timezone falls within [start, end).
// Handles overnight windows (e.g. 22:00–08:00) correctly.
func inQuietHours(start, end *string, timezone string) bool {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}

	now := time.Now().In(loc)
	layout := "15:04"

	s, err := time.Parse(layout, *start)
	if err != nil {
		return false
	}
	e, err := time.Parse(layout, *end)
	if err != nil {
		return false
	}

	// Anchor start/end to today's date in the user's timezone for comparison.
	startToday := time.Date(now.Year(), now.Month(), now.Day(), s.Hour(), s.Minute(), 0, 0, loc)
	endToday := time.Date(now.Year(), now.Month(), now.Day(), e.Hour(), e.Minute(), 0, 0, loc)

	if !startToday.After(endToday) {
		// Same-day window e.g. 09:00–17:00
		return !now.Before(startToday) && now.Before(endToday)
	}
	// Overnight window e.g. 22:00–08:00: active if now >= start OR now < end
	return !now.Before(startToday) || now.Before(endToday)
}
