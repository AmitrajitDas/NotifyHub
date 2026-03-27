package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// PreferenceRepository defines DB operations for user notification preferences.
type PreferenceRepository interface {
	Get(ctx context.Context, userID string, channel domain.Channel) (*domain.Preference, error)
	ListByUser(ctx context.Context, userID string) ([]domain.Preference, error)
	Upsert(ctx context.Context, userID string, channel domain.Channel, req domain.UpsertPreferenceRequest) (*domain.Preference, error)
	Delete(ctx context.Context, userID string, channel domain.Channel) error
}

type postgresPreferenceRepo struct {
	q *db.Queries
}

func NewPreferenceRepository(q *db.Queries) PreferenceRepository {
	return &postgresPreferenceRepo{q: q}
}

func (r *postgresPreferenceRepo) Get(ctx context.Context, userID string, channel domain.Channel) (*domain.Preference, error) {
	row, err := r.q.GetPreference(ctx, db.GetPreferenceParams{
		UserID:  userID,
		Channel: string(channel),
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("preference not found")
		}
		return nil, domain.NewInternalError("failed to get preference", err)
	}
	p := dbPreferenceToDomain(row)
	return &p, nil
}

func (r *postgresPreferenceRepo) ListByUser(ctx context.Context, userID string) ([]domain.Preference, error) {
	rows, err := r.q.ListPreferencesByUser(ctx, userID)
	if err != nil {
		return nil, domain.NewInternalError("failed to list preferences", err)
	}
	out := make([]domain.Preference, len(rows))
	for i, row := range rows {
		out[i] = dbPreferenceToDomain(row)
	}
	return out, nil
}

func (r *postgresPreferenceRepo) Upsert(ctx context.Context, userID string, channel domain.Channel, req domain.UpsertPreferenceRequest) (*domain.Preference, error) {
	freqWindow := toNullInt32(req.FrequencyWindowMinutes)

	row, err := r.q.UpsertPreference(ctx, db.UpsertPreferenceParams{
		UserID:                 userID,
		Channel:                string(channel),
		Enabled:                req.Enabled,
		QuietHoursStart:        toNullTimeFromTimeStr(req.QuietHoursStart),
		QuietHoursEnd:          toNullTimeFromTimeStr(req.QuietHoursEnd),
		FrequencyCap:           toNullInt32(req.FrequencyCap),
		FrequencyWindowMinutes: freqWindow,
		Timezone:               toNullString(req.Timezone),
	})
	if err != nil {
		return nil, domain.NewInternalError("failed to upsert preference", err)
	}
	p := dbPreferenceToDomain(row)
	return &p, nil
}

func (r *postgresPreferenceRepo) Delete(ctx context.Context, userID string, channel domain.Channel) error {
	err := r.q.DeletePreference(ctx, db.DeletePreferenceParams{
		UserID:  userID,
		Channel: string(channel),
	})
	if err != nil {
		return domain.NewInternalError("failed to delete preference", err)
	}
	return nil
}

func dbPreferenceToDomain(p db.Preference) domain.Preference {
	timezone := "UTC"
	if tz := fromNullString(p.Timezone); tz != nil {
		timezone = *tz
	}

	freqWindow := 0
	if p.FrequencyWindowMinutes.Valid {
		freqWindow = int(p.FrequencyWindowMinutes.Int32)
	}

	return domain.Preference{
		ID:                     p.ID,
		UserID:                 p.UserID,
		Channel:                domain.Channel(p.Channel),
		Enabled:                p.Enabled,
		QuietHoursStart:        fromNullTimeToTimeStr(p.QuietHoursStart),
		QuietHoursEnd:          fromNullTimeToTimeStr(p.QuietHoursEnd),
		FrequencyCap:           fromNullInt32(p.FrequencyCap),
		FrequencyWindowMinutes: freqWindow,
		Timezone:               timezone,
		CreatedAt:              p.CreatedAt,
		UpdatedAt:              p.UpdatedAt,
	}
}
