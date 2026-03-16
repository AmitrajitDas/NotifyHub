-- name: UpsertPreference :one
INSERT INTO preferences (
    user_id,
    channel,
    enabled,
    quiet_hours_start,
    quiet_hours_end,
    frequency_cap,
    frequency_window_minutes,
    timezone
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
ON CONFLICT (user_id, channel) DO UPDATE SET
    enabled                  = EXCLUDED.enabled,
    quiet_hours_start        = EXCLUDED.quiet_hours_start,
    quiet_hours_end          = EXCLUDED.quiet_hours_end,
    frequency_cap            = EXCLUDED.frequency_cap,
    frequency_window_minutes = EXCLUDED.frequency_window_minutes,
    timezone                 = EXCLUDED.timezone,
    updated_at               = NOW()
RETURNING *;

-- name: GetPreference :one
SELECT * FROM preferences
WHERE user_id = $1 AND channel = $2;

-- name: ListPreferencesByUser :many
SELECT * FROM preferences
WHERE user_id = $1
ORDER BY channel ASC;

-- name: DeletePreference :exec
DELETE FROM preferences
WHERE user_id = $1 AND channel = $2;
