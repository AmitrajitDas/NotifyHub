-- name: UpsertDeviceToken :one
INSERT INTO device_tokens (tenant_id, user_id, token, platform)
VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, token) DO UPDATE
    SET user_id    = EXCLUDED.user_id,
        platform   = EXCLUDED.platform,
        is_active  = true,
        updated_at = NOW()
RETURNING *;

-- name: DeactivateDeviceToken :exec
UPDATE device_tokens
SET is_active = false, updated_at = NOW()
WHERE tenant_id = $1 AND token = $2;

-- name: ListActiveDeviceTokensByUser :many
SELECT * FROM device_tokens
WHERE tenant_id = $1 AND user_id = $2 AND is_active = true
ORDER BY created_at DESC;

-- name: GetDeviceToken :one
SELECT * FROM device_tokens
WHERE tenant_id = $1 AND token = $2;
