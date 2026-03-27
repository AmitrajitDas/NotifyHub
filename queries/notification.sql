-- name: InsertNotification :one
INSERT INTO notifications (
    idempotency_key,
    type,
    channel,
    recipient_id,
    recipient_address,
    template_id,
    payload,
    priority,
    status,
    scheduled_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
RETURNING *;

-- name: GetNotificationByID :one
SELECT * FROM notifications
WHERE id = $1;

-- name: GetNotificationByIdempotencyKey :one
SELECT * FROM notifications
WHERE idempotency_key = $1;

-- name: ListNotifications :many
SELECT * FROM notifications
WHERE
    (sqlc.arg(recipient_id)::text = '' OR recipient_id = sqlc.arg(recipient_id)) AND
    (sqlc.arg(channel)::text = '' OR channel = sqlc.arg(channel)) AND
    (sqlc.arg(status)::text = '' OR status = sqlc.arg(status))
ORDER BY created_at DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountNotifications :one
SELECT COUNT(*) FROM notifications
WHERE
    (sqlc.arg(recipient_id)::text = '' OR recipient_id = sqlc.arg(recipient_id)) AND
    (sqlc.arg(channel)::text = '' OR channel = sqlc.arg(channel)) AND
    (sqlc.arg(status)::text = '' OR status = sqlc.arg(status));

-- name: UpdateNotificationStatus :one
UPDATE notifications
SET status = $2, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: CancelNotification :one
UPDATE notifications
SET status = 'dropped', updated_at = NOW()
WHERE id = $1 AND status IN ('pending', 'queued')
RETURNING *;

-- name: ListDueScheduledNotifications :many
SELECT * FROM notifications
WHERE status = 'pending'
  AND scheduled_at IS NOT NULL
  AND scheduled_at <= NOW()
ORDER BY priority ASC, scheduled_at ASC
LIMIT $1;
