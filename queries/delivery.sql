-- name: InsertDeliveryLog :one
INSERT INTO delivery_logs (
    notification_id,
    channel,
    provider,
    status,
    provider_message_id,
    error_message,
    error_code,
    retry_count,
    delivered_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
RETURNING *;

-- name: ListDeliveryLogsByNotification :many
SELECT * FROM delivery_logs
WHERE notification_id = $1
ORDER BY attempted_at ASC;

-- name: GetLatestDeliveryLog :one
SELECT * FROM delivery_logs
WHERE notification_id = $1
ORDER BY attempted_at DESC
LIMIT 1;
