-- name: InsertInAppMessage :one
INSERT INTO inapp_messages (
    tenant_id,
    notification_id,
    recipient_id,
    title,
    body,
    payload
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING *;

-- name: GetInAppMessage :one
SELECT * FROM inapp_messages
WHERE id = $1 AND tenant_id = $2;

-- name: ListInbox :many
SELECT * FROM inapp_messages
WHERE tenant_id = sqlc.arg(tenant_id)::uuid
  AND recipient_id = sqlc.arg(recipient_id)::text
  AND (NOT sqlc.arg(unread_only)::boolean OR read_at IS NULL)
ORDER BY created_at DESC
LIMIT sqlc.arg(limit_)::int
OFFSET sqlc.arg(offset_)::int;

-- name: CountUnread :one
SELECT COUNT(*) FROM inapp_messages
WHERE tenant_id = $1
  AND recipient_id = $2
  AND read_at IS NULL;

-- name: MarkInAppMessageRead :exec
UPDATE inapp_messages
SET read_at = now()
WHERE id = $1 AND tenant_id = $2 AND recipient_id = $3 AND read_at IS NULL;

-- name: MarkAllInAppMessagesRead :exec
UPDATE inapp_messages
SET read_at = now()
WHERE tenant_id = $1 AND recipient_id = $2 AND read_at IS NULL;

-- name: ListInboxAfterCursor :many
SELECT * FROM inapp_messages
WHERE tenant_id = $1::uuid
  AND recipient_id = $2::text
  AND (NOT $3::boolean OR read_at IS NULL)
  AND (created_at < $4 OR (created_at = $4 AND id < $5::uuid))
ORDER BY created_at DESC, id DESC
LIMIT $6::int;

-- name: ListInboxSince :many
SELECT * FROM inapp_messages
WHERE tenant_id = $1::uuid
  AND recipient_id = $2::text
  AND (created_at > $3 OR (created_at = $3 AND id > $4::uuid))
ORDER BY created_at ASC, id ASC
LIMIT 200;
