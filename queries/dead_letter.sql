-- name: CreateDeadLetter :one
INSERT INTO dead_letter_messages (
    tenant_id,
    notification_id,
    channel,
    original_topic,
    payload,
    failure_reason,
    error_message,
    attempts
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING *;

-- name: GetDeadLetterByID :one
SELECT * FROM dead_letter_messages
WHERE id = $1 AND tenant_id = $2;

-- name: ListDeadLetters :many
SELECT * FROM dead_letter_messages
WHERE tenant_id = sqlc.arg(tenant_id)::uuid
  AND COALESCE(NULLIF(sqlc.arg(channel)::text, ''), channel) = channel
  AND (NOT sqlc.arg(unreplayed_only)::boolean OR replayed_at IS NULL)
ORDER BY created_at DESC
LIMIT sqlc.arg(limit_)::int
OFFSET sqlc.arg(offset_)::int;

-- name: MarkDeadLetterReplayed :exec
UPDATE dead_letter_messages
SET replayed_at = now()
WHERE id = $1 AND tenant_id = $2;

-- name: DeleteDeadLetter :exec
DELETE FROM dead_letter_messages
WHERE id = $1 AND tenant_id = $2;

-- name: CountDeadLettersByChannel :many
SELECT channel, COUNT(*) AS count
FROM dead_letter_messages
WHERE tenant_id = $1 AND replayed_at IS NULL
GROUP BY channel;
