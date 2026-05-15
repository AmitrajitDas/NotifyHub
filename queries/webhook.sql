-- name: InsertWebhookEndpoint :one
INSERT INTO webhook_endpoints (tenant_id, url, secret, events, is_active)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetWebhookEndpoint :one
SELECT * FROM webhook_endpoints
WHERE id = $1 AND tenant_id = $2;

-- name: ListWebhookEndpoints :many
SELECT * FROM webhook_endpoints
WHERE tenant_id = $1
ORDER BY created_at DESC;

-- name: UpdateWebhookEndpoint :one
UPDATE webhook_endpoints
SET url = $3, events = $4, is_active = $5, updated_at = now()
WHERE id = $1 AND tenant_id = $2
RETURNING *;

-- name: DeleteWebhookEndpoint :exec
DELETE FROM webhook_endpoints
WHERE id = $1 AND tenant_id = $2;

-- name: ListActiveWebhookEndpointsForEvent :many
SELECT * FROM webhook_endpoints
WHERE tenant_id = $1
  AND is_active = TRUE
  AND events @> jsonb_build_array($2::text);

-- name: InsertWebhookDelivery :one
INSERT INTO webhook_deliveries (endpoint_id, notification_id, event, payload, status, attempt, next_retry_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: UpdateWebhookDelivery :one
UPDATE webhook_deliveries
SET status = $2, attempt = $3, next_retry_at = $4, last_error = $5, response_status = $6, updated_at = now()
WHERE id = $1
RETURNING *;
