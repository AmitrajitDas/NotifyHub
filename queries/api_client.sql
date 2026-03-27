-- name: GetAPIClientByKeyHash :one
SELECT id, name, api_key_hash, is_active, created_at, updated_at
FROM api_clients
WHERE api_key_hash = $1 AND is_active = true;

-- name: InsertAPIClient :one
INSERT INTO api_clients (name, api_key_hash)
VALUES ($1, $2)
RETURNING id, name, api_key_hash, is_active, created_at, updated_at;
