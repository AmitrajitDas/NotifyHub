-- name: InsertTenant :one
INSERT INTO tenants (name)
VALUES ($1)
RETURNING *;

-- name: GetTenantByID :one
SELECT * FROM tenants
WHERE id = $1;

-- name: GetTenantByName :one
SELECT * FROM tenants
WHERE name = $1;
