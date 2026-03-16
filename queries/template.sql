-- name: InsertTemplate :one
INSERT INTO templates (
    name,
    channel,
    subject_template,
    body_template,
    metadata
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING *;

-- name: GetTemplateByID :one
SELECT * FROM templates
WHERE id = $1;

-- name: GetTemplateByName :one
SELECT * FROM templates
WHERE name = $1 AND is_active = true;

-- name: ListTemplates :many
SELECT * FROM templates
WHERE ($1::text = '' OR channel = $1)
ORDER BY name ASC
LIMIT $2 OFFSET $3;

-- name: CountTemplates :one
SELECT COUNT(*) FROM templates
WHERE ($1::text = '' OR channel = $1);

-- name: UpdateTemplate :one
UPDATE templates
SET
    subject_template = COALESCE($2, subject_template),
    body_template    = COALESCE($3, body_template),
    metadata         = COALESCE($4, metadata),
    is_active        = COALESCE($5, is_active),
    version          = version + 1,
    updated_at       = NOW()
WHERE id = $1
RETURNING *;

-- name: DeleteTemplate :one
UPDATE templates
SET is_active = false, updated_at = NOW()
WHERE id = $1
RETURNING *;
