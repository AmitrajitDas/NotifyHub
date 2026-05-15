CREATE TABLE device_tokens (
    id          UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id   UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id     VARCHAR(255) NOT NULL,
    token       TEXT         NOT NULL,
    platform    VARCHAR(20)  NOT NULL CHECK (platform IN ('ios', 'android', 'web')),
    is_active   BOOLEAN      NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, token)
);

CREATE INDEX idx_device_tokens_user ON device_tokens(tenant_id, user_id) WHERE is_active = true;
