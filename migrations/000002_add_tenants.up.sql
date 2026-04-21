-- Create tenants table
CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add tenant_id to api_clients
ALTER TABLE api_clients ADD COLUMN tenant_id UUID REFERENCES tenants(id);
CREATE INDEX idx_api_clients_tenant ON api_clients(tenant_id);

-- Add tenant_id to templates
ALTER TABLE templates ADD COLUMN tenant_id UUID REFERENCES tenants(id);
CREATE INDEX idx_templates_tenant ON templates(tenant_id);

-- Add tenant_id to preferences
ALTER TABLE preferences ADD COLUMN tenant_id UUID REFERENCES tenants(id);
-- Replace old unique constraint with tenant-scoped one
ALTER TABLE preferences DROP CONSTRAINT IF EXISTS preferences_user_id_channel_key;
ALTER TABLE preferences ADD CONSTRAINT preferences_tenant_user_channel_key UNIQUE (tenant_id, user_id, channel);
CREATE INDEX idx_preferences_tenant ON preferences(tenant_id);

-- Add tenant_id to notifications
ALTER TABLE notifications ADD COLUMN tenant_id UUID REFERENCES tenants(id);
CREATE INDEX idx_notifications_tenant ON notifications(tenant_id);
