DROP INDEX IF EXISTS idx_notifications_tenant;
ALTER TABLE notifications DROP COLUMN IF EXISTS tenant_id;

DROP INDEX IF EXISTS idx_preferences_tenant;
ALTER TABLE preferences DROP COLUMN IF EXISTS tenant_id;

DROP INDEX IF EXISTS idx_templates_tenant;
ALTER TABLE templates DROP COLUMN IF EXISTS tenant_id;

DROP INDEX IF EXISTS idx_api_clients_tenant;
ALTER TABLE api_clients DROP COLUMN IF EXISTS tenant_id;

DROP TABLE IF EXISTS tenants;
