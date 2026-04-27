CREATE TABLE dead_letter_messages (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    notification_id UUID NOT NULL,
    channel         TEXT NOT NULL,
    original_topic  TEXT NOT NULL,
    payload         JSONB NOT NULL,
    failure_reason  TEXT NOT NULL,
    error_message   TEXT NOT NULL,
    attempts        INT  NOT NULL,
    replayed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dlq_tenant_channel_created ON dead_letter_messages (tenant_id, channel, created_at DESC);
CREATE INDEX idx_dlq_unreplayed ON dead_letter_messages (tenant_id, created_at DESC) WHERE replayed_at IS NULL;
