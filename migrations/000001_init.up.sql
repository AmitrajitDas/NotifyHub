-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- API clients
CREATE TABLE api_clients (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(255) NOT NULL UNIQUE,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Notification templates
CREATE TABLE templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    channel VARCHAR(50) NOT NULL, -- 'email', 'push', 'sms', 'inapp'
    subject_template TEXT, -- for email
    body_template TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    version INT NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_templates_channel ON templates(channel);
CREATE INDEX idx_templates_name ON templates(name);

-- User notification preferences
CREATE TABLE preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id VARCHAR(255) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    quiet_hours_start TIME, -- e.g., '22:00'
    quiet_hours_end TIME,   -- e.g., '08:00'
    frequency_cap INT, -- max notifications per window
    frequency_window_minutes INT DEFAULT 60,
    timezone VARCHAR(100) DEFAULT 'UTC',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, channel)
);

CREATE INDEX idx_preferences_user ON preferences(user_id);

-- Notifications (the core table)
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) UNIQUE, -- prevent duplicates
    type VARCHAR(100) NOT NULL, -- 'claim_status', 'policy_renewal', etc.
    channel VARCHAR(50) NOT NULL,
    recipient_id VARCHAR(255) NOT NULL,
    recipient_address VARCHAR(500), -- email/phone/device_token
    template_id UUID REFERENCES templates(id),
    payload JSONB NOT NULL DEFAULT '{}', -- template variables
    priority INT NOT NULL DEFAULT 5, -- 1 (highest) to 10 (lowest)
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
        -- pending -> queued -> processing -> delivered / failed / dropped
    scheduled_at TIMESTAMPTZ, -- NULL = send immediately
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_recipient ON notifications(recipient_id);
CREATE INDEX idx_notifications_scheduled ON notifications(scheduled_at) WHERE scheduled_at IS NOT NULL AND status = 'pending';
CREATE INDEX idx_notifications_idempotency ON notifications(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_notifications_created ON notifications(created_at);

-- Delivery attempts log
CREATE TABLE delivery_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID NOT NULL REFERENCES notifications(id),
    channel VARCHAR(50) NOT NULL,
    provider VARCHAR(100) NOT NULL, -- 'ses', 'fcm', 'twilio'
    status VARCHAR(50) NOT NULL, -- 'success', 'failed', 'bounced', 'rejected'
    provider_message_id VARCHAR(500),
    error_message TEXT,
    error_code VARCHAR(100),
    retry_count INT NOT NULL DEFAULT 0,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_delivery_notification ON delivery_logs(notification_id);
CREATE INDEX idx_delivery_status ON delivery_logs(status);
CREATE INDEX idx_delivery_attempted ON delivery_logs(attempted_at);
