# Database Schema

```mermaid
erDiagram
    api_clients {
        UUID id PK
        VARCHAR name
        VARCHAR api_key_hash UK
        BOOLEAN is_active
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    templates {
        UUID id PK
        VARCHAR name UK
        VARCHAR channel
        TEXT subject_template "email only"
        TEXT body_template
        JSONB metadata
        INT version
        BOOLEAN is_active
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    preferences {
        UUID id PK
        VARCHAR user_id
        VARCHAR channel
        BOOLEAN enabled
        TIME quiet_hours_start
        TIME quiet_hours_end
        INT frequency_cap
        INT frequency_window_minutes
        VARCHAR timezone
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    notifications {
        UUID id PK
        VARCHAR idempotency_key UK
        VARCHAR type
        VARCHAR channel
        VARCHAR recipient_id
        VARCHAR recipient_address
        UUID template_id FK
        JSONB payload
        INT priority
        VARCHAR status "pending|queued|processing|delivered|failed|dropped"
        TIMESTAMPTZ scheduled_at "NULL = immediate"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }

    delivery_logs {
        UUID id PK
        UUID notification_id FK
        VARCHAR channel
        VARCHAR provider "ses|fcm|twilio"
        VARCHAR status "success|failed|bounced|rejected"
        VARCHAR provider_message_id
        TEXT error_message
        VARCHAR error_code
        INT retry_count
        TIMESTAMPTZ attempted_at
        TIMESTAMPTZ delivered_at
    }

    templates ||--o{ notifications : "referenced by"
    notifications ||--o{ delivery_logs : "has many"
    preferences }o--|| notifications : "checked against"
```

## Notification Lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending : Created
    pending --> queued : Sent to Kafka
    pending --> dropped : Preference check failed
    queued --> processing : Worker picks up
    processing --> delivered : Provider success
    processing --> failed : Max retries exhausted
    processing --> processing : Retry on transient error
```

## Key Constraints

| Table           | Constraint                 | Purpose                             |
| --------------- | -------------------------- | ----------------------------------- |
| `api_clients`   | `UNIQUE(api_key_hash)`     | One key per client                  |
| `templates`     | `UNIQUE(name)`             | Lookup by name                      |
| `preferences`   | `UNIQUE(user_id, channel)` | One preference per user per channel |
| `notifications` | `UNIQUE(idempotency_key)`  | Prevent duplicate sends             |
| `delivery_logs` | `FK(notification_id)`      | Links attempts to notification      |

## Indexes

| Index                           | Table         | Purpose                             |
| ------------------------------- | ------------- | ----------------------------------- |
| `idx_templates_channel`         | templates     | Filter by channel                   |
| `idx_templates_name`            | templates     | Lookup by name                      |
| `idx_preferences_user`          | preferences   | Lookup by user_id                   |
| `idx_notifications_status`      | notifications | Filter by status                    |
| `idx_notifications_recipient`   | notifications | Filter by recipient                 |
| `idx_notifications_scheduled`   | notifications | Partial: pending + has scheduled_at |
| `idx_notifications_idempotency` | notifications | Partial: non-null idempotency keys  |
| `idx_notifications_created`     | notifications | Sort by creation time               |
| `idx_delivery_notification`     | delivery_logs | Join to notification                |
| `idx_delivery_status`           | delivery_logs | Filter by delivery status           |
| `idx_delivery_attempted`        | delivery_logs | Sort by attempt time                |
