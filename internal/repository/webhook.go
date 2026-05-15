package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// WebhookRepository defines DB operations for webhook endpoints and deliveries.
type WebhookRepository interface {
	CreateEndpoint(ctx context.Context, ep domain.WebhookEndpoint) (*domain.WebhookEndpoint, error)
	GetEndpoint(ctx context.Context, tenantID, id uuid.UUID) (*domain.WebhookEndpoint, error)
	ListEndpoints(ctx context.Context, tenantID uuid.UUID) ([]domain.WebhookEndpoint, error)
	UpdateEndpoint(ctx context.Context, ep domain.WebhookEndpoint) (*domain.WebhookEndpoint, error)
	DeleteEndpoint(ctx context.Context, tenantID, id uuid.UUID) error
	ListActiveForEvent(ctx context.Context, tenantID uuid.UUID, event string) ([]domain.WebhookEndpoint, error)
	CreateDelivery(ctx context.Context, d domain.WebhookDelivery) (*domain.WebhookDelivery, error)
	UpdateDelivery(ctx context.Context, d domain.WebhookDelivery) (*domain.WebhookDelivery, error)
}

type postgresWebhookRepo struct {
	q *db.Queries
}

func NewWebhookRepository(q *db.Queries) WebhookRepository {
	return &postgresWebhookRepo{q: q}
}

func (r *postgresWebhookRepo) CreateEndpoint(ctx context.Context, ep domain.WebhookEndpoint) (*domain.WebhookEndpoint, error) {
	eventsJSON, err := json.Marshal(ep.Events)
	if err != nil {
		return nil, domain.NewInternalError("marshal events", err)
	}
	row, err := r.q.InsertWebhookEndpoint(ctx, db.InsertWebhookEndpointParams{
		TenantID: ep.TenantID,
		Url:      ep.URL,
		Secret:   ep.Secret,
		Events:   eventsJSON,
		IsActive: ep.IsActive,
	})
	if err != nil {
		return nil, domain.NewInternalError("insert webhook endpoint", err)
	}
	out := dbWebhookEndpointToDomain(row)
	return &out, nil
}

func (r *postgresWebhookRepo) GetEndpoint(ctx context.Context, tenantID, id uuid.UUID) (*domain.WebhookEndpoint, error) {
	row, err := r.q.GetWebhookEndpoint(ctx, db.GetWebhookEndpointParams{
		ID:       id,
		TenantID: tenantID,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("webhook endpoint not found")
		}
		return nil, domain.NewInternalError("get webhook endpoint", err)
	}
	out := dbWebhookEndpointToDomain(row)
	return &out, nil
}

func (r *postgresWebhookRepo) ListEndpoints(ctx context.Context, tenantID uuid.UUID) ([]domain.WebhookEndpoint, error) {
	rows, err := r.q.ListWebhookEndpoints(ctx, tenantID)
	if err != nil {
		return nil, domain.NewInternalError("list webhook endpoints", err)
	}
	out := make([]domain.WebhookEndpoint, len(rows))
	for i, row := range rows {
		out[i] = dbWebhookEndpointToDomain(row)
	}
	return out, nil
}

func (r *postgresWebhookRepo) UpdateEndpoint(ctx context.Context, ep domain.WebhookEndpoint) (*domain.WebhookEndpoint, error) {
	eventsJSON, err := json.Marshal(ep.Events)
	if err != nil {
		return nil, domain.NewInternalError("marshal events", err)
	}
	row, err := r.q.UpdateWebhookEndpoint(ctx, db.UpdateWebhookEndpointParams{
		ID:       ep.ID,
		TenantID: ep.TenantID,
		Url:      ep.URL,
		Events:   eventsJSON,
		IsActive: ep.IsActive,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.NewNotFoundError("webhook endpoint not found")
		}
		return nil, domain.NewInternalError("update webhook endpoint", err)
	}
	out := dbWebhookEndpointToDomain(row)
	return &out, nil
}

func (r *postgresWebhookRepo) DeleteEndpoint(ctx context.Context, tenantID, id uuid.UUID) error {
	if err := r.q.DeleteWebhookEndpoint(ctx, db.DeleteWebhookEndpointParams{
		ID:       id,
		TenantID: tenantID,
	}); err != nil {
		return domain.NewInternalError("delete webhook endpoint", err)
	}
	return nil
}

func (r *postgresWebhookRepo) ListActiveForEvent(ctx context.Context, tenantID uuid.UUID, event string) ([]domain.WebhookEndpoint, error) {
	rows, err := r.q.ListActiveWebhookEndpointsForEvent(ctx, db.ListActiveWebhookEndpointsForEventParams{
		TenantID: tenantID,
		Column2:  event,
	})
	if err != nil {
		return nil, domain.NewInternalError("list active webhook endpoints", err)
	}
	out := make([]domain.WebhookEndpoint, len(rows))
	for i, row := range rows {
		out[i] = dbWebhookEndpointToDomain(row)
	}
	return out, nil
}

func (r *postgresWebhookRepo) CreateDelivery(ctx context.Context, d domain.WebhookDelivery) (*domain.WebhookDelivery, error) {
	payloadJSON, err := json.Marshal(d.Payload)
	if err != nil {
		return nil, domain.NewInternalError("marshal delivery payload", err)
	}
	row, err := r.q.InsertWebhookDelivery(ctx, db.InsertWebhookDeliveryParams{
		EndpointID:     d.EndpointID,
		NotificationID: toNullUUID(d.NotificationID),
		Event:          d.Event,
		Payload:        payloadJSON,
		Status:         string(d.Status),
		Attempt:        int32(d.Attempt),
		NextRetryAt:    toNullTime(d.NextRetryAt),
	})
	if err != nil {
		return nil, domain.NewInternalError("insert webhook delivery", err)
	}
	out := dbWebhookDeliveryToDomain(row)
	return &out, nil
}

func (r *postgresWebhookRepo) UpdateDelivery(ctx context.Context, d domain.WebhookDelivery) (*domain.WebhookDelivery, error) {
	row, err := r.q.UpdateWebhookDelivery(ctx, db.UpdateWebhookDeliveryParams{
		ID:             d.ID,
		Status:         string(d.Status),
		Attempt:        int32(d.Attempt),
		NextRetryAt:    toNullTime(d.NextRetryAt),
		LastError:      toNullString(d.LastError),
		ResponseStatus: toNullInt32(d.ResponseStatus),
	})
	if err != nil {
		return nil, domain.NewInternalError("update webhook delivery", err)
	}
	out := dbWebhookDeliveryToDomain(row)
	return &out, nil
}

func dbWebhookEndpointToDomain(row db.WebhookEndpoint) domain.WebhookEndpoint {
	var events []string
	_ = json.Unmarshal(row.Events, &events)
	if events == nil {
		events = []string{}
	}
	return domain.WebhookEndpoint{
		ID:        row.ID,
		TenantID:  row.TenantID,
		URL:       row.Url,
		Secret:    row.Secret,
		Events:    events,
		IsActive:  row.IsActive,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}
}

func dbWebhookDeliveryToDomain(row db.WebhookDelivery) domain.WebhookDelivery {
	var payload map[string]any
	_ = json.Unmarshal(row.Payload, &payload)
	d := domain.WebhookDelivery{
		ID:          row.ID,
		EndpointID:  row.EndpointID,
		Event:       row.Event,
		Payload:     payload,
		Status:      domain.WebhookDeliveryStatus(row.Status),
		Attempt:     int(row.Attempt),
		CreatedAt:   row.CreatedAt,
		UpdatedAt:   row.UpdatedAt,
		LastError:   fromNullString(row.LastError),
		NextRetryAt: fromNullTime(row.NextRetryAt),
	}
	if row.NotificationID.Valid {
		id := row.NotificationID.UUID
		d.NotificationID = &id
	}
	if row.ResponseStatus.Valid {
		v := int(row.ResponseStatus.Int32)
		d.ResponseStatus = &v
	}
	return d
}
