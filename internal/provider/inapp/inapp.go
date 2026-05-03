package inapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
	"github.com/amitrajitdas31/notifyhub/internal/realtime"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

type Provider struct {
	repo   repository.InAppRepository
	rdb    *redis.Client
	logger *slog.Logger
}

func New(repo repository.InAppRepository, rdb *redis.Client, logger *slog.Logger) *Provider {
	return &Provider{repo: repo, rdb: rdb, logger: logger}
}

func (p *Provider) Channel() domain.Channel { return domain.ChannelInApp }
func (p *Provider) Name() string            { return "inapp" }

func (p *Provider) Send(ctx context.Context, n domain.Notification) error {
	title, body := extractTitleBody(n.Payload)
	if title == "" || body == "" {
		return &provider.ErrDeliveryPermanent{
			Err: errors.New("inapp notification requires title and body in payload"),
		}
	}

	notifID := &n.ID
	msg := domain.InAppMessage{
		TenantID:       n.TenantID,
		NotificationID: notifID,
		RecipientID:    n.RecipientID,
		Title:          title,
		Body:           body,
		Payload:        n.Payload,
	}

	saved, err := p.repo.Insert(ctx, msg)
	if err != nil {
		return &provider.ErrDeliveryTemporary{Err: fmt.Errorf("persist inapp message: %w", err)}
	}

	payload, err := json.Marshal(saved)
	if err != nil {
		p.logger.Warn("inapp: failed to marshal for pub/sub", slog.Any("error", err))
		return nil
	}

	ch := realtime.RedisChannel(n.TenantID.String(), n.RecipientID)
	if err := p.rdb.Publish(ctx, ch, payload).Err(); err != nil {
		p.logger.Warn("inapp: redis publish failed", slog.Any("error", err))
		// Non-fatal: message persisted; real-time delivery best-effort.
	}
	return nil
}

func extractTitleBody(payload map[string]any) (title, body string) {
	// Template render sets _subject → title, _body → body.
	if v, ok := payload["_subject"].(string); ok && v != "" {
		title = v
	} else if v, ok := payload["title"].(string); ok {
		title = v
	}
	if v, ok := payload["_body"].(string); ok && v != "" {
		body = v
	} else if v, ok := payload["body"].(string); ok {
		body = v
	}
	return title, body
}
