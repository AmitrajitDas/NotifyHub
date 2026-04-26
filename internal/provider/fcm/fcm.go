package fcm

import (
	"context"
	"fmt"
	"log/slog"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
)

// messagingClient is the subset of Firebase Messaging we use. It lets tests
// inject a fake without spinning up the real SDK.
type messagingClient interface {
	Send(ctx context.Context, message *messaging.Message) (string, error)
}

// Provider delivers push notifications via Firebase Cloud Messaging.
type Provider struct {
	client messagingClient
	logger *slog.Logger
}

// New builds a Provider from a Firebase service account credentials file.
// The file is typically obtained via Google Cloud Console and contains
// the private key, project ID, and other credentials.
func New(ctx context.Context, credentialsFile string, logger *slog.Logger) (*Provider, error) {
	if credentialsFile == "" {
		return nil, fmt.Errorf("fcm: credentials file required")
	}

	opt := option.WithCredentialsFile(credentialsFile)
	app, err := firebase.NewApp(ctx, nil, opt)
	if err != nil {
		return nil, fmt.Errorf("fcm: initialize app: %w", err)
	}

	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("fcm: initialize messaging client: %w", err)
	}

	return newWithClient(client, logger), nil
}

// newWithClient is the seam used by tests.
func newWithClient(c messagingClient, logger *slog.Logger) *Provider {
	return &Provider{client: c, logger: logger}
}

// Channel reports that this provider handles push notifications.
func (p *Provider) Channel() domain.Channel { return domain.ChannelPush }

// Name returns the provider identifier used in metric labels.
func (p *Provider) Name() string { return "fcm" }

// Send builds a Message and sends it via FCM. Payload is flattened to FCM's
// Data map (strings only); subject/body go into the Notification struct.
func (p *Provider) Send(ctx context.Context, n domain.Notification) error {
	title, body := extractContent(n.Payload)
	if body == "" {
		return &provider.ErrDeliveryPermanent{Err: fmt.Errorf("fcm: empty body")}
	}
	if n.RecipientAddress == "" {
		return &provider.ErrDeliveryPermanent{Err: fmt.Errorf("fcm: missing recipient address (token)")}
	}

	notif := &messaging.Notification{
		Title: title,
		Body:  body,
	}

	data := flattenPayload(n.Payload)

	msg := &messaging.Message{
		Token:        n.RecipientAddress,
		Notification: notif,
		Data:         data,
	}

	msgID, err := p.client.Send(ctx, msg)
	if err != nil {
		return classify(err)
	}

	p.logger.InfoContext(ctx, "fcm delivery",
		slog.String("notification_id", n.ID.String()),
		slog.String("message_id", msgID),
	)
	return nil
}

// extractContent pulls title and body from the payload.
// Title prefers "_subject" or "title"; body prefers "_body" or "body".
func extractContent(payload map[string]any) (title, body string) {
	title = firstString(payload, "_subject", "title", "subject")
	body = firstString(payload, "_body", "body", "message")
	return title, body
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// flattenPayload converts all payload fields to strings for FCM's Data map.
// Skips the reserved keys (_subject, _body) since they're in Notification.
func flattenPayload(payload map[string]any) map[string]string {
	result := make(map[string]string)
	for k, v := range payload {
		if k == "_subject" || k == "_body" {
			continue
		}
		switch val := v.(type) {
		case string:
			result[k] = val
		case float64:
			result[k] = fmt.Sprintf("%v", val)
		case bool:
			result[k] = fmt.Sprintf("%v", val)
		default:
			result[k] = fmt.Sprintf("%v", val)
		}
	}
	return result
}

// classify wraps an SDK error as a temporary or permanent delivery error.
// Firebase Cloud Messaging error codes are defined in the messaging package.
func classify(err error) error {
	if err == nil {
		return nil
	}

	if messaging.IsRegistrationTokenNotRegistered(err) {
		return &provider.ErrDeliveryPermanent{Err: err}
	}
	if messaging.IsInvalidArgument(err) {
		return &provider.ErrDeliveryPermanent{Err: err}
	}
	if messaging.IsSenderIDMismatch(err) {
		return &provider.ErrDeliveryPermanent{Err: err}
	}

	if messaging.IsUnavailable(err) {
		return &provider.ErrDeliveryTemporary{Err: err}
	}
	if messaging.IsInternal(err) {
		return &provider.ErrDeliveryTemporary{Err: err}
	}
	if messaging.IsQuotaExceeded(err) {
		return &provider.ErrDeliveryTemporary{Err: err}
	}

	return &provider.ErrDeliveryTemporary{Err: err}
}
