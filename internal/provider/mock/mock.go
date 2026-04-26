package mock

import (
	"context"
	"log/slog"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// Provider is a log-only delivery backend for local dev and testing.
// It satisfies provider.Provider without making any external calls.
type Provider struct {
	channel domain.Channel
	logger  *slog.Logger
}

// New returns a mock Provider for the given channel.
// Register one instance per channel in the registry at startup.
func New(ch domain.Channel, logger *slog.Logger) *Provider {
	return &Provider{channel: ch, logger: logger}
}

// Send logs the notification details and returns nil, simulating a successful delivery.
func (p *Provider) Send(ctx context.Context, n domain.Notification) error {
	p.logger.InfoContext(ctx, "mock delivery",
		slog.String("channel", string(n.Channel)),
		slog.String("notification_id", n.ID.String()),
		slog.String("recipient_id", n.RecipientID),
		slog.String("recipient_address", n.RecipientAddress),
		slog.Any("payload", n.Payload),
	)
	return nil
}

// Channel returns the delivery channel this provider handles.
func (p *Provider) Channel() domain.Channel { return p.channel }

// Name returns the provider identifier used in metric labels.
func (p *Provider) Name() string { return "mock" }
