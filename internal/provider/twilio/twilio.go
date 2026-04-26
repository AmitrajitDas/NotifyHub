package twilio

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twilio/twilio-go"
	"github.com/twilio/twilio-go/client"
	api "github.com/twilio/twilio-go/rest/api/v2010"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
)

// sender is the subset of the Twilio ApiService we use. It lets tests
// inject a fake without spinning up the real SDK.
type sender interface {
	CreateMessage(params *api.CreateMessageParams) (*api.ApiV2010Message, error)
}

// Provider delivers SMS via Twilio.
type Provider struct {
	client        sender
	fromNumber    string
	messagingSvcID string
	logger        *slog.Logger
}

// New builds a Provider using Twilio credentials (account SID and auth token).
// fromNumber is the sending phone number (or empty if messagingSvcID is set).
// messagingSvcID is optional; if set, it takes precedence over fromNumber.
func New(accountSID, authToken, fromNumber, messagingSvcID string, logger *slog.Logger) (*Provider, error) {
	if accountSID == "" || authToken == "" {
		return nil, fmt.Errorf("twilio: account SID and auth token required")
	}
	if fromNumber == "" && messagingSvcID == "" {
		return nil, fmt.Errorf("twilio: either from number or messaging service ID required")
	}
	restClient := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: accountSID,
		Password: authToken,
	})
	return newWithClient(restClient.Api, fromNumber, messagingSvcID, logger), nil
}

// newWithClient is the seam used by tests.
func newWithClient(c sender, fromNumber, messagingSvcID string, logger *slog.Logger) *Provider {
	return &Provider{
		client:        c,
		fromNumber:    fromNumber,
		messagingSvcID: messagingSvcID,
		logger:        logger,
	}
}

// Channel reports that this provider handles SMS.
func (p *Provider) Channel() domain.Channel { return domain.ChannelSMS }

// Name returns the provider identifier used in metric labels.
func (p *Provider) Name() string { return "twilio" }

// Send builds a CreateMessage request and classifies failures for the worker.
func (p *Provider) Send(ctx context.Context, n domain.Notification) error {
	body := extractBody(n.Payload)
	if body == "" {
		return &provider.ErrDeliveryPermanent{Err: fmt.Errorf("twilio: empty body")}
	}
	if n.RecipientAddress == "" {
		return &provider.ErrDeliveryPermanent{Err: fmt.Errorf("twilio: missing recipient address")}
	}

	params := &api.CreateMessageParams{}
	params.SetTo(n.RecipientAddress)
	params.SetBody(body)

	if p.messagingSvcID != "" {
		params.SetMessagingServiceSid(p.messagingSvcID)
	} else {
		params.SetFrom(p.fromNumber)
	}

	msg, err := p.client.CreateMessage(params)
	if err != nil {
		return classify(err)
	}

	p.logger.InfoContext(ctx, "twilio delivery",
		slog.String("notification_id", n.ID.String()),
		slog.String("message_sid", *msg.Sid),
	)
	return nil
}

// extractBody pulls the message text from the payload, preferring the reserved
// key written by the template-render step. Falls back to other keys for
// non-templated sends.
func extractBody(payload map[string]any) string {
	for _, k := range []string{"_body", "body", "text", "message"} {
		if v, ok := payload[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// permanentErrorCodes are Twilio error codes that will not succeed on retry.
var permanentErrorCodes = map[int]struct{}{
	21211: {}, // Invalid 'To' number
	21610: {}, // Unsubscribed
	21614: {}, // Not a mobile number
	20003: {}, // Authentication failed
	21401: {}, // Invalid 'From' number
	21602: {}, // Message queue limit exceeded
	21608: {}, // Invalid phone number
	21609: {}, // Cannot send to short codes
}

// classify wraps an SDK error as a temporary or permanent delivery error.
func classify(err error) error {
	if err == nil {
		return nil
	}
	restErr, ok := err.(*client.TwilioRestError)
	if !ok {
		return &provider.ErrDeliveryTemporary{Err: err}
	}

	if _, ok := permanentErrorCodes[restErr.Code]; ok {
		return &provider.ErrDeliveryPermanent{Err: restErr}
	}

	if restErr.Code == 20429 || restErr.Code == 20003 {
		return &provider.ErrDeliveryTemporary{Err: restErr}
	}

	if restErr.Code >= 400 && restErr.Code < 500 {
		return &provider.ErrDeliveryPermanent{Err: restErr}
	}

	return &provider.ErrDeliveryTemporary{Err: restErr}
}
