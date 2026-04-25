package ses

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
)

// sender is the subset of the SES v2 client surface we use. It lets tests
// inject a fake without spinning up the real AWS SDK.
type sender interface {
	SendEmail(ctx context.Context, in *sesv2.SendEmailInput, opts ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error)
}

// Provider delivers email via Amazon SES v2.
type Provider struct {
	client    sender
	fromEmail string
	logger    *slog.Logger
}

// New builds a Provider using the default AWS credential chain (env, shared
// config, IRSA, EC2 instance profile). Pass a logger; region and fromEmail
// come from config.
func New(ctx context.Context, region, fromEmail string, logger *slog.Logger) (*Provider, error) {
	if fromEmail == "" {
		return nil, fmt.Errorf("ses: from email required")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("ses: load aws config: %w", err)
	}
	return newWithClient(sesv2.NewFromConfig(cfg), fromEmail, logger), nil
}

// newWithClient is the seam used by tests.
func newWithClient(c sender, fromEmail string, logger *slog.Logger) *Provider {
	return &Provider{client: c, fromEmail: fromEmail, logger: logger}
}

// Channel reports that this provider handles email.
func (p *Provider) Channel() domain.Channel { return domain.ChannelEmail }

// Send builds a SendEmail request and classifies failures for the worker.
func (p *Provider) Send(ctx context.Context, n domain.Notification) error {
	subject, body, isHTML := extractContent(n.Payload)
	if body == "" {
		return &provider.ErrDeliveryPermanent{Err: fmt.Errorf("ses: empty body")}
	}
	if n.RecipientAddress == "" {
		return &provider.ErrDeliveryPermanent{Err: fmt.Errorf("ses: missing recipient address")}
	}

	input := &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(p.fromEmail),
		Destination:      &sestypes.Destination{ToAddresses: []string{n.RecipientAddress}},
		Content: &sestypes.EmailContent{
			Simple: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String(subject), Charset: aws.String("UTF-8")},
				Body:    buildBody(body, isHTML),
			},
		},
	}

	out, err := p.client.SendEmail(ctx, input)
	if err != nil {
		return classify(err)
	}
	p.logger.InfoContext(ctx, "ses delivery",
		slog.String("notification_id", n.ID.String()),
		slog.String("message_id", aws.ToString(out.MessageId)),
	)
	return nil
}

func buildBody(body string, isHTML bool) *sestypes.Body {
	if isHTML {
		return &sestypes.Body{
			Html: &sestypes.Content{Data: aws.String(body), Charset: aws.String("UTF-8")},
		}
	}
	return &sestypes.Body{
		Text: &sestypes.Content{Data: aws.String(body), Charset: aws.String("UTF-8")},
	}
}

// extractContent pulls subject/body from the payload, preferring the reserved
// keys written by the template-render step. Falls back to raw payload keys so
// non-templated sends still work.
func extractContent(payload map[string]any) (subject, body string, isHTML bool) {
	subject = firstString(payload, "_subject", "subject")
	body = firstString(payload, "_body", "body", "html", "text")
	isHTML = strings.Contains(strings.ToLower(body), "<html") || strings.Contains(body, "<!DOCTYPE")
	if _, ok := payload["html"]; ok && body == payload["html"] {
		isHTML = true
	}
	return subject, body, isHTML
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

// permanentErrorCodes are SES v2 API error codes that will not succeed on retry.
var permanentErrorCodes = map[string]struct{}{
	"MessageRejected":                    {},
	"MailFromDomainNotVerifiedException": {},
	"SendingPausedException":             {},
	"AccountSuspendedException":          {},
	"MailFromDomainNotVerified":          {},
	"InvalidParameterValue":              {},
	"ConfigurationSetDoesNotExist":       {},
}

// classify wraps an SDK error as a temporary or permanent delivery error.
func classify(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if _, ok := permanentErrorCodes[apiErr.ErrorCode()]; ok {
			return &provider.ErrDeliveryPermanent{Err: err}
		}
		if apiErr.ErrorFault() == smithy.FaultClient {
			return &provider.ErrDeliveryPermanent{Err: err}
		}
	}
	return &provider.ErrDeliveryTemporary{Err: err}
}
