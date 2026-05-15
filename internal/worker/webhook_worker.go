package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
)

// WebhookWorker consumes from the outbound webhook Kafka topic and delivers
// HTTP POST callbacks to registered tenant endpoints with HMAC signatures.
// Inline retries with exponential backoff; delivery record updated after each attempt.
type WebhookWorker struct {
	consumer    *queue.Consumer
	repo        repository.WebhookRepository
	metrics     *observability.Metrics
	logger      *slog.Logger
	httpClient  *http.Client
	retryDelay  time.Duration
}

// NewWebhookWorker wires up a WebhookWorker.
func NewWebhookWorker(
	consumer *queue.Consumer,
	repo repository.WebhookRepository,
	metrics *observability.Metrics,
	retryDelay time.Duration,
	logger *slog.Logger,
) *WebhookWorker {
	return &WebhookWorker{
		consumer:   consumer,
		repo:       repo,
		metrics:    metrics,
		logger:     logger,
		retryDelay: retryDelay,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Run blocks, consuming the webhook topic until ctx is cancelled.
func (w *WebhookWorker) Run(ctx context.Context) error {
	for {
		raw, err := w.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.logger.ErrorContext(ctx, "webhook: fetch error", slog.Any("error", err))
			continue
		}

		var msg queue.WebhookMessage
		if err := json.Unmarshal(raw.Value, &msg); err != nil {
			w.logger.ErrorContext(ctx, "webhook: decode failed, skipping",
				slog.Any("error", err),
				slog.Int("bytes", len(raw.Value)),
			)
			_ = w.consumer.Commit(ctx, raw)
			continue
		}

		if err := w.process(ctx, msg); err != nil {
			// Infrastructure error — don't commit; Kafka will redeliver.
			w.logger.ErrorContext(ctx, "webhook: process error, not committing",
				slog.String("delivery_id", msg.DeliveryID.String()),
				slog.Any("error", err),
			)
			continue
		}

		if err := w.consumer.Commit(ctx, raw); err != nil {
			w.logger.ErrorContext(ctx, "webhook: offset commit failed", slog.Any("error", err))
		}
	}

	if err := w.consumer.Close(); err != nil {
		w.logger.Error("webhook: consumer close error", slog.Any("error", err))
	}
	return nil
}

// process delivers the webhook with inline retries. Returns non-nil only on
// infrastructure errors (DB write failure) that must block the offset commit.
func (w *WebhookWorker) process(ctx context.Context, msg queue.WebhookMessage) error {
	body, err := json.Marshal(msg.Payload)
	if err != nil {
		// Payload is already validated upstream; marshal failure is permanent.
		w.logger.ErrorContext(ctx, "webhook: marshal payload failed", slog.Any("error", err))
		return w.markFailed(ctx, msg, 0, "marshal payload: "+err.Error())
	}

	attempt := msg.Attempt
	for {
		statusCode, sendErr := w.send(ctx, msg.EndpointURL, msg.Secret, msg.Event, msg.DeliveryID.String(), body)

		if sendErr == nil {
			w.metrics.WebhookDelivered.WithLabelValues(msg.Event).Inc()
			w.logger.InfoContext(ctx, "webhook: delivered",
				slog.String("delivery_id", msg.DeliveryID.String()),
				slog.String("url", msg.EndpointURL),
				slog.Int("attempt", attempt),
			)
			return w.markDelivered(ctx, msg, attempt, statusCode)
		}

		w.logger.WarnContext(ctx, "webhook: send failed",
			slog.String("delivery_id", msg.DeliveryID.String()),
			slog.String("url", msg.EndpointURL),
			slog.Int("attempt", attempt),
			slog.Any("error", sendErr),
		)

		if attempt >= msg.MaxAttempts {
			w.metrics.WebhookFailed.WithLabelValues(msg.Event).Inc()
			return w.markFailed(ctx, msg, statusCode, sendErr.Error())
		}

		delay := w.retryDelay * time.Duration(math.Pow(2, float64(attempt-1)))
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during webhook retry: %w", ctx.Err())
		case <-time.After(delay):
		}
		attempt++
	}
}

// send POSTs the event payload to the endpoint URL with HMAC signature headers.
// Returns the HTTP status code and any error.
func (w *WebhookWorker) send(ctx context.Context, url, secret, event, deliveryID string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	sig := computeHMAC(body, secret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-NotifyHub-Signature", "sha256="+sig)
	req.Header.Set("X-NotifyHub-Event", event)
	req.Header.Set("X-NotifyHub-Delivery-ID", deliveryID)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("non-2xx response: %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func (w *WebhookWorker) markDelivered(ctx context.Context, msg queue.WebhookMessage, attempt, statusCode int) error {
	_, err := w.repo.UpdateDelivery(ctx, domain.WebhookDelivery{
		ID:             msg.DeliveryID,
		Status:         domain.WebhookDeliveryDelivered,
		Attempt:        attempt,
		ResponseStatus: &statusCode,
	})
	if err != nil {
		return fmt.Errorf("mark delivery delivered: %w", err)
	}
	return nil
}

func (w *WebhookWorker) markFailed(ctx context.Context, msg queue.WebhookMessage, statusCode int, errMsg string) error {
	d := domain.WebhookDelivery{
		ID:        msg.DeliveryID,
		Status:    domain.WebhookDeliveryFailed,
		Attempt:   msg.MaxAttempts,
		LastError: &errMsg,
	}
	if statusCode != 0 {
		d.ResponseStatus = &statusCode
	}
	if _, err := w.repo.UpdateDelivery(ctx, d); err != nil {
		return fmt.Errorf("mark delivery failed: %w", err)
	}
	return nil
}

// computeHMAC returns the hex-encoded HMAC-SHA256 of body using secret.
func computeHMAC(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
