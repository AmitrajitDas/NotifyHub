package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// Provider is the interface every delivery backend must implement.
// The worker calls Send and never touches the concrete type directly.
type Provider interface {
	// Send delivers the notification. Returns nil on success, a typed
	// ErrDeliveryTemporary or ErrDeliveryPermanent on failure so the
	// worker can decide whether to retry.
	Send(ctx context.Context, n domain.Notification) error

	// Channel returns the delivery channel this provider handles.
	// Used at registration time and in log/metric labels.
	Channel() domain.Channel

	// Name returns a short human-readable identifier for this backend
	// (e.g. "ses", "fcm", "twilio", "mock"). Used as a Prometheus label
	// and span attribute so metrics stay low-cardinality and informative.
	Name() string
}

// ErrDeliveryTemporary signals a transient failure (network blip, upstream
// rate limit). The worker should retry with backoff.
type ErrDeliveryTemporary struct{ Err error }

func (e *ErrDeliveryTemporary) Error() string { return fmt.Sprintf("temporary delivery error: %s", e.Err) }
func (e *ErrDeliveryTemporary) Unwrap() error { return e.Err }

// ErrDeliveryPermanent signals a non-retryable failure (invalid address,
// provider rejection). The worker should mark the notification as failed.
type ErrDeliveryPermanent struct{ Err error }

func (e *ErrDeliveryPermanent) Error() string { return fmt.Sprintf("permanent delivery error: %s", e.Err) }
func (e *ErrDeliveryPermanent) Unwrap() error { return e.Err }

// IsTemporary reports whether err is a transient delivery failure.
func IsTemporary(err error) bool {
	var t *ErrDeliveryTemporary
	return errors.As(err, &t)
}

// IsPermanent reports whether err is a permanent delivery failure.
func IsPermanent(err error) bool {
	var p *ErrDeliveryPermanent
	return errors.As(err, &p)
}

// ErrNoProvider is returned by Registry.Get when no provider is registered
// for the requested channel.
var ErrNoProvider = errors.New("no provider registered for channel")

// Registry maps each delivery channel to its Provider implementation.
// Populated once at startup; read-only at runtime — no locking needed.
type Registry struct {
	providers map[domain.Channel]Provider
}

// NewRegistry returns an empty Registry ready for provider registration.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[domain.Channel]Provider)}
}

// Register adds p to the registry keyed by p.Channel().
// Calling Register twice for the same channel overwrites the previous entry.
func (r *Registry) Register(p Provider) {
	r.providers[p.Channel()] = p
}

// Get returns the Provider for ch. Returns ErrNoProvider if none is registered.
func (r *Registry) Get(ch domain.Channel) (Provider, error) {
	p, ok := r.providers[ch]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoProvider, ch)
	}
	return p, nil
}
