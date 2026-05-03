package realtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Hub manages in-process WebSocket connections keyed by "{tenantID}:{recipientID}".
// One Redis PSUBSCRIBE goroutine fans published events out to all local subscribers.
type Hub struct {
	mu      sync.RWMutex
	conns   map[string]map[chan []byte]struct{}
	rdb     *redis.Client
	logger  *slog.Logger
	bufSize int
}

func NewHub(rdb *redis.Client, logger *slog.Logger, bufSize int) *Hub {
	if bufSize <= 0 {
		bufSize = 32
	}
	return &Hub{
		conns:   make(map[string]map[chan []byte]struct{}),
		rdb:     rdb,
		logger:  logger,
		bufSize: bufSize,
	}
}

// Subscribe registers a new listener for the given tenant+recipient pair.
// The returned channel receives JSON-encoded InAppMessage payloads.
// Call cancel when the WebSocket connection closes.
func (h *Hub) Subscribe(tenantID, recipientID string) (<-chan []byte, func()) {
	key := tenantID + ":" + recipientID
	ch := make(chan []byte, h.bufSize)

	h.mu.Lock()
	if h.conns[key] == nil {
		h.conns[key] = make(map[chan []byte]struct{})
	}
	h.conns[key][ch] = struct{}{}
	h.mu.Unlock()

	cancel := func() {
		h.mu.Lock()
		delete(h.conns[key], ch)
		if len(h.conns[key]) == 0 {
			delete(h.conns, key)
		}
		h.mu.Unlock()
		close(ch)
	}
	return ch, cancel
}

// Publish delivers msg to all local subscribers for key "{tenantID}:{recipientID}".
// Drops on full channel buffer; never blocks.
func (h *Hub) Publish(tenantID, recipientID string, msg []byte) {
	key := tenantID + ":" + recipientID
	h.mu.RLock()
	chans := h.conns[key]
	h.mu.RUnlock()
	for ch := range chans {
		select {
		case ch <- msg:
		default:
			h.logger.Warn("inapp hub: dropped message (buffer full)",
				slog.String("tenant_id", tenantID),
				slog.String("recipient_id", recipientID),
			)
		}
	}
}

// Run starts the Redis PSUBSCRIBE loop. Blocks until ctx is cancelled.
// Launch in a goroutine from main.
func (h *Hub) Run(ctx context.Context) error {
	pubsub := h.rdb.PSubscribe(ctx, "inapp:*:*")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			// Pattern: inapp:{tenant_id}:{recipient_id}
			// Strip "inapp:" prefix then split on first ":"
			rest := strings.TrimPrefix(msg.Channel, "inapp:")
			idx := strings.IndexByte(rest, ':')
			if idx < 0 {
				h.logger.Warn("inapp hub: malformed redis channel", slog.String("channel", msg.Channel))
				continue
			}
			tenantID := rest[:idx]
			recipientID := rest[idx+1:]
			h.Publish(tenantID, recipientID, []byte(msg.Payload))
		}
	}
}

// RedisChannel returns the Redis pub/sub channel name for a given tenant+recipient.
func RedisChannel(tenantID, recipientID string) string {
	return fmt.Sprintf("inapp:%s:%s", tenantID, recipientID)
}
