package service

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/amitrajitdas31/notifyhub/internal/domain"
)

// RateLimitService checks and records per-user per-channel send rates using
// a sliding window log in Redis (sorted set, scored by Unix nanoseconds).
type RateLimitService interface {
	// Allow checks whether the user is within their rate limit for the channel.
	// It atomically records the attempt and returns (true, nil) if allowed,
	// or (false, nil) if the limit is exceeded. Non-nil err means Redis failure.
	Allow(ctx context.Context, userID string, channel domain.Channel, windowMinutes, cap int) (bool, error)

	// AllowKey is the generic form of Allow. key is the full Redis sorted-set key;
	// window and cap define the sliding window. Callers compose keys themselves.
	AllowKey(ctx context.Context, key string, window time.Duration, cap int) (bool, error)
}

type rateLimitService struct {
	redis *redis.Client
}

func NewRateLimitService(r *redis.Client) RateLimitService {
	return &rateLimitService{redis: r}
}

// Allow delegates to AllowKey using a key composed from userID and channel.
func (s *rateLimitService) Allow(ctx context.Context, userID string, channel domain.Channel, windowMinutes, cap int) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s:%s", userID, string(channel))
	return s.AllowKey(ctx, key, time.Duration(windowMinutes)*time.Minute, cap)
}

// AllowKey implements a sliding window log using a Redis sorted set.
//
// Score: current Unix nanoseconds (unique per entry)
// Steps (executed in a pipeline for efficiency):
//  1. ZREMRANGEBYSCORE — evict entries older than the window
//  2. ZCARD           — count remaining entries
//  3. ZADD            — add the current attempt
//  4. EXPIRE          — reset TTL to window duration
//
// If the count before adding is >= cap, the attempt is still recorded
// (so we don't lose the slot) but we return false to the caller.
func (s *rateLimitService) AllowKey(ctx context.Context, key string, window time.Duration, cap int) (bool, error) {
	now := time.Now()
	windowStart := now.Add(-window).UnixNano()
	score := float64(now.UnixNano())

	var countCmd *redis.IntCmd

	_, err := s.redis.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// 1. Remove entries outside the window.
		pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", windowStart))

		// 2. Count entries currently in the window (result available after pipeline exec).
		countCmd = pipe.ZCard(ctx, key)

		// 3. Add current attempt.
		pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: score})

		// 4. Set TTL so the key expires naturally.
		pipe.Expire(ctx, key, window+time.Second)

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("ratelimit pipeline: %w", err)
	}

	// countCmd.Val() is populated after TxPipelined returns.
	return countCmd.Val() < int64(cap), nil
}
