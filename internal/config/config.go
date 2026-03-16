package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// Server
	Port        string
	LogLevel    string
	Environment string

	// Database
	DatabaseURL      string
	DatabaseMaxConns int
	DatabaseMinConns int

	// Redis
	RedisURL      string
	RedisPassword string

	// Kafka
	KafkaBrokers          []string
	KafkaPartitionCount   int
	KafkaReplicationFactor int

	// Workers
	WorkerEmailConcurrency int
	WorkerPushConcurrency  int
	WorkerSMSConcurrency   int
	WorkerInAppConcurrency int
	WorkerRetryMaxAttempts int
	WorkerRetryBaseDelayMS int

	// Providers
	SESRegion        string
	SESFromEmail     string
	FCMCredentialsFile string
	TwilioAccountSID string
	TwilioAuthToken  string
	TwilioFromNumber string

	// Rate Limiting
	RateLimitAPIRPM       int
	RateLimitEmailPerHour int
	RateLimitPushPerHour  int
	RateLimitSMSPerHour   int
	RateLimitInAppPerHour int
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:                   getEnv("PORT", "8080"),
		LogLevel:               getEnv("LOG_LEVEL", "info"),
		Environment:            getEnv("ENVIRONMENT", "development"),
		DatabaseURL:            getEnv("DATABASE_URL", ""),
		RedisURL:               getEnv("REDIS_URL", "redis://localhost:6379"),
		RedisPassword:          getEnv("REDIS_PASSWORD", ""),
		SESRegion:              getEnv("SES_REGION", "ap-south-1"),
		SESFromEmail:           getEnv("SES_FROM_EMAIL", ""),
		FCMCredentialsFile:     getEnv("FCM_CREDENTIALS_FILE", ""),
		TwilioAccountSID:       getEnv("TWILIO_ACCOUNT_SID", ""),
		TwilioAuthToken:        getEnv("TWILIO_AUTH_TOKEN", ""),
		TwilioFromNumber:       getEnv("TWILIO_FROM_NUMBER", ""),
	}

	// Required
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	// Kafka brokers (comma-separated)
	brokersRaw := getEnv("KAFKA_BROKERS", "localhost:9092")
	cfg.KafkaBrokers = strings.Split(brokersRaw, ",")

	// Integers with defaults
	var err error
	if cfg.DatabaseMaxConns, err = getEnvInt("DATABASE_MAX_CONNS", 25); err != nil {
		return nil, fmt.Errorf("DATABASE_MAX_CONNS: %w", err)
	}
	if cfg.DatabaseMinConns, err = getEnvInt("DATABASE_MIN_CONNS", 5); err != nil {
		return nil, fmt.Errorf("DATABASE_MIN_CONNS: %w", err)
	}
	if cfg.KafkaPartitionCount, err = getEnvInt("KAFKA_PARTITION_COUNT", 6); err != nil {
		return nil, fmt.Errorf("KAFKA_PARTITION_COUNT: %w", err)
	}
	if cfg.KafkaReplicationFactor, err = getEnvInt("KAFKA_REPLICATION_FACTOR", 1); err != nil {
		return nil, fmt.Errorf("KAFKA_REPLICATION_FACTOR: %w", err)
	}
	if cfg.WorkerEmailConcurrency, err = getEnvInt("WORKER_EMAIL_CONCURRENCY", 5); err != nil {
		return nil, fmt.Errorf("WORKER_EMAIL_CONCURRENCY: %w", err)
	}
	if cfg.WorkerPushConcurrency, err = getEnvInt("WORKER_PUSH_CONCURRENCY", 3); err != nil {
		return nil, fmt.Errorf("WORKER_PUSH_CONCURRENCY: %w", err)
	}
	if cfg.WorkerSMSConcurrency, err = getEnvInt("WORKER_SMS_CONCURRENCY", 2); err != nil {
		return nil, fmt.Errorf("WORKER_SMS_CONCURRENCY: %w", err)
	}
	if cfg.WorkerInAppConcurrency, err = getEnvInt("WORKER_INAPP_CONCURRENCY", 5); err != nil {
		return nil, fmt.Errorf("WORKER_INAPP_CONCURRENCY: %w", err)
	}
	if cfg.WorkerRetryMaxAttempts, err = getEnvInt("WORKER_RETRY_MAX_ATTEMPTS", 3); err != nil {
		return nil, fmt.Errorf("WORKER_RETRY_MAX_ATTEMPTS: %w", err)
	}
	if cfg.WorkerRetryBaseDelayMS, err = getEnvInt("WORKER_RETRY_BASE_DELAY_MS", 1000); err != nil {
		return nil, fmt.Errorf("WORKER_RETRY_BASE_DELAY_MS: %w", err)
	}
	if cfg.RateLimitAPIRPM, err = getEnvInt("RATELIMIT_API_RPM", 100); err != nil {
		return nil, fmt.Errorf("RATELIMIT_API_RPM: %w", err)
	}
	if cfg.RateLimitEmailPerHour, err = getEnvInt("RATELIMIT_EMAIL_PER_HOUR", 10); err != nil {
		return nil, fmt.Errorf("RATELIMIT_EMAIL_PER_HOUR: %w", err)
	}
	if cfg.RateLimitPushPerHour, err = getEnvInt("RATELIMIT_PUSH_PER_HOUR", 5); err != nil {
		return nil, fmt.Errorf("RATELIMIT_PUSH_PER_HOUR: %w", err)
	}
	if cfg.RateLimitSMSPerHour, err = getEnvInt("RATELIMIT_SMS_PER_HOUR", 3); err != nil {
		return nil, fmt.Errorf("RATELIMIT_SMS_PER_HOUR: %w", err)
	}
	if cfg.RateLimitInAppPerHour, err = getEnvInt("RATELIMIT_INAPP_PER_HOUR", 50); err != nil {
		return nil, fmt.Errorf("RATELIMIT_INAPP_PER_HOUR: %w", err)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid integer value %q: %w", v, err)
	}
	return n, nil
}
