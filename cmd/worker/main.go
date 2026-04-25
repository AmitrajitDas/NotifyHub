package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/amitrajitdas31/notifyhub/internal/config"
	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/domain"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
	fcmprovider "github.com/amitrajitdas31/notifyhub/internal/provider/fcm"
	mockprovider "github.com/amitrajitdas31/notifyhub/internal/provider/mock"
	sesprovider "github.com/amitrajitdas31/notifyhub/internal/provider/ses"
	twilioprovider "github.com/amitrajitdas31/notifyhub/internal/provider/twilio"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
	"github.com/amitrajitdas31/notifyhub/internal/service"
	"github.com/amitrajitdas31/notifyhub/internal/worker"
)

const (
	kafkaGroupID      = "notifyhub-worker"
	schedulerInterval = 15 * time.Second
	schedulerBatch    = 100
)

func main() {
	// 1. Config
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// 2. Logger
	logger := buildLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	// 3. Postgres
	pool, err := buildPool(cfg)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("postgres connected")

	// 4. Redis
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		logger.Error("invalid redis url", "error", err)
		os.Exit(1)
	}
	if cfg.RedisPassword != "" {
		redisOpts.Password = cfg.RedisPassword
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	logger.Info("redis connected")

	// 5. sqlc queries
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	queries := db.New(sqlDB)

	// 6. Repositories
	notifRepo := repository.NewNotificationRepository(queries)
	delivRepo := repository.NewDeliveryRepository(queries)
	prefRepo := repository.NewPreferenceRepository(queries)
	templateRepo := repository.NewTemplateRepository(queries)

	// 7. Services
	validate := validator.New()
	preferenceSvc := service.NewPreferenceService(prefRepo, validate)
	rateLimitSvc := service.NewRateLimitService(redisClient)
	templateSvc := service.NewTemplateService(templateRepo, validate)

	// 8. Kafka producer (used by the scheduler to enqueue due notifications)
	producer := queue.NewProducer(cfg.KafkaBrokers, logger)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("producer close error", "error", err)
		}
	}()

	// 9. Provider registry
	// Registers real providers if credentials are configured, falls back to mock for local dev.
	registry := provider.NewRegistry()

	// Email: SES if configured, else mock
	if cfg.SESFromEmail != "" {
		emailProvider, err := sesprovider.New(context.Background(), cfg.SESRegion, cfg.SESFromEmail, logger)
		if err != nil {
			logger.Error("failed to initialize SES provider, falling back to mock", "error", err)
			registry.Register(mockprovider.New(domain.ChannelEmail, logger))
		} else {
			registry.Register(emailProvider)
			logger.Info("registered SES email provider")
		}
	} else {
		registry.Register(mockprovider.New(domain.ChannelEmail, logger))
	}

	// Push: FCM if configured, else mock
	if cfg.FCMCredentialsFile != "" {
		pushProvider, err := fcmprovider.New(context.Background(), cfg.FCMCredentialsFile, logger)
		if err != nil {
			logger.Error("failed to initialize FCM provider, falling back to mock", "error", err)
			registry.Register(mockprovider.New(domain.ChannelPush, logger))
		} else {
			registry.Register(pushProvider)
			logger.Info("registered FCM push provider")
		}
	} else {
		registry.Register(mockprovider.New(domain.ChannelPush, logger))
	}

	// SMS: Twilio if configured, else mock
	if cfg.TwilioAccountSID != "" && cfg.TwilioAuthToken != "" {
		smsProvider, err := twilioprovider.New(
			cfg.TwilioAccountSID,
			cfg.TwilioAuthToken,
			cfg.TwilioFromNumber,
			"",
			logger,
		)
		if err != nil {
			logger.Error("failed to initialize Twilio provider, falling back to mock", "error", err)
			registry.Register(mockprovider.New(domain.ChannelSMS, logger))
		} else {
			registry.Register(smsProvider)
			logger.Info("registered Twilio SMS provider")
		}
	} else {
		registry.Register(mockprovider.New(domain.ChannelSMS, logger))
	}

	// InApp: always mock (no external provider)
	registry.Register(mockprovider.New(domain.ChannelInApp, logger))

	// 10. Per-channel rate caps (notifications per hour)
	rateCaps := map[domain.Channel]int{
		domain.ChannelEmail: cfg.RateLimitEmailPerHour,
		domain.ChannelPush:  cfg.RateLimitPushPerHour,
		domain.ChannelSMS:   cfg.RateLimitSMSPerHour,
		domain.ChannelInApp: cfg.RateLimitInAppPerHour,
	}

	// 11. Processor (stateless — shared safely across all pools)
	processor := worker.NewProcessor(
		notifRepo,
		delivRepo,
		preferenceSvc,
		rateLimitSvc,
		templateSvc,
		registry,
		rateCaps,
		time.Duration(cfg.WorkerRetryBaseDelayMS)*time.Millisecond,
		logger,
	)

	// 12. One pool per channel, each with its own consumer
	type poolCfg struct {
		channel     domain.Channel
		concurrency int
	}
	channelPools := []poolCfg{
		{domain.ChannelEmail, cfg.WorkerEmailConcurrency},
		{domain.ChannelPush, cfg.WorkerPushConcurrency},
		{domain.ChannelSMS, cfg.WorkerSMSConcurrency},
		{domain.ChannelInApp, cfg.WorkerInAppConcurrency},
	}

	// 13. Scheduler
	scheduler := service.NewScheduler(
		notifRepo,
		producer,
		schedulerInterval,
		schedulerBatch,
		cfg.WorkerRetryMaxAttempts,
		logger,
	)

	// 14. Root context — cancelled on SIGTERM/SIGINT
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("notifyhub worker starting",
		slog.String("env", cfg.Environment),
		slog.Int("email_concurrency", cfg.WorkerEmailConcurrency),
		slog.Int("push_concurrency", cfg.WorkerPushConcurrency),
		slog.Int("sms_concurrency", cfg.WorkerSMSConcurrency),
		slog.Int("inapp_concurrency", cfg.WorkerInAppConcurrency),
	)

	// 15. Launch all goroutines
	var wg sync.WaitGroup

	for _, pc := range channelPools {
		consumer := queue.NewConsumer(
			cfg.KafkaBrokers,
			queue.TopicForChannel(pc.channel),
			kafkaGroupID,
			logger,
		)
		p := worker.NewPool(consumer, processor, pc.concurrency, logger)

		wg.Add(1)
		go func(p *worker.Pool, ch domain.Channel) {
			defer wg.Done()
			logger.Info("pool started", slog.String("channel", string(ch)))
			if err := p.Run(ctx); err != nil {
				logger.Error("pool exited with error",
					slog.String("channel", string(ch)),
					slog.Any("error", err),
				)
			}
			logger.Info("pool stopped", slog.String("channel", string(ch)))
		}(p, pc.channel)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := scheduler.Run(ctx); err != nil {
			logger.Error("scheduler exited with error", slog.Any("error", err))
		}
	}()

	// 16. Block until context is cancelled (SIGTERM/SIGINT)
	<-ctx.Done()
	logger.Info("shutdown signal received, draining...")

	// Wait for all pools and scheduler to finish in-flight work
	wg.Wait()
	logger.Info("worker stopped cleanly")
}

func buildLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func buildPool(cfg *config.Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = int32(cfg.DatabaseMaxConns)
	poolCfg.MinConns = int32(cfg.DatabaseMinConns)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}
