package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
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
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/provider"
	fcmprovider "github.com/amitrajitdas31/notifyhub/internal/provider/fcm"
	inappprovider "github.com/amitrajitdas31/notifyhub/internal/provider/inapp"
	mockprovider "github.com/amitrajitdas31/notifyhub/internal/provider/mock"
	sesprovider "github.com/amitrajitdas31/notifyhub/internal/provider/ses"
	twilioprovider "github.com/amitrajitdas31/notifyhub/internal/provider/twilio"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
	"github.com/amitrajitdas31/notifyhub/internal/service"
	"github.com/amitrajitdas31/notifyhub/internal/worker"
)

// version and commit are injected at build time via -ldflags.
var (
	version = "dev"
	commit  = "unknown"
)

const (
	kafkaGroupID      = "notifyhub-worker"
	schedulerInterval = 15 * time.Second
	schedulerBatch    = 100
)

func main() {
	// 1. Config, logger, tracing, metrics
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceVersion = version
	cfg.GitCommit = commit

	logger := buildLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	shutdownTracer, err := observability.InitTracer(context.Background(), observability.TracingConfig{
		Endpoint:    cfg.OTELEndpoint,
		Insecure:    cfg.OTELInsecure,
		ServiceName: "notifyhub-worker",
		Version:     cfg.ServiceVersion,
		Environment: cfg.Environment,
		SampleRatio: cfg.OTELSampleRatio,
	})
	if err != nil {
		logger.Error("failed to init tracer", "error", err)
		os.Exit(1)
	}

	metrics := observability.NewMetrics(cfg.ServiceVersion, cfg.GitCommit, cfg.Environment)

	// 2. Postgres
	pool, err := buildPool(cfg)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("postgres connected")

	// 3. Redis
	redisClient, err := buildRedis(cfg)
	if err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer redisClient.Close()
	logger.Info("redis connected")

	// 4. sqlc queries
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	queries := db.New(sqlDB)

	// 5. Repositories
	notifRepo := repository.NewNotificationRepository(queries)
	delivRepo := repository.NewDeliveryRepository(queries)
	prefRepo := repository.NewPreferenceRepository(queries)
	templateRepo := repository.NewTemplateRepository(queries)
	dlqRepo := repository.NewDeadLetterRepository(queries)
	inappRepo := repository.NewInAppRepository(queries)
	webhookRepo := repository.NewWebhookRepository(queries)

	// 6. Services
	validate := validator.New()
	preferenceSvc := service.NewPreferenceService(prefRepo, validate)
	rateLimitSvc := service.NewRateLimitService(redisClient)
	templateSvc := service.NewTemplateService(templateRepo, validate, metrics)

	// 7. Kafka producer (scheduler uses this to enqueue due notifications)
	producer := queue.NewProducer(cfg.KafkaBrokers, logger)
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("producer close error", "error", err)
		}
	}()

	webhookSvc := service.NewWebhookService(webhookRepo, producer, cfg.WebhookMaxAttempts, validate, logger)

	// 8. Pre-create Kafka topics so consumers never join with empty assignments.
	// CreateTopics is idempotent — safe to call on every startup.
	if err := queue.EnsureTopics(context.Background(), cfg.KafkaBrokers, logger); err != nil {
		logger.Error("failed to ensure kafka topics", "error", err)
		os.Exit(1)
	}

	// 9. Provider registry, processor, scheduler
	registry := buildRegistry(cfg, logger, inappRepo, redisClient)

	processor := worker.NewProcessor(worker.ProcessorDeps{
		NotificationRepo: notifRepo,
		DeliveryRepo:     delivRepo,
		Preferences:      preferenceSvc,
		RateLimit:        rateLimitSvc,
		Templates:        templateSvc,
		Registry:         registry,
		Metrics:          metrics,
		RateCaps: map[domain.Channel]int{
			domain.ChannelEmail: cfg.RateLimitEmailPerHour,
			domain.ChannelPush:  cfg.RateLimitPushPerHour,
			domain.ChannelSMS:   cfg.RateLimitSMSPerHour,
			domain.ChannelInApp: cfg.RateLimitInAppPerHour,
		},
		RetryDelay:   time.Duration(cfg.WorkerRetryBaseDelayMS) * time.Millisecond,
		Logger:       logger,
		DLQPublisher: producer,
		DLQEnabled:   cfg.DLQEnabled,
		Webhook:      webhookSvc,
	})

	scheduler := service.NewScheduler(notifRepo, producer, schedulerInterval, schedulerBatch, cfg.WorkerRetryMaxAttempts, logger)

	// 9. Health checker — shared by admin server and used for /readyz
	checker := observability.NewChecker(3*time.Second,
		observability.PostgresProbe(pool),
		observability.RedisProbe(redisClient),
		observability.KafkaProbe(cfg.KafkaBrokers),
	)

	// 10. Admin HTTP server — /metrics, /livez, /readyz, /health on MetricsPort
	adminSrv := startAdminServer(cfg.MetricsPort, metrics, checker)

	// 11. Root context cancelled on SIGTERM/SIGINT
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("notifyhub worker starting",
		slog.String("env", cfg.Environment),
		slog.String("version", version),
		slog.String("metrics_port", cfg.MetricsPort),
		slog.Int("email_concurrency", cfg.WorkerEmailConcurrency),
		slog.Int("push_concurrency", cfg.WorkerPushConcurrency),
		slog.Int("sms_concurrency", cfg.WorkerSMSConcurrency),
		slog.Int("inapp_concurrency", cfg.WorkerInAppConcurrency),
	)

	// 12. Launch pools, DLQ consumers, scheduler, and webhook worker
	var wg sync.WaitGroup
	startPools(ctx, cfg, processor, logger, &wg)
	startDLQConsumers(ctx, cfg, dlqRepo, metrics, logger, &wg)
	startScheduler(ctx, scheduler, logger, &wg)
	startWebhookWorker(ctx, cfg, webhookRepo, metrics, logger, &wg)

	// 13. Block until signal, then drain in order
	<-ctx.Done()
	logger.Info("shutdown signal received, draining...")

	// Wait for all in-flight messages to finish before stopping infra.
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := adminSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("admin server shutdown failed", "error", err)
	}

	tracerCtx, tracerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tracerCancel()
	if err := shutdownTracer(tracerCtx); err != nil {
		logger.Error("tracer flush failed", "error", err)
	}

	logger.Info("worker stopped cleanly")
}

// startAdminServer runs /metrics, /livez, /readyz, /health on port in a goroutine.
func startAdminServer(port string, metrics *observability.Metrics, checker *observability.Checker) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/livez", checker.Live)
	mux.HandleFunc("/readyz", checker.Ready)
	mux.HandleFunc("/health", checker.Health)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin server error", "error", err)
		}
	}()
	return srv
}

// startPools launches one worker.Pool goroutine per channel.
func startPools(ctx context.Context, cfg *config.Config, proc *worker.Processor, logger *slog.Logger, wg *sync.WaitGroup) {
	type entry struct {
		channel     domain.Channel
		concurrency int
	}
	channels := []entry{
		{domain.ChannelEmail, cfg.WorkerEmailConcurrency},
		{domain.ChannelPush, cfg.WorkerPushConcurrency},
		{domain.ChannelSMS, cfg.WorkerSMSConcurrency},
		{domain.ChannelInApp, cfg.WorkerInAppConcurrency},
	}
	for _, e := range channels {
		consumer := queue.NewConsumer(cfg.KafkaBrokers, queue.TopicForChannel(e.channel), kafkaGroupID, logger)
		p := worker.NewPool(consumer, proc, e.concurrency, logger)

		wg.Add(1)
		go func(p *worker.Pool, ch domain.Channel) {
			defer wg.Done()
			logger.Info("pool started", slog.String("channel", string(ch)))
			if err := p.Run(ctx); err != nil {
				logger.Error("pool error", slog.String("channel", string(ch)), slog.Any("error", err))
			}
			logger.Info("pool stopped", slog.String("channel", string(ch)))
		}(p, e.channel)
	}
}

// startDLQConsumers launches one DLQConsumer goroutine per channel when enabled.
func startDLQConsumers(ctx context.Context, cfg *config.Config, repo repository.DeadLetterRepository, metrics *observability.Metrics, logger *slog.Logger, wg *sync.WaitGroup) {
	if !cfg.DLQConsumerEnabled {
		return
	}
	channels := []domain.Channel{
		domain.ChannelEmail,
		domain.ChannelPush,
		domain.ChannelSMS,
		domain.ChannelInApp,
	}
	for _, ch := range channels {
		consumer := queue.NewConsumer(cfg.KafkaBrokers, queue.DLQTopicForChannel(ch), cfg.DLQGroupID, logger)
		dlqConsumer := worker.NewDLQConsumer(consumer, repo, metrics, logger)

		wg.Add(1)
		go func(c *worker.DLQConsumer, ch domain.Channel) {
			defer wg.Done()
			logger.Info("dlq consumer started", slog.String("channel", string(ch)))
			if err := c.Run(ctx); err != nil {
				logger.Error("dlq consumer error", slog.String("channel", string(ch)), slog.Any("error", err))
			}
			logger.Info("dlq consumer stopped", slog.String("channel", string(ch)))
		}(dlqConsumer, ch)
	}
}

// startScheduler launches the scheduler goroutine.
func startScheduler(ctx context.Context, sched *service.Scheduler, logger *slog.Logger, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := sched.Run(ctx); err != nil {
			logger.Error("scheduler error", slog.Any("error", err))
		}
	}()
}

// startWebhookWorker launches the outbound webhook delivery goroutine when enabled.
func startWebhookWorker(ctx context.Context, cfg *config.Config, repo repository.WebhookRepository, metrics *observability.Metrics, logger *slog.Logger, wg *sync.WaitGroup) {
	if !cfg.WebhookEnabled {
		return
	}
	consumer := queue.NewConsumer(cfg.KafkaBrokers, queue.WebhookTopic, cfg.WebhookGroupID, logger)
	w := worker.NewWebhookWorker(
		consumer,
		repo,
		metrics,
		time.Duration(cfg.WebhookRetryBaseMS)*time.Millisecond,
		logger,
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("webhook worker started")
		if err := w.Run(ctx); err != nil {
			logger.Error("webhook worker error", slog.Any("error", err))
		}
		logger.Info("webhook worker stopped")
	}()
}

// buildRegistry registers real providers when credentials are configured,
// falling back to the mock for any channel without credentials.
func buildRegistry(cfg *config.Config, logger *slog.Logger, inappRepo repository.InAppRepository, rdb *redis.Client) *provider.Registry {
	reg := provider.NewRegistry()
	reg.Register(buildEmailProvider(cfg, logger))
	reg.Register(buildPushProvider(cfg, logger))
	reg.Register(buildSMSProvider(cfg, logger))
	reg.Register(inappprovider.New(inappRepo, rdb, logger))
	return reg
}

func buildEmailProvider(cfg *config.Config, logger *slog.Logger) provider.Provider {
	if cfg.SESFromEmail == "" {
		return mockprovider.New(domain.ChannelEmail, logger)
	}
	p, err := sesprovider.New(context.Background(), cfg.SESRegion, cfg.SESFromEmail, logger)
	if err != nil {
		logger.Error("SES init failed, using mock", "error", err)
		return mockprovider.New(domain.ChannelEmail, logger)
	}
	logger.Info("registered SES email provider")
	return p
}

func buildPushProvider(cfg *config.Config, logger *slog.Logger) provider.Provider {
	if cfg.FCMCredentialsFile == "" {
		return mockprovider.New(domain.ChannelPush, logger)
	}
	p, err := fcmprovider.New(context.Background(), cfg.FCMCredentialsFile, logger)
	if err != nil {
		logger.Error("FCM init failed, using mock", "error", err)
		return mockprovider.New(domain.ChannelPush, logger)
	}
	logger.Info("registered FCM push provider")
	return p
}

func buildSMSProvider(cfg *config.Config, logger *slog.Logger) provider.Provider {
	if cfg.TwilioAccountSID == "" || cfg.TwilioAuthToken == "" {
		return mockprovider.New(domain.ChannelSMS, logger)
	}
	p, err := twilioprovider.New(cfg.TwilioAccountSID, cfg.TwilioAuthToken, cfg.TwilioFromNumber, "", logger)
	if err != nil {
		logger.Error("Twilio init failed, using mock", "error", err)
		return mockprovider.New(domain.ChannelSMS, logger)
	}
	logger.Info("registered Twilio SMS provider")
	return p
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
	return observability.NewLogger(l)
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

func buildRedis(cfg *config.Config) (*redis.Client, error) {
	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, err
	}
	if cfg.RedisPassword != "" {
		opts.Password = cfg.RedisPassword
	}
	client := redis.NewClient(opts)
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
