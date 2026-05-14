package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/amitrajitdas31/notifyhub/internal/api"
	"github.com/amitrajitdas31/notifyhub/internal/api/handler"
	"github.com/amitrajitdas31/notifyhub/internal/auth"
	"github.com/amitrajitdas31/notifyhub/internal/config"
	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/observability"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/realtime"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
	"github.com/amitrajitdas31/notifyhub/internal/service"
)

// version and commit are injected at build time via -ldflags.
var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	// 1. Config
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	cfg.ServiceVersion = version
	cfg.GitCommit = commit

	// 2. Logger — JSON handler with trace_id / span_id injection
	logger := buildLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	// 3. Tracing — no-op when OTEL_EXPORTER_OTLP_ENDPOINT is empty
	shutdownTracer, err := observability.InitTracer(context.Background(), observability.TracingConfig{
		Endpoint:    cfg.OTELEndpoint,
		Insecure:    cfg.OTELInsecure,
		ServiceName: "notifyhub-api",
		Version:     cfg.ServiceVersion,
		Environment: cfg.Environment,
		SampleRatio: cfg.OTELSampleRatio,
	})
	if err != nil {
		logger.Error("failed to init tracer", "error", err)
		os.Exit(1)
	}

	// 4. Metrics
	metrics := observability.NewMetrics(cfg.ServiceVersion, cfg.GitCommit, cfg.Environment)

	// 5. Postgres
	pool, err := buildPool(cfg)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("postgres connected")

	// 6. Redis
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

	// 7. sqlc queries
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	queries := db.New(sqlDB)

	// 8. Repositories
	apiClientRepo := repository.NewAPIClientRepository(queries)
	tenantRepo := repository.NewTenantRepository(queries)
	notifRepo := repository.NewNotificationRepository(queries)
	templateRepo := repository.NewTemplateRepository(queries)
	preferenceRepo := repository.NewPreferenceRepository(queries)
	dlqRepo := repository.NewDeadLetterRepository(queries)
	inappRepo := repository.NewInAppRepository(queries)
	deviceTokenRepo := repository.NewDeviceTokenRepository(queries)
	webhookRepo := repository.NewWebhookRepository(queries)

	// 9. Kafka producer
	publisher := queue.NewProducer(cfg.KafkaBrokers, logger)
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Error("producer close error", "error", err)
		}
	}()

	// 10. Services
	validate := validator.New()
	tenantSvc := service.NewTenantService(tenantRepo, apiClientRepo, validate)
	templateSvc := service.NewTemplateService(templateRepo, validate, metrics)
	preferenceSvc := service.NewPreferenceService(preferenceRepo, validate)
	notifSvc := service.NewNotificationService(notifRepo, publisher, validate, metrics, cfg.WorkerRetryMaxAttempts)
	rateLimitSvc := service.NewRateLimitService(redisClient)
	deviceTokenSvc := service.NewDeviceTokenService(deviceTokenRepo, validate)
	wsTokenSvc := auth.NewWSTokenService(cfg.WSJWTSecret, cfg.WSJWTTTLSeconds)
	webhookSvc := service.NewWebhookService(webhookRepo, publisher, cfg.WebhookMaxAttempts, validate, logger)

	// 10b. In-app realtime hub — subscribe to Redis pub/sub
	hub := realtime.NewHub(redisClient, logger, cfg.InAppWSBufferSize)
	go func() {
		if err := hub.Run(context.Background()); err != nil {
			logger.Error("inapp hub error", "error", err)
		}
	}()

	// 11. Health checker — probes Postgres, Redis, and Kafka
	checker := observability.NewChecker(3*time.Second,
		observability.PostgresProbe(pool),
		observability.RedisProbe(redisClient),
		observability.KafkaProbe(cfg.KafkaBrokers),
	)

	// 12. Handlers
	notifHandler := handler.NewNotificationHandler(notifSvc)
	templateHandler := handler.NewTemplateHandler(templateSvc)
	prefHandler := handler.NewPreferenceHandler(preferenceSvc)
	tenantHandler := handler.NewTenantHandler(tenantSvc)
	dlqHandler := handler.NewDLQHandler(dlqRepo, publisher, cfg.WorkerRetryMaxAttempts)
	deviceTokenHandler := handler.NewDeviceTokenHandler(deviceTokenSvc)
	webhookHandler := handler.NewWebhookHandler(webhookSvc)
	wsTokenHandler := handler.NewWSTokenHandler(wsTokenSvc)
	inboxHandler := handler.NewInboxHandler(inappRepo, hub, wsTokenSvc, metrics, logger, handler.InboxHandlerConfig{
		HeartbeatSeconds:   cfg.InAppWSHeartbeatSeconds,
		ReadTimeoutSeconds: cfg.InAppWSReadTimeoutSeconds,
	})

	// 13. Router
	router := api.NewRouter(api.RouterDeps{
		Auth:         apiClientRepo,
		AdminToken:   cfg.AdminToken,
		Logger:       logger,
		RateLimit:    rateLimitSvc,
		RateLimitRPM: cfg.RateLimitAPIRPM,
		Metrics:      metrics,
		Health:       checker,
		Notification: notifHandler,
		Template:     templateHandler,
		Preference:   prefHandler,
		Tenant:       tenantHandler,
		DLQ:          dlqHandler,
		Inbox:        inboxHandler,
		DeviceToken:  deviceTokenHandler,
		WSToken:      wsTokenHandler,
		Webhook:      webhookHandler,
	})

	// 14. HTTP server
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 15. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("api server starting", "port", cfg.Port, "env", cfg.Environment, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-quit
	logger.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown order: stop accepting → flush tracer → done.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}

	tracerCtx, tracerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tracerCancel()
	if err := shutdownTracer(tracerCtx); err != nil {
		logger.Error("tracer flush failed", "error", err)
	}

	logger.Info("server stopped")
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

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
