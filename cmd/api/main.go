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
	"github.com/amitrajitdas31/notifyhub/internal/config"
	"github.com/amitrajitdas31/notifyhub/internal/db"
	"github.com/amitrajitdas31/notifyhub/internal/queue"
	"github.com/amitrajitdas31/notifyhub/internal/repository"
	"github.com/amitrajitdas31/notifyhub/internal/service"
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

	// 5. sqlc queries — pgx/v5/stdlib bridges pgxpool to database/sql so sqlc's DBTX interface is satisfied
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer sqlDB.Close()
	queries := db.New(sqlDB)

	// 6. Repositories
	apiClientRepo := repository.NewAPIClientRepository(queries)
	tenantRepo := repository.NewTenantRepository(queries)
	notifRepo := repository.NewNotificationRepository(queries)
	templateRepo := repository.NewTemplateRepository(queries)
	preferenceRepo := repository.NewPreferenceRepository(queries)

	// 7. Kafka producer
	publisher := queue.NewProducer(cfg.KafkaBrokers, logger)
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Error("producer close error", "error", err)
		}
	}()

	// 8. Services
	validate := validator.New()
	tenantSvc := service.NewTenantService(tenantRepo, apiClientRepo, validate)
	templateSvc := service.NewTemplateService(templateRepo, validate)
	preferenceSvc := service.NewPreferenceService(preferenceRepo, validate)
	notifSvc := service.NewNotificationService(notifRepo, publisher, validate, cfg.WorkerRetryMaxAttempts)

	// 9. Handlers
	notifHandler := handler.NewNotificationHandler(notifSvc)
	templateHandler := handler.NewTemplateHandler(templateSvc)
	prefHandler := handler.NewPreferenceHandler(preferenceSvc)
	healthHandler := handler.NewHealthHandler(pool, redisClient)
	tenantHandler := handler.NewTenantHandler(tenantSvc)

	// 10. Router
	router := api.NewRouter(api.RouterDeps{
		Auth:         apiClientRepo,
		AdminToken:   cfg.AdminToken,
		Logger:       logger,
		Notification: notifHandler,
		Template:     templateHandler,
		Preference:   prefHandler,
		Health:       healthHandler,
		Tenant:       tenantHandler,
	})

	// 11. HTTP server
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 12. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("api server starting", "port", cfg.Port, "env", cfg.Environment)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-quit
	logger.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
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
