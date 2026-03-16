package main

import (
	"log/slog"
	"os"

	"github.com/amitrajitdas31/notifyhub/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	slog.Info("notifyhub worker starting",
		"env", cfg.Environment,
		"email_concurrency", cfg.WorkerEmailConcurrency,
		"push_concurrency", cfg.WorkerPushConcurrency,
		"sms_concurrency", cfg.WorkerSMSConcurrency,
	)

	// TODO(phase-3): wire Kafka consumers, worker pool
}
