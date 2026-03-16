package main

import (
	"fmt"
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

	slog.Info("notifyhub api starting",
		"port", cfg.Port,
		"env", cfg.Environment,
		"log_level", cfg.LogLevel,
	)

	// TODO(phase-2): wire router, handlers, server
	fmt.Printf("API server will listen on :%s\n", cfg.Port)
}
