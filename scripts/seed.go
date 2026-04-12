//go:build ignore

// seed.go provisions a local API client for manual E2E testing.
//
// Usage:
//
//	go run scripts/seed.go -db "postgres://notifyhub:notifyhub@localhost:5432/notifyhub?sslmode=disable"
//	go run scripts/seed.go -db "..." -name "my-client" -key "custom-raw-key"
//
// Flags:
//
//	-db    DATABASE_URL (falls back to DATABASE_URL env var)
//	-name  client name stored in the DB           (default: dev-client)
//	-key   raw API key to hash and store          (default: random 32-byte hex)
//
// Output: the raw API key to use in X-API-Key header.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	dbURL := flag.String("db", "", "PostgreSQL connection URL (falls back to DATABASE_URL env var)")
	name := flag.String("name", "dev-client", "Name for the API client record")
	rawKey := flag.String("key", "", "Raw API key to use (leave blank to generate one)")
	flag.Parse()

	if *dbURL == "" {
		*dbURL = os.Getenv("DATABASE_URL")
	}
	if *dbURL == "" {
		fatalf("DATABASE_URL is required (use -db flag or set DATABASE_URL env var)\n")
	}

	// Generate a random key if none was provided.
	if *rawKey == "" {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			fatalf("generate random key: %v\n", err)
		}
		*rawKey = hex.EncodeToString(buf)
	}

	hash := hashKey(*rawKey)

	db, err := sql.Open("pgx", *dbURL)
	if err != nil {
		fatalf("open db: %v\n", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		fatalf("ping db: %v\n", err)
	}

	var id string
	err = db.QueryRowContext(ctx,
		`INSERT INTO api_clients (name, api_key_hash)
		 VALUES ($1, $2)
		 ON CONFLICT (api_key_hash) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
		*name, hash,
	).Scan(&id)
	if err != nil {
		fatalf("insert api_client: %v\n", err)
	}

	fmt.Printf("\nAPI client seeded successfully\n")
	fmt.Printf("  ID:      %s\n", id)
	fmt.Printf("  Name:    %s\n", *name)
	fmt.Printf("  API Key: %s\n\n", *rawKey)
	fmt.Printf("Use this header in every request:\n")
	fmt.Printf("  X-API-Key: %s\n\n", *rawKey)
}

func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "seed: "+format, args...)
	os.Exit(1)
}
