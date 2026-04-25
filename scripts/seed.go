//go:build ignore

// seed.go provisions a local tenant + API client for manual E2E testing.
//
// Usage:
//
//	go run scripts/seed.go -db "postgres://user:pass@localhost:5432/notifyhub?sslmode=disable"
//	go run scripts/seed.go -db "..." -tenant "acme" -name "my-client" -key "custom-raw-key"
//
// Flags:
//
//	-db      DATABASE_URL (falls back to DATABASE_URL env var)
//	-tenant  tenant name stored in the DB             (default: default-tenant)
//	-name    client name stored in the DB             (default: dev-client)
//	-key     raw API key to hash and store            (default: random 32-byte hex)
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
	"path/filepath"
	"runtime"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	tmplloader "github.com/amitrajitdas31/notifyhub/internal/template"
)

func main() {
	dbURL := flag.String("db", "", "PostgreSQL connection URL (falls back to DATABASE_URL env var)")
	tenantName := flag.String("tenant", "default-tenant", "Name for the tenant record")
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

	// Upsert tenant.
	var tenantID string
	err = db.QueryRowContext(ctx,
		`INSERT INTO tenants (name)
		 VALUES ($1)
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
		*tenantName,
	).Scan(&tenantID)
	if err != nil {
		fatalf("insert tenant: %v\n", err)
	}

	// Upsert API client scoped to tenant.
	var clientID string
	err = db.QueryRowContext(ctx,
		`INSERT INTO api_clients (tenant_id, name, api_key_hash)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (api_key_hash) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`,
		tenantID, *name, hash,
	).Scan(&clientID)
	if err != nil {
		fatalf("insert api_client: %v\n", err)
	}

	// Seed starter templates from the templates/ directory.
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(thisFile))
	tmplCount := 0
	for _, subdir := range []string{"email", "sms"} {
		dir := filepath.Join(repoRoot, "templates", subdir)
		templates, err := tmplloader.LoadFromDir(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: load templates from %s: %v\n", dir, err)
			continue
		}
		for _, t := range templates {
			var subj *string
			if t.SubjectTemplate != nil {
				subj = t.SubjectTemplate
			}
			_, err := db.ExecContext(ctx,
				`INSERT INTO templates (tenant_id, name, channel, subject_template, body_template, version, is_active)
				 VALUES ($1, $2, $3, $4, $5, 1, true)
				 ON CONFLICT (name) DO UPDATE
				   SET subject_template = EXCLUDED.subject_template,
				       body_template    = EXCLUDED.body_template,
				       updated_at       = NOW()`,
				tenantID, t.Name, string(t.Channel), subj, t.BodyTemplate,
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "warn: upsert template %q: %v\n", t.Name, err)
				continue
			}
			tmplCount++
		}
	}

	fmt.Printf("\nSeeded successfully\n")
	fmt.Printf("  Tenant ID:  %s\n", tenantID)
	fmt.Printf("  Tenant:     %s\n", *tenantName)
	fmt.Printf("  Client ID:  %s\n", clientID)
	fmt.Printf("  Client:     %s\n", *name)
	fmt.Printf("  API Key:    %s\n", *rawKey)
	fmt.Printf("  Templates:  %d seeded\n\n", tmplCount)
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
