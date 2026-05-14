# ADR 0004: sqlc for Type-Safe Queries

## Status

Accepted

## Context

NotifyHub should keep SQL explicit while still catching schema and query mistakes before runtime. Generated database code must be isolated from domain and service logic so migrations and query changes remain controlled.

## Decision

Use `sqlc` to generate typed Go query methods from SQL files in `queries/`. Generated files live in `internal/db/` and are not edited manually.

## Consequences

- Query parameters and scanned columns are checked at generation time.
- Repository wrappers can keep all database-to-domain conversion in one layer.
- Schema changes follow a repeatable path: edit migration, edit query, run `make generate`, update repository code.
- Generated files add churn to diffs, but the stronger query contract is worth it for a multi-tenant system.
