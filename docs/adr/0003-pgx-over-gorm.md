# ADR 0003: pgx over GORM

## Status

Accepted

## Context

NotifyHub is multi-tenant and every query must be explicitly scoped by `tenant_id`. The data access layer needs predictable SQL, clear transaction behavior, PostgreSQL-native types, and easy integration with migrations and generated query methods.

## Decision

Use `jackc/pgx/v5` for PostgreSQL access and avoid ORMs.

## Consequences

- SQL remains explicit and reviewable, which lowers the risk of tenant isolation bugs.
- `pgx` gives direct PostgreSQL behavior without ORM mapping surprises.
- Repository wrappers own conversion between generated DB models and domain types.
- Engineers must write SQL directly, but this is intentional for correctness and performance-critical paths.
