# ADR 0001: Go for NotifyHub Services

## Status

Accepted

## Context

NotifyHub is a latency-sensitive notification platform with long-running API and worker processes, Kafka consumers, PostgreSQL access, Redis rate limiting, provider SDK calls, and explicit graceful shutdown behavior. The system benefits from predictable concurrency, small container images, static binaries, and low operational overhead.

## Decision

Use Go as the implementation language for the API, workers, providers, repositories, and shared domain logic.

## Consequences

- Goroutines and contexts map cleanly to concurrent worker pools, Kafka fetch loops, provider retries, and shutdown draining.
- Static binaries keep Docker images small and reduce runtime dependency drift.
- Standard library HTTP, logging, templates, and signal handling cover much of the platform without extra framework weight.
- The team gives up some of Node.js's package ecosystem velocity, but gains simpler runtime operations and strong compile-time checks for a backend-heavy system.
