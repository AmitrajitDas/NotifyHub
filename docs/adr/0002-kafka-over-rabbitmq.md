# ADR 0002: Kafka for Notification Queues

## Status

Accepted

## Context

NotifyHub needs durable channel-specific queues, per-recipient ordering, replayable messages during failures, worker fan-out, dead-letter routing, and a path to high-throughput delivery workloads. Messages are naturally append-only notification events.

## Decision

Use Apache Kafka in KRaft mode as the message queue, accessed through `segmentio/kafka-go`.

## Consequences

- Topic partitioning lets NotifyHub preserve ordering by keying messages on `recipient_id`.
- Consumer groups allow each channel worker pool to scale independently.
- Retention and replay semantics support DLQ recovery and operational debugging.
- Kafka is heavier to run than RabbitMQ for small deployments, so local Compose and deployment docs must make broker bootstrap and topic health explicit.
