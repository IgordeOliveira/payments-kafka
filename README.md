# payments-kafka

A study project: an event pipeline for simulated Pix payments on Apache Kafka.

A simulator publishes Pix payment events for many restaurants to a Kafka topic.
Three independent services consume them: antifraud, notification and
reconciliation. The goal is to provoke, and then solve, the real problems of an
event-driven payment system:

- Partitioning by key and per-merchant ordering
- Consumer groups, offsets, lag and rebalancing
- At-least-once delivery and idempotent consumers
- Retry with backoff and a dead letter queue (DLQ)
- The outbox pattern, so no event is lost between the database and Kafka
- Reprocessing history by resetting offsets

Everything runs locally with Docker Compose. Pix is simulated: there is no real
bank, PSP or Pix API.

## Stack

- **Producer and API:** Java 21, Spring Boot, Spring Kafka
- **Consumers:** Node.js, TypeScript, `@confluentinc/kafka-javascript`
- **Outbox via CDC:** Kafka Connect with the Debezium Postgres connector
- **Local environment:** Docker Compose with Kafka (KRaft), Kafka Connect, Kafka UI and Postgres

## Documentation

- [docs/architecture.md](docs/architecture.md) — architecture and shared conventions
- [contracts/](contracts/) — JSON Schema event contracts
- [openspec/](openspec/) — specs and change proposals ([OpenSpec](https://github.com/Fission-AI/OpenSpec))

## Status

Work in progress. The project is built spec-first: each component is an
OpenSpec change under `openspec/changes/`. Run `openspec list` to see progress.
Setup and usage instructions are added as the components land.
