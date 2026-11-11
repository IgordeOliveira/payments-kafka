# Design

## Context

Service names, ports, topics, databases, the outbox DDL and the Compose
conventions are fixed in `docs/architecture.md`. This change is built in
parallel with the application services, so their Dockerfiles may not exist yet
when it is implemented.

## Goals / Non-Goals

**Goals:**
- `docker compose up -d` gives a healthy infrastructure with topics and databases ready.
- `docker compose --profile apps up -d --build` runs the whole pipeline.
- Works on Windows, macOS and Linux with Docker Desktop.

**Non-Goals:**
- Multi-broker cluster, TLS, SASL, Schema Registry.
- Building application images (owned by each service change).

## Decisions

- **Single Kafka broker in KRaft combined mode** (official `apache/kafka`
  image), two listeners: `kafka:29092` in-network and `localhost:9092` for the
  host. One broker makes "kill Kafka" experiments unambiguous.
- **Topics created by a one-shot `kafka-init` service**, with
  `auto.create.topics.enable=false` on the broker, so partition counts are
  explicit and a typo in a topic name fails loudly.
- **Postgres started with `wal_level=logical`**; an init script in
  `infra/postgres/` creates databases `payments`, `reconciliation` and
  `notification`. Tables are owned by the services, not by this change.
- **Debezium connector as a JSON file in `infra/connect/`**, registered by a
  one-shot `connector-init` service (profile `apps`) that waits for
  `payment-api` to be healthy and `PUT`s the config to
  `/connectors/<name>/config` (idempotent). Outbox event router SMT with
  `table.field.event.key=merchant_id`, fixed route to `pix.payments.v1`,
  `table.expand.json.payload=true`, string key converter and JSON value
  converter with `schemas.enable=false`, so the record value is the bare event.
- **Pin every image to an explicit version tag**; no `latest`.
- **Application services under profile `apps`** with healthchecks and
  `depends_on` conditions; consumers have no `container_name` or host ports.
- **Shell scripts are POSIX `sh` and run inside containers**, so nothing
  depends on the host shell. `.gitattributes` forces LF line endings.

## Risks / Trade-offs

- [The connector cannot be verified end to end before payment-api exists] →
  verify with a manually created `outbox` table using the DDL from
  `docs/architecture.md` and a manual `INSERT`.
- [Debezium replication slot keeps WAL if Connect stays down] → acceptable
  locally; `docker compose down -v` resets everything. Document it.
- [`apps` services fail to build until their changes merge] → they are behind
  a profile, so the default `docker compose up -d` is unaffected.
