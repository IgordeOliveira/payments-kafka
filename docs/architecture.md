# Architecture and shared conventions

This document is the single source of truth for everything that more than one
component depends on: topic names, the event contract, the outbox table, ports,
environment variables and the repository layout. Every OpenSpec change in
`openspec/changes/` builds on it. If a change needs to alter something here,
update this file in the same pull request.

## Overview

A simulator produces the lifecycle of Pix payments for many restaurants
(merchants). Three independent services consume the events: antifraud,
notification and reconciliation. Everything runs locally with Docker Compose.
Pix is simulated; there is no real bank, PSP or Pix API.

```
                    normal mode                      chaos mode
  simulator ──HTTP──► payment-api                simulator ──────────────┐
                        │ one transaction                                │
                        ▼                                                │
              Postgres: payments + outbox                                │
                        │ WAL (logical replication)                      │
                        ▼                                                ▼
              Kafka Connect (Debezium) ───────────────────► pix.payments.v1
                                                                         │
                 ┌───────────────────────┬───────────────────────────────┤
                 ▼                       ▼                               ▼
             antifraud              notification                  reconciliation
                 │                       ▲                         (Postgres)
                 └─► pix.fraud-alerts.v1 ┘

  any consumer failure ─► pix.payments.retry.v1 ─(attempts exhausted)─► pix.payments.dlq.v1
```

Two producers (Debezium for the outbox path, the simulator in chaos mode), four
topics, three consumer groups. No consumer knows about the others; the event
contract is the only coupling.

## Resolved decisions

| Open question | Decision | Why |
| --- | --- | --- |
| Java or Kotlin for Spring Boot | **Java 21**, Spring Boot 3.x, Maven | Most common pairing; keeps focus on Kafka |
| Node Kafka client | **`@confluentinc/kafka-javascript`** | Maintained, librdkafka-based; KafkaJS is no longer maintained |
| Contract format | **JSON Schema (draft 2020-12)** | Schema Registry and Avro are out of scope |
| Debezium output | **Outbox event router SMT** | Publishes the clean envelope, keyed by `merchant_id`, instead of raw CDC rows |
| Antifraud state | **In memory**, rebuilt by replay | Simplest; makes rebalance and replay effects observable |

Language rule: all code, identifiers, comments, commit messages, documentation
and READMEs are written in **English**.

## Repository layout

```
contracts/                     JSON Schemas and example events (the contract)
docs/architecture.md           this file
docker-compose.yml             full local stack                    (add-local-environment)
infra/                         Postgres init, Kafka Connect config, topic and connector init
services/payment-api/          Spring Boot API with outbox         (add-payment-api-outbox)
services/simulator/            Spring Boot simulator               (add-payment-simulator)
packages/consumer-runtime/     shared TypeScript consumer library  (add-consumer-runtime)
services/antifraud/            TypeScript consumer                 (add-antifraud-consumer)
services/notification/         TypeScript consumer                 (add-notification-consumer)
services/reconciliation/       TypeScript consumer                 (add-reconciliation-consumer)
experiments/                   failure experiments, scripts, docs  (add-validation-experiments)
package.json, tsconfig.base.json   npm workspaces root             (add-consumer-runtime)
```

Each change owns its directory and must not edit files owned by another change.
Per-component documentation goes in the component's own `README.md`; the root
`README.md` is finalized by `add-validation-experiments`.

## Topics

Single broker (KRaft mode), replication factor 1.

| Topic | Partitions | Key | Value | Producers | Consumers |
| --- | --- | --- | --- | --- | --- |
| `pix.payments.v1` | 6 | `merchant_id` | payment event | Debezium, simulator (chaos) | antifraud, notification, reconciliation |
| `pix.fraud-alerts.v1` | 6 | `merchant_id` | fraud alert | antifraud | notification |
| `pix.payments.retry.v1` | 6 | original key | original value | any consumer | any consumer (own records only) |
| `pix.payments.dlq.v1` | 1 | original key | original value | any consumer | none (inspected manually) |

Keys are plain UTF-8 strings. Values are UTF-8 JSON without a schema envelope.
Keying by `merchant_id` guarantees per-merchant ordering on the main topic.

Consumer group ids: `antifraud`, `notification`, `reconciliation`.

## Event contract

Schemas live in `contracts/`:

- `contracts/payment-event.v1.schema.json` — value of `pix.payments.v1`
- `contracts/fraud-alert.v1.schema.json` — value of `pix.fraud-alerts.v1`
- `contracts/examples/` — valid and invalid samples used by tests

### Payment event

```json
{
  "event_id": "5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11",
  "event_type": "payment.confirmed",
  "schema_version": 1,
  "occurred_at": "2026-11-11T14:03:27.512Z",
  "payment_id": "0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42",
  "merchant_id": "merchant-007",
  "sequence": 2,
  "amount_cents": 4590,
  "currency": "BRL"
}
```

Lifecycle and `sequence` (per payment, strictly increasing):

| `event_type` | `sequence` | Allowed from |
| --- | --- | --- |
| `payment.created` | 1 | (start) |
| `payment.confirmed` | 2 | created |
| `payment.expired` | 2 | created |
| `payment.refunded` | 3 | confirmed |

Refunds are always for the full `amount_cents`. `event_id` is unique per event
and is the idempotency key for every consumer. Consumers must ignore unknown
fields (forward compatibility) and must rely on the payload, never on headers.

### Fraud alert

```json
{
  "alert_id": "amount_outlier:5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11",
  "schema_version": 1,
  "rule": "amount_outlier",
  "merchant_id": "merchant-007",
  "payment_id": "0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42",
  "triggered_by_event_id": "5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11",
  "occurred_at": "2026-11-11T14:03:27.512Z",
  "details": { "amount_cents": 990000, "merchant_mean_cents": 5210 }
}
```

`alert_id` is deterministic: `<rule>:<triggered_by_event_id>`. Reprocessing the
same event produces the same `alert_id`, so downstream consumers can dedupe.

## Retry and DLQ protocol

Implemented once in `packages/consumer-runtime` and used by all consumers.

1. Offsets are committed manually, only after a record is fully handled
   (processed, or handed off to retry/DLQ). Delivery is at-least-once.
2. A record that fails validation (not JSON, or violates the schema) is
   **non-retryable** and goes straight to the DLQ.
3. A record whose handler throws is published to `pix.payments.retry.v1` with
   the original key and value plus the headers below, then its offset is
   committed.
4. Every consumer also subscribes to the retry topic with its own group id. It
   skips (and commits) records whose `x-retry-group` is not its own group. For
   its own records it waits until `x-retry-not-before`, then runs the handler
   again.
5. Backoff is exponential: `RETRY_BACKOFF_MS * 2^(attempt-1)`. After
   `RETRY_MAX_ATTEMPTS` failed retries the record goes to the DLQ.

| Header | Topic | Meaning |
| --- | --- | --- |
| `x-retry-group` | retry | consumer group that owns the retry |
| `x-retry-attempt` | retry | retry number, starting at `1` |
| `x-retry-not-before` | retry | epoch milliseconds before which it must not run |
| `x-original-topic` / `x-original-partition` / `x-original-offset` | retry, DLQ | where the record first came from |
| `x-error-message` | retry, DLQ | last error message (truncated) |
| `x-dlq-group` | DLQ | consumer group that gave up |
| `x-dlq-reason` | DLQ | `invalid_message` or `retries_exhausted` |
| `x-dlq-attempts` | DLQ | retries performed before giving up |

Retrying breaks per-merchant ordering for the retried record. Handlers must
therefore be idempotent and tolerate reordering using `sequence`.

## Postgres

One Postgres instance with logical replication enabled (`wal_level=logical`).
User `pix`, password `pix`. The init script in `infra/` creates the databases;
each service owns the tables in its database through its own migrations.

| Database | Owner | Contents |
| --- | --- | --- |
| `payments` | payment-api | `payments`, `outbox` |
| `reconciliation` | reconciliation | payment state, merchant balances, ledger, processed events |
| `notification` | notification | sent-notification dedupe keys |

### Outbox table (contract between payment-api and Debezium)

```sql
CREATE TABLE outbox (
    id             UUID         PRIMARY KEY,           -- equals the event_id
    aggregate_type VARCHAR(64)  NOT NULL,              -- always 'payment'
    aggregate_id   VARCHAR(64)  NOT NULL,              -- payment_id
    merchant_id    VARCHAR(64)  NOT NULL,              -- becomes the Kafka key
    event_type     VARCHAR(64)  NOT NULL,              -- e.g. 'payment.created'
    payload        JSONB        NOT NULL,              -- the full payment event
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

The Debezium connector watches `public.outbox` in database `payments` and uses
the outbox event router so that each inserted row becomes one record on
`pix.payments.v1` with key `merchant_id` and a value equal to `payload` (the
bare JSON object, no CDC envelope, no schema wrapper). Rows are never updated.

## Payment API (HTTP contract)

Base URL `http://localhost:8081` (`http://payment-api:8081` inside Compose).

| Method and path | Body | Success | Errors |
| --- | --- | --- | --- |
| `POST /payments` | `{"merchant_id": "merchant-007", "amount_cents": 4590}` | `201` payment | `400` invalid body |
| `POST /payments/{id}/confirm` | none | `200` payment | `404`, `409` invalid transition |
| `POST /payments/{id}/expire` | none | `200` payment | `404`, `409` |
| `POST /payments/{id}/refund` | none | `200` payment | `404`, `409` |
| `GET /payments/{id}` | none | `200` payment | `404` |
| `GET /actuator/health` | none | `200` | |

Payment representation:

```json
{
  "payment_id": "0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42",
  "merchant_id": "merchant-007",
  "amount_cents": 4590,
  "currency": "BRL",
  "status": "CONFIRMED",
  "sequence": 2,
  "created_at": "2026-11-11T14:03:20.100Z",
  "updated_at": "2026-11-11T14:03:27.512Z"
}
```

`status` is one of `CREATED`, `CONFIRMED`, `EXPIRED`, `REFUNDED`.

## Ports and hostnames

| Component | Compose service | Host port | In-network address |
| --- | --- | --- | --- |
| Kafka | `kafka` | 9092 | `kafka:29092` |
| Kafka UI | `kafka-ui` | 8080 | |
| Kafka Connect | `kafka-connect` | 8083 | `kafka-connect:8083` |
| Postgres | `postgres` | 5432 | `postgres:5432` |
| Payment API | `payment-api` | 8081 | `payment-api:8081` |
| Simulator | `simulator` | 8082 | |
| Consumers | `antifraud`, `notification`, `reconciliation` | none | scalable with `--scale` |

## Docker Compose conventions

- `docker compose up -d` starts infrastructure only: `kafka`, `kafka-init`
  (creates topics), `kafka-ui`, `postgres`, `kafka-connect`.
- Application services are in the `apps` profile:
  `docker compose --profile apps up -d --build`. `connector-init` registers the
  Debezium connector once `payment-api` is healthy (its migrations create the
  outbox table).
- Java services: build context is the service directory
  (`services/payment-api`, `services/simulator`), with a `Dockerfile` in it.
- TypeScript services: build context is the **repository root** (npm
  workspaces) with `dockerfile: services/<name>/Dockerfile`.
- Consumers declare no `container_name` and no host ports so they can be scaled:
  `docker compose --profile apps up -d --scale reconciliation=3`.

## Environment variables

| Variable | Used by | Default (local run) | Value in Compose |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | simulator, consumers | `localhost:9092` | `kafka:29092` |
| `KAFKA_GROUP_ID` | consumers | service name | service name |
| `DATABASE_URL` | reconciliation, notification | `postgres://pix:pix@localhost:5432/<db>` | `postgres://pix:pix@postgres:5432/<db>` |
| `SPRING_DATASOURCE_URL` / `_USERNAME` / `_PASSWORD` | payment-api | `jdbc:postgresql://localhost:5432/payments`, `pix`, `pix` | host `postgres` |
| `SERVER_PORT` | payment-api, simulator | `8081`, `8082` | same |
| `PAYMENT_API_BASE_URL` | simulator | `http://localhost:8081` | `http://payment-api:8081` |
| `RETRY_MAX_ATTEMPTS` | consumers | `4` | `4` |
| `RETRY_BACKOFF_MS` | consumers | `1000` | `1000` |
| `LOG_LEVEL` | consumers | `info` | `info` |

Simulator-specific variables (`SIMULATOR_*`) are defined in the
`add-payment-simulator` change; antifraud thresholds (`ANTIFRAUD_*`) in
`add-antifraud-consumer`.

## Testing conventions

- The fixed host ports above belong to the Compose stack. Automated tests must
  not depend on it: use **Testcontainers** (random ports) for Kafka and
  Postgres, in both Java and TypeScript.
- Java: JUnit 5, `mvn verify` runs unit and integration tests.
- TypeScript: Vitest, `npm test` at the root runs every workspace.
- Consumers log structured JSON lines to stdout.

## Out of scope

Authentication, frontend, cloud deployment, Schema Registry and Avro, Kafka
Streams / ksqlDB, cluster security (TLS, SASL). A consumer feeding a RAG index
is a possible later extension and has no change yet.
