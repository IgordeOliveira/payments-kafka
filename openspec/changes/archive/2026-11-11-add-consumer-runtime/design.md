# Design

## Context

The retry and DLQ protocol, headers, topics and environment variables are
fixed in `docs/architecture.md`. Three services (`antifraud`, `notification`,
`reconciliation`) will be written in parallel against this package as soon as
it merges, so its public API must be small, documented and stable.

## Goals / Non-Goals

**Goals:**
- A service is a handler function plus configuration; everything about
  commits, retries and the DLQ is inherited.
- At-least-once delivery that is demonstrably correct when a consumer is
  killed mid-processing.

**Non-Goals:**
- Exactly-once / Kafka transactions.
- Idempotency storage: each service owns its own dedupe store, because the
  dedupe write must share a transaction with that service's side effect.

## Decisions

- **npm workspaces at the repository root**: `packages/*` and
  `services/antifraud`, `services/notification`, `services/reconciliation`.
  Node.js 22, TypeScript strict, ES modules, Vitest. The root `npm test` and
  `npm run build` run every workspace.
- **`@confluentinc/kafka-javascript`** (KafkaJS-compatible API) with
  `enable.auto.commit=false`. Offsets are committed explicitly after the record
  is handled or handed off.
- **Public API** (illustrative; names may be refined, shape may not):

  ```ts
  const runtime = createConsumerRuntime({
    groupId: 'reconciliation',
    topics: [{ topic: 'pix.payments.v1', schema: 'payment-event' }],
    handler: async (message, ctx) => { /* throw to retry */ },
    config: loadConfig(),          // env vars from docs/architecture.md
  });
  await runtime.start();           // resolves when running
  await runtime.stop();            // graceful
  ```

  `message` carries the validated, typed value plus `topic`, `partition`,
  `offset`, `key`, `headers` and `attempt` (0 for a first delivery). The
  handler signals a retryable failure by throwing; throwing
  `NonRetryableError` sends the record straight to the DLQ.
- **Per-partition sequential processing, partitions concurrent.** Order within
  a partition is the guarantee Kafka gives; processing a partition's records
  one at a time preserves it and makes "commit offset N+1 after record N" safe.
- **Retry via a single shared topic with an owner header** rather than one
  retry topic per group: fewer topics to reason about, and the skip logic makes
  the ownership model visible. A consumer subscribes to its source topics plus
  `pix.payments.retry.v1`; foreign records are skipped and committed.
- **Delay by pausing the retry partition** until `x-retry-not-before`, instead
  of sleeping in the handler: sleeping would exceed `max.poll.interval.ms` and
  trigger a rebalance. A due record blocks later records in its partition,
  which is acceptable for a retry topic.
- **Hand-off before commit.** The produce to retry or DLQ is awaited
  (`acks=all`) before the source offset is committed. A crash in between
  yields a duplicate, never a loss: that is the at-least-once contract, and it
  is why handlers must be idempotent.
- **Validation with Ajv 2020 + `ajv-formats`**, schemas loaded from
  `contracts/` at startup. A record that is not JSON or fails its schema is
  non-retryable.
- **Graceful shutdown** on `SIGTERM`/`SIGINT`: stop fetching, finish in-flight
  records, commit, leave the group. Rebalance events (assign/revoke) are logged
  with the partitions involved so they can be observed in experiments.
- **Structured JSON logs** (pino) with `group`, `topic`, `partition`, `offset`,
  `event_id`.
- **Test helpers exported from the package** (`/testing` entry point): start
  Kafka in Testcontainers, create the topics, produce and collect records.
  Consumer services reuse them instead of rebuilding the harness.
- **Docker base for services**: document the Dockerfile pattern (root build
  context, `npm ci`, build the workspace, Debian-slim runtime because
  librdkafka needs glibc) in the package README so each service copies it.

## Risks / Trade-offs

- [A slow due record delays later retries in the same partition] → 6
  partitions and short backoff keep this small; documented.
- [Retries reorder events for a merchant] → documented protocol property;
  handlers use `sequence` and idempotency.
- [Shared retry topic means every group reads every retry record] → negligible
  at this scale and useful for observing how skipping affects lag.
- [API churn would break three dependent services] → the README documents the
  public API and it is covered by an integration test acting as a sample
  service.
