# @payments-kafka/consumer-runtime

Shared runtime for the TypeScript consumers (`antifraud`, `notification`,
`reconciliation`). A service supplies a handler and its configuration; the
runtime does everything else the same way for every consumer:

- consumes with **manual offset commits** after each record is handled
  (at-least-once, never auto-commit);
- **validates** each value against its JSON Schema in `contracts/` before the
  handler sees it;
- **retries** failed records through `pix.payments.retry.v1` with exponential
  backoff, and **dead-letters** them to `pix.payments.dlq.v1`;
- handles records of a partition **one at a time, in order**, and partitions
  in parallel;
- **logs rebalances**, exposes assign/revoke hooks, and **shuts down
  gracefully** on `SIGTERM`/`SIGINT`;
- provides a JSON logger, config loading and **test helpers** (Kafka in
  Testcontainers).

Shared conventions (topics, headers, environment variables) are defined in
[`docs/architecture.md`](../../docs/architecture.md); this README documents how
the package implements them and how to use it.

Contents: [Sample service](#sample-service) ·
[API](#api) · [Errors](#errors-retryable-or-not) ·
[Configuration](#configuration) · [Delivery guarantees](#delivery-guarantees-and-handler-rules) ·
[Retry and DLQ flow](#retry-and-dlq-flow) · [Rebalances](#rebalances-and-partition-hooks) ·
[Shutdown](#shutdown) · [Logging](#logging) · [Test helpers](#test-helpers) ·
[Service package setup](#service-package-setup) · [Dockerfile pattern](#dockerfile-pattern) ·
[Developing this package](#developing-this-package)

## Sample service

A complete service. It is compiled against the package's published types and
run against Kafka by this package's test suite (`examples/sample-service.ts`,
`test/integration/sample-service.test.ts`), so it is known to work as written.

<!-- sample-service:start (copy of examples/sample-service.ts, checked by test/unit/readme-sample.test.ts) -->
```ts
/**
 * A complete service built on @payments-kafka/consumer-runtime.
 * (Illustrative only: its "large payment" rule is not the antifraud service.)
 *
 * It consumes two topics with different contracts through one handler,
 * publishes records of its own, rejects some records as non-retryable and
 * reacts to partition assignment changes. Commits, validation, retries, the
 * DLQ and graceful shutdown all come from the runtime.
 */
import {
  createConsumerRuntime,
  createLogger,
  envInteger,
  loadConfig,
  NonRetryableError,
  TOPICS,
  type ConsumedMessage,
  type FraudAlert,
  type HandlerContext,
} from '@payments-kafka/consumer-runtime';

const SERVICE_NAME = 'sample';

// Service-specific settings use the same env helpers as loadConfig():
// unset means default, an invalid value fails at startup naming the variable.
const LARGE_PAYMENT_CENTS = envInteger(process.env, 'SAMPLE_LARGE_PAYMENT_CENTS', 500_000, { min: 1 });

/** In-memory state, kept only for the partitions this instance owns. */
const paymentsSeenByMerchant = new Map<string, number>();

type Message = ConsumedMessage<'payment-event' | 'fraud-alert'>;

async function handle(message: Message, ctx: HandlerContext): Promise<void> {
  // `schema` tells the two contracts apart and narrows `message.value`.
  if (message.schema === 'fraud-alert') {
    ctx.logger.info({ rule: message.value.rule }, 'fraud alert received');
    return;
  }

  const event = message.value; // PaymentEvent, already validated
  if (event.event_type === 'payment.refunded' && event.amount_cents > 100_000_000) {
    // Retrying cannot fix this: straight to the DLQ (x-dlq-reason: non_retryable).
    throw new NonRetryableError(`refund of ${event.amount_cents} cents is above the sample limit`);
  }

  paymentsSeenByMerchant.set(
    event.merchant_id,
    (paymentsSeenByMerchant.get(event.merchant_id) ?? 0) + 1,
  );

  if (event.event_type === 'payment.created' && event.amount_cents >= LARGE_PAYMENT_CENTS) {
    const rule = 'amount_outlier';
    const alert: FraudAlert = {
      alert_id: `${rule}:${event.event_id}`, // deterministic, so redelivery is harmless
      schema_version: 1,
      rule,
      merchant_id: event.merchant_id,
      payment_id: event.payment_id,
      triggered_by_event_id: event.event_id,
      occurred_at: event.occurred_at,
      details: { amount_cents: event.amount_cents, threshold_cents: LARGE_PAYMENT_CENTS },
    };
    // Await the acknowledgement before returning: the offset is committed
    // right after the handler returns. Throwing here (e.g. broker down)
    // sends the event through the retry path instead.
    await ctx.producer.send({ topic: TOPICS.fraudAlerts, key: alert.merchant_id, value: alert });
    ctx.logger.info({ alert_id: alert.alert_id }, 'fraud alert published');
  }

  ctx.logger.info({ event_type: event.event_type }, 'payment event handled');
}

async function main(): Promise<void> {
  const config = loadConfig({ serviceName: SERVICE_NAME });
  const logger = createLogger({ level: config.logLevel, bindings: { service: SERVICE_NAME } });

  const runtime = createConsumerRuntime({
    config,
    logger,
    topics: [
      { topic: TOPICS.payments, schema: 'payment-event' },
      { topic: TOPICS.fraudAlerts, schema: 'fraud-alert' },
    ],
    handler: handle,
    // Called after the in-flight handlers of those partitions finished. The
    // state is keyed by merchant, not by partition, so drop all of it and
    // rebuild from the records this instance receives next.
    onPartitionsRevoked: () => paymentsSeenByMerchant.clear(),
  });

  await runtime.start();
  // Resolves after SIGTERM/SIGINT: in-flight records were finished and
  // committed, and the instance left the group.
  await runtime.stopped;
  // Close the service's own resources here (database pool...).
}

main().catch((error: unknown) => {
  // ConfigError and startup failures end up here.
  createLogger().fatal({ err: error instanceof Error ? error.message : String(error) }, 'startup failed');
  process.exit(1);
});
```
<!-- sample-service:end -->

The minimal shape is just:

```ts
const runtime = createConsumerRuntime({
  config: loadConfig({ serviceName: 'reconciliation' }),
  topics: [{ topic: TOPICS.payments, schema: 'payment-event' }],
  handler: async (message, ctx) => {
    /* message.value is a validated PaymentEvent; throw to retry */
  },
});
await runtime.start();
await runtime.stopped;
```

## API

Everything below is exported from `@payments-kafka/consumer-runtime`.

### `createConsumerRuntime(options): ConsumerRuntime`

One runtime = one consumer group member. It subscribes to `options.topics`
**plus the retry topic** (automatically; do not list it).

| Option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `config` | `RuntimeConfig` | required | Usually `loadConfig({ serviceName })`. |
| `topics` | `{ topic, schema }[]` | required | Source topics and the contract of each: `'payment-event'` or `'fraud-alert'`. Write them inline (or `as const`) so the handler gets precise types. |
| `handler` | `(message, ctx) => unknown` | required | Sync or async; whatever it returns is awaited. See [handler](#the-handler). |
| `logger` | `Logger` | `createLogger({ level: config.logLevel })` | Every line gets `group`. Pass your own to add `service`. |
| `onPartitionsAssigned` | `(partitions) => unknown` | none | After partitions are assigned, before any of their records is handled. May be async (awaited). |
| `onPartitionsRevoked` | `(partitions, { lost }) => unknown` | none | When partitions are taken away, **after** the handler calls running for them have settled. May be async (awaited). |
| `handleSignals` | `boolean` | `true` | Graceful stop on `SIGTERM`/`SIGINT`. Tests pass `false`. |
| `retryTopic`, `dlqTopic` | `string` | `pix.payments.retry.v1`, `pix.payments.dlq.v1` | Override only in tests (isolated topics). |
| `contractsDir` | `string` | `<repo>/contracts` | Where the JSON Schemas are read from at startup. |
| `partitionsConsumedConcurrently` | `number` | `12` | Max partitions handled in parallel (still one record at a time per partition). |
| `kafka` | `{ consumer?, producer? }` | none | librdkafka property overrides, e.g. `{ consumer: { 'session.timeout.ms': 10000 } }`. |

`ConsumerRuntime`:

| Member | Meaning |
| --- | --- |
| `start(): Promise<void>` | Connects the producer and the consumer, subscribes and starts consuming. Resolves once running (partition assignment follows asynchronously). Rejects, after cleaning up, if Kafka is unreachable. |
| `stop(): Promise<void>` | Graceful stop (see [Shutdown](#shutdown)). Idempotent. |
| `stopped: Promise<void>` | Resolves once stopped, whether by `stop()` or by a signal. `await` it in `main()`, then close your own resources. |
| `producer: RecordProducer` | The runtime's producer (also `ctx.producer`). Connected between `start()` and `stop()`. |
| `logger: Logger` | The runtime's logger (with `group`). |
| `assignment(): TopicPartition[]` | Partitions currently owned by this instance. |

### The handler

```ts
type MessageHandler<S> = (message: ConsumedMessage<S>, ctx: HandlerContext) => unknown; // sync or async, awaited
```

`message` (`ConsumedMessage<S>`):

| Field | Meaning |
| --- | --- |
| `schema` | `'payment-event'` or `'fraud-alert'`. Discriminates the union: `if (message.schema === 'fraud-alert')` narrows `message.value` to `FraudAlert`. |
| `value` | The parsed value, **already validated** against the contract (`PaymentEvent` or `FraudAlert`). Unknown extra fields are kept and should be ignored. |
| `topic`, `partition`, `offset` | Where the record was **first published**. For a retry these come from the `x-original-*` headers, so dispatching on `message.topic` works the same for retries. `offset` is a string. |
| `key` | Record key as a string (`merchant_id`), or `null`. |
| `headers` | Record headers as strings (on a retry they include the `x-retry-*` headers). Contracts say: rely on the payload, never on headers. |
| `attempt` | `0` on first delivery, then the retry number (`1..RETRY_MAX_ATTEMPTS`). |
| `timestamp` | Record timestamp in epoch milliseconds, as a string. |
| `record` | `{ topic, partition, offset }` of the Kafka record actually being consumed (the retry topic when `attempt > 0`). |

`ctx` (`HandlerContext`):

| Field | Meaning |
| --- | --- |
| `logger` | Child logger carrying `group`, `topic`, `partition`, `offset`, `key`, `attempt` and `event_id` / `alert_id`. Log through it. |
| `producer` | `RecordProducer` (idempotent, `acks=all`). `await ctx.producer.send({ topic, key, value, headers? })`; `value` may be an object (JSON-encoded), a string or a Buffer. `sendMany(records)` for several. |
| `groupId` | The consumer group id. |

Return normally when the record is fully handled; the offset is committed
right after. Throw to retry (see [Errors](#errors-retryable-or-not)).

### Other exports

| Export | Purpose |
| --- | --- |
| `loadConfig`, `RuntimeConfig`, `envString`, `envInteger`, `envNumber`, `envEnum`, `envBootstrapServers` | [Configuration](#configuration). |
| `createLogger`, `Logger`, `LogLevel`, `LOG_LEVELS`, `recordLogFields` | [Logging](#logging). |
| `NonRetryableError`, `isNonRetryableError`, `ConfigError` | [Errors](#errors-retryable-or-not). |
| `createProducer({ bootstrapServers, logger?, clientId?, kafkaConfig? })` | A standalone `ManagedProducer` (`connect`, `send`, `sendMany`, `disconnect`) with the same settings as the runtime's. Only needed outside a runtime. |
| `loadContracts({ contractsDir? })` → `{ validate(schema, value), decode(schema, buffer) }` | The validator the runtime uses (Ajv, draft 2020-12, formats asserted). Useful to validate what a service *produces* (e.g. antifraud alerts) in its tests. |
| `PaymentEvent`, `PaymentEventType`, `FraudAlert`, `FraudRule`, `SchemaName`, `SchemaTypes` | TypeScript types of the contracts. |
| `TOPICS` | `{ payments, fraudAlerts, retry, dlq }` topic names. |
| `HEADERS`, `DlqReason`, `backoffDelayMs`, `MAX_ERROR_MESSAGE_LENGTH` | Protocol constants. |
| `LibrdkafkaConfig` | Type of the `kafka.consumer` / `kafka.producer` overrides. |

## Errors: retryable or not

| What happens | Outcome |
| --- | --- |
| Value is not JSON, or violates its schema | Handler **not called**. Record goes to the DLQ with `x-dlq-reason: invalid_message`. |
| Handler throws `NonRetryableError` | Straight to the DLQ with `x-dlq-reason: non_retryable`; never on the retry topic. Use it when retrying cannot help (e.g. reconciliation's "conflicting event for a step already taken"). |
| Handler throws anything else | **Retryable**: published to the retry topic, handled again after the backoff. After `RETRY_MAX_ATTEMPTS` failed retries: DLQ with `x-dlq-reason: retries_exhausted`. Database down, broker hiccup, "predecessor event not arrived yet" are all retryable. |
| `ConfigError` (from `loadConfig` / `env*`) | Thrown at startup, `message` names the variable, e.g. `Invalid RETRY_MAX_ATTEMPTS: expected a non-negative integer, got "-1"`; `error.variable` holds the name. Let it crash the process. |

`NonRetryableError` accepts `{ cause }`: `throw new NonRetryableError('conflict', { cause: err })`.

## Configuration

`loadConfig({ serviceName, env? })` reads `process.env` (or `env`) and returns
a `RuntimeConfig`. Variables and defaults, per `docs/architecture.md`:

| Variable | Field | Default | Valid values |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafkaBootstrapServers` | `localhost:9092` | comma-separated `host:port` (Compose: `kafka:29092`) |
| `KAFKA_GROUP_ID` | `groupId` | `serviceName` | non-empty |
| `RETRY_MAX_ATTEMPTS` | `retryMaxAttempts` | `4` | integer ≥ 0 (`0` = first failure goes to the DLQ) |
| `RETRY_BACKOFF_MS` | `retryBackoffMs` | `1000` | integer ≥ 0 |
| `LOG_LEVEL` | `logLevel` | `info` | `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `silent` (case-insensitive) |

An unset variable takes its default; any set value (including an empty
string) must be valid, otherwise a `ConfigError` is thrown.

For service-specific variables (`DATABASE_URL`, `ANTIFRAUD_*`...) use the same
helpers, which have the same behavior and error messages:

```ts
const env = process.env;
const outlierFactor = envNumber(env, 'ANTIFRAUD_OUTLIER_FACTOR', 5, { min: 1 });
const burstLimit = envInteger(env, 'ANTIFRAUD_BURST_LIMIT', 30, { min: 1 });
const databaseUrl = envString(env, 'DATABASE_URL', 'postgres://pix:pix@localhost:5432/reconciliation');
```

## Delivery guarantees and handler rules

- **At-least-once.** An offset is committed only after the handler returned,
  or after the record was handed off to the retry topic or the DLQ and the
  broker acknowledged that write (`acks=all`). A crash at any point before the
  commit means the record is delivered again (proven by
  `test/integration/redelivery.test.ts`, which kills a consumer process
  mid-handler). **Handlers must be idempotent**; dedupe on `event_id` /
  `alert_id` in the same transaction as your side effect.
- **Order.** Records of one partition reach the handler one at a time, in
  offset order; different partitions run in parallel. Since keys are
  `merchant_id`, a merchant's events are in order on first delivery. A
  **retried record is out of order** with respect to the records after it:
  use `sequence` and idempotency, never arrival order.
- **Await everything** the handler starts (database writes, `ctx.producer.send`)
  before returning. Work left running after the handler returns is not covered
  by the commit.
- **Do not sleep or block for long** in a handler. A handler running longer
  than `max.poll.interval.ms` (default 300 s) gets the member evicted. Delayed
  retries are done by the runtime without blocking (see below).
- **No `eachBatch`, no auto-commit, no transactions.** Exactly-once is out of
  scope: idempotent handlers plus at-least-once give the same observable
  result.

## Retry and DLQ flow

```
pix.payments.v1 / pix.fraud-alerts.v1
   │
   ├─ not JSON / schema violation ──────────────────► DLQ (invalid_message), commit
   ├─ handler OK ───────────────────────────────────► commit
   ├─ handler throws NonRetryableError ─────────────► DLQ (non_retryable), commit
   └─ handler throws ───► retry topic (attempt 1, not-before = now + B), commit
                              │
   pix.payments.retry.v1 ◄────┘   (every group reads it, with its own group id)
   │
   ├─ x-retry-group ≠ my group ─────────────────────► skip, commit
   ├─ now < x-retry-not-before ─────────────────────► pause partition until due, no commit
   └─ due: validate + handler again
        ├─ OK ──────────────────────────────────────► commit
        ├─ NonRetryableError ───────────────────────► DLQ (non_retryable), commit
        ├─ throws, attempt < RETRY_MAX_ATTEMPTS ────► retry topic (attempt+1, B·2^attempt), commit
        └─ throws, attempt = RETRY_MAX_ATTEMPTS ────► DLQ (retries_exhausted), commit
```

`B` is `RETRY_BACKOFF_MS`; retry `n` is due `B · 2^(n-1)` ms after the failure
(defaults: 1 s, 2 s, 4 s, 8 s, then DLQ). Retry and DLQ records keep the
original key and value bytes. Headers, as specified in `docs/architecture.md`:

| Header | On | Value |
| --- | --- | --- |
| `x-retry-group` | retry | consumer group that owns the retry |
| `x-retry-attempt` | retry | `1`, `2`, ... |
| `x-retry-not-before` | retry | epoch milliseconds |
| `x-original-topic`, `x-original-partition`, `x-original-offset` | retry, DLQ | first publication of the record (carried over across retries) |
| `x-error-message` | retry, DLQ | last error message, truncated to 1000 characters |
| `x-dlq-group` | DLQ | group that gave up |
| `x-dlq-reason` | DLQ | `invalid_message`, `non_retryable` or `retries_exhausted` |
| `x-dlq-attempts` | DLQ | retries performed before giving up (`0` if it never went through the retry topic) |

Implementation notes worth knowing:

- **Waiting for a retry does not block the consumer.** A retry record that is
  not yet due makes the runtime pause that retry partition, rewind it to the
  record and resume it at `x-retry-not-before`. The consumer keeps polling, so
  it stays in the group (tested with a 12 s delay and a 4 s poll interval)
  and other partitions keep flowing. A due record blocks the records behind
  it in the same retry partition, which is acceptable for a retry topic.
- **Hand-off before commit.** If the write to the retry topic or the DLQ fails
  (broker down, topic missing), the source offset is **not** committed: the
  partition is paused, rewound and the record is processed again after
  `max(RETRY_BACKOFF_MS, 1000)` ms. Your handler may therefore run again for
  the same record with the same `attempt` (tested in `retry.test.ts`).
- A retry record is validated again against the contract of its
  `x-original-topic`, so a retry can only reach a group that subscribes to
  that topic.
- Foreign retry records are skipped and committed: every group reads the whole
  retry topic, which shows up as lag in Kafka UI and is part of the lesson.

## Rebalances and partition hooks

- Every assignment and revocation is logged at `info`:
  `{"msg":"partitions assigned","partitions":[{"topic":"pix.payments.v1","partition":0},...],"count":12}`
  and `{"msg":"partitions revoked",...,"lost":false}`. The partition list
  includes the retry topic partitions.
- The group uses the client's default **eager** protocol (round robin):
  on **every** rebalance an instance is revoked **all** its partitions and
  then assigned a new set, which may overlap the old one. Code reacting to
  revocation must therefore be cheap and correct for "everything revoked".
- `onPartitionsRevoked(partitions, { lost })` runs **after** the handler calls
  that were running for those partitions when the revocation arrived have
  finished (and committed). Use it to drop in-memory state (antifraud).
  `lost: true` means the group already gave the partitions to another member
  (this member was evicted, e.g. after a long pause).
- `onPartitionsAssigned(partitions)` runs before records of those partitions
  are handled. State can start empty; there is no "warm-up" support.
- Errors thrown by hooks are logged (`rebalance hook failed`) and ignored.

## Shutdown

On `SIGTERM` or `SIGINT` (when `handleSignals` is `true`) or `stop()`:

1. stop fetching new records and cancel pending retry resumes;
2. let in-flight handlers finish and commit their offsets (a handler that
   never returns blocks shutdown until Docker's stop timeout kills the
   process, which is still safe: the record is redelivered);
3. leave the group (revocation is logged, partitions move immediately instead
   of after the session timeout);
4. flush and disconnect the producer;
5. resolve `runtime.stopped`.

A second signal while stopping exits immediately with code 1. After
`stopped` resolves the process exits by itself once your own resources are
closed (verified with a real `SIGTERM` via `docker stop`, exit code 0).

## Logging

`createLogger({ level?, bindings?, destination? })` returns a
[pino](https://getpino.io) logger writing **one JSON object per line to
stdout**, synchronously (no lines lost on container stop):

```json
{"level":"info","time":"2026-11-11T14:03:27.512Z","service":"reconciliation","group":"reconciliation","topic":"pix.payments.v1","partition":3,"offset":"42","key":"merchant-007","attempt":0,"event_id":"5f0c1c1e-...","msg":"payment applied"}
```

- `level` is a string label, `time` ISO-8601, the message is under `msg`.
- Use `bindings: { service: '<name>' }` so every line names the service.
- `ctx.logger` already carries the record fields; don't repeat them in the log
  object (pino would emit duplicate keys).
- pino's API: `logger.info({ field: 1 }, 'message')` (object first).
- Runtime messages you can grep in experiments: `consumer runtime started`,
  `partitions assigned`, `partitions revoked`, `handler failed; record sent to
  retry topic` (warn), `record sent to DLQ` (warn), `hand-off failed; offset
  not committed, record will be redelivered` (error), `termination signal
  received; stopping gracefully`, `consumer runtime stopped`. At `debug`:
  `record handled`, `skipping retry record owned by another group`,
  `partition paused`, `partition resumed`.
- Messages from the Kafka client itself carry `component: "kafka-client"`.

## Test helpers

`@payments-kafka/consumer-runtime/testing` (needs Docker and the
`testcontainers` package as a devDependency of your service; never uses the
fixed Compose ports):

| Helper | Purpose |
| --- | --- |
| `startKafka({ image? })` → `{ bootstrapServers, container, stop() }` | Single-node KRaft broker (`apache/kafka:4.4.0`, like Compose, `auto.create.topics.enable=false`) on a random free port. ~5 s. Override the image with `KAFKA_TEST_IMAGE`. |
| `createRuntimeTopics(bootstrap, { unique? })` → `{ payments, fraudAlerts, retry, dlq }` | Creates the four topics with the architecture's partition counts. `unique: true` (default) appends a random suffix so tests sharing a broker do not interfere: pass `retry`/`dlq` as `retryTopic`/`dlqTopic`. |
| `createTopics(bootstrap, [{ topic, partitions }])` | Any other topic (existing ones are left alone). |
| `produce(bootstrap, topic, [{ key, value, headers?, partition? }])` | Publishes and waits for the acks. Objects are JSON-encoded. |
| `collect(bootstrap, topic, { count?, timeoutMs? })` → `CollectedRecord[]` | Reads from the beginning with a throwaway group. With `count`: resolves at `count` records or throws on timeout. Without: reads for `timeoutMs` and returns everything (assert absence). Records have `key`, `value`, `headers`, `partition`, `offset`, `json()`. |
| `committedOffsets(bootstrap, groupId, topic)` → `{ [partition]: offset }` | Next offset to read per partition, `-1` if never committed. |
| `groupMemberCount(bootstrap, groupId)` | Members currently in a group. |
| `testRuntimeConfig(bootstrap, overrides?)` | `RuntimeConfig` with a unique group id, 2 retries, 200 ms backoff, silent logs. |
| `FAST_GROUP_CONFIG` | `{ 'session.timeout.ms': 3000, 'heartbeat.interval.ms': 300 }`: pass as `kafka: { consumer: FAST_GROUP_CONFIG }` so killed members are evicted in ~3 s. |
| `createCapturingLogger(level?)` → `{ logger, lines, find(msg) }` | Logger that keeps parsed JSON lines in memory (assert on logs, e.g. notification lines). |
| `waitFor(fn, { timeoutMs?, intervalMs?, message? })`, `sleep(ms)` | Polling; `waitFor` resolves with the first truthy value. |
| `uniqueId(prefix)`, `makePaymentEvent(overrides?)`, `makeFraudAlert(overrides?)` | Unique names and valid contract fixtures with random ids. |

Recommended setup (one broker per test run, see this package's own
`vitest.config.ts` and `test/integration/global-setup.ts`):

```ts
// test/global-setup.ts
import type { TestProject } from 'vitest/node';
import { startKafka } from '@payments-kafka/consumer-runtime/testing';

declare module 'vitest' {
  export interface ProvidedContext {
    kafkaBootstrapServers: string;
  }
}

export default async function setup(project: TestProject) {
  const kafka = await startKafka();
  project.provide('kafkaBootstrapServers', kafka.bootstrapServers);
  return () => kafka.stop();
}
```

```ts
// vitest.config.ts
import { defineConfig } from 'vitest/config';
export default defineConfig({
  test: { globalSetup: ['test/global-setup.ts'], testTimeout: 90_000, hookTimeout: 180_000 },
});
```

```ts
// a test
import { expect, inject, it } from 'vitest';
import { createConsumerRuntime } from '@payments-kafka/consumer-runtime';
import {
  collect, createCapturingLogger, createRuntimeTopics, FAST_GROUP_CONFIG,
  makePaymentEvent, produce, testRuntimeConfig, waitFor,
} from '@payments-kafka/consumer-runtime/testing';

const bootstrap = inject('kafkaBootstrapServers');

it('dead-letters a payment it can never apply', async () => {
  const topics = await createRuntimeTopics(bootstrap);
  const { logger } = createCapturingLogger();
  const runtime = createConsumerRuntime({
    config: testRuntimeConfig(bootstrap, { retryMaxAttempts: 1, retryBackoffMs: 100 }),
    topics: [{ topic: topics.payments, schema: 'payment-event' }],
    retryTopic: topics.retry,
    dlqTopic: topics.dlq,
    handleSignals: false,
    logger,
    kafka: { consumer: FAST_GROUP_CONFIG },
    handler: myHandler,
  });
  await runtime.start();
  try {
    const event = makePaymentEvent({ event_type: 'payment.refunded', sequence: 3 });
    await produce(bootstrap, topics.payments, [{ key: event.merchant_id, value: event }]);
    const [dead] = await collect(bootstrap, topics.dlq, { count: 1 });
    expect(dead?.headers['x-dlq-reason']).toBe('retries_exhausted');
  } finally {
    await runtime.stop();
  }
});
```

Tips: always `stop()` runtimes in `afterEach`/`finally`; use unique topics
and group ids per test; to test a real crash, run the consumer in a child
process (`spawn(process.execPath, ['--import', 'tsx', file])`) and
`kill('SIGKILL')` it (see `test/integration/child.ts`). On Windows a real
`SIGTERM` cannot be delivered to a Node.js process (`kill('SIGTERM')`
terminates it abruptly); emit `process.emit('SIGTERM')` inside the child
instead, as `test/fixtures/child-consumer.ts` does.

## Service package setup

`services/<name>/package.json`:

```json
{
  "name": "@payments-kafka/<name>",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "typecheck": "tsc -p tsconfig.json --noEmit",
    "test": "vitest run",
    "start": "node dist/main.js"
  },
  "dependencies": {
    "@payments-kafka/consumer-runtime": "*"
  },
  "devDependencies": {
    "testcontainers": "^12.2.1"
  }
}
```

`services/<name>/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist" },
  "include": ["src"]
}
```

Notes:

- TypeScript, Vitest, ESLint, tsx and `@types/node` come from the workspace
  root; don't add them again. Root scripts: `npm run build`, `npm test`
  (builds first, then runs every workspace's `test`), `npm run lint` (ESLint
  on the whole repo plus every workspace's `typecheck`).
- The package resolves to `dist/`. Build it before running a service's tests
  on their own: `npm run build -w @payments-kafka/consumer-runtime` (the root
  `npm test` does it).
- `tsconfig.base.json` is strict with `noUncheckedIndexedAccess` and
  `verbatimModuleSyntax` (use `import type`), ES modules, `NodeNext`
  resolution: relative imports need the `.js` extension.
- Vitest is pinned to `~4.0.18`: 4.1.x crashes the `npm install` of npm 10.9
  (the npm bundled with Node 22).
- Node 22 LTS is the target (`.nvmrc`); the code also runs on Node 24.
- Each service adds itself to `package-lock.json` with `npm install`; if two
  branches conflict on the lockfile, take either side and run `npm install`
  again.

## Dockerfile pattern

Build context is the **repository root** (npm workspaces), as
`docs/architecture.md` specifies:
`build: { context: ., dockerfile: services/<name>/Dockerfile }`. The root
`.dockerignore` keeps host `node_modules` (Windows/macOS librdkafka binaries)
and `dist` out of the context. Copy this to `services/<name>/Dockerfile` and
replace `<name>`:

```dockerfile
# syntax=docker/dockerfile:1
# Build context: the repository root (npm workspaces):
#   docker build -f services/<name>/Dockerfile .
# Debian slim (glibc): @confluentinc/kafka-javascript ships prebuilt
# librdkafka binaries for glibc; Alpine (musl) would need a source build.
ARG NODE_IMAGE=node:22-bookworm-slim

FROM ${NODE_IMAGE} AS build
WORKDIR /app
COPY . .
RUN npm ci --no-audit --no-fund
RUN npm run build -w @payments-kafka/consumer-runtime -w @payments-kafka/<name>
RUN npm prune --omit=dev --no-audit --no-fund

FROM ${NODE_IMAGE}
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/contracts ./contracts
# Whole workspace directories: npm may install some dependencies in a
# workspace's own node_modules (e.g. ajv@8 under packages/consumer-runtime).
COPY --from=build /app/packages/consumer-runtime ./packages/consumer-runtime
COPY --from=build /app/services/<name> ./services/<name>
USER node
CMD ["node", "services/<name>/dist/main.js"]
```

Why each part matters:

- `contracts/` must sit at `/app/contracts`: the runtime reads the schemas
  from `<repo>/contracts` relative to its own location.
- Copy the **whole** `packages/consumer-runtime` and `services/<name>`
  directories, not just `dist/`: npm installs `ajv@8` (and possibly your own
  dependencies) in a workspace's nested `node_modules` when the root holds a
  different version. Copying only `dist/` fails at startup with
  `Cannot find package 'ajv'`.
- `npm ci` needs every workspace listed in the root `package.json` to be in
  the context, hence `COPY . .` (the `.dockerignore` keeps it small).
- `CMD` runs `node` directly (no `npm start`), so `SIGTERM` from
  `docker stop` reaches the process and triggers the graceful shutdown.
- No `EXPOSE` and no `container_name` in Compose: consumers are scaled with
  `--scale`.

This pattern was verified by building it for the sample service and running
the image against a Kafka container: records handled and dead-lettered,
`docker stop` → graceful stop, exit code 0.

## Developing this package

```
npm run build -w @payments-kafka/consumer-runtime
npm test -w @payments-kafka/consumer-runtime             # unit + integration (Docker)
npm run test:unit -w @payments-kafka/consumer-runtime
npm run test:integration -w @payments-kafka/consumer-runtime
```

- `src/runtime.ts`: the consumer loop; `src/protocol.ts`: topics, headers,
  backoff; `src/contracts.ts`: Ajv validation and types; `src/config.ts`;
  `src/logger.ts`; `src/producer.ts`; `src/kafka.ts`: the only module that
  touches `@confluentinc/kafka-javascript`; `src/testing/`: test helpers.
- Integration tests share one Kafka container (`test/integration/global-setup.ts`)
  and isolate themselves with unique topics and groups; the whole suite takes
  about a minute.

### Notes on `@confluentinc/kafka-javascript`

It exposes a KafkaJS-like API over librdkafka, but it is not KafkaJS:

- Config goes in two places: librdkafka properties at the top level
  (`'bootstrap.servers'`, `'enable.auto.commit'`, `'session.timeout.ms'`) and
  KafkaJS-style ones in a `kafkaJS` block (`groupId`, `logger`). The typings
  require `groupId` in the `kafkaJS` block once that block exists.
- The client **doubles** `max.poll.interval.ms` internally (it caches
  messages). With the default 300 s, a handler gets about 300 s.
- `pause()` and `seek()` take effect between messages; the runtime pauses,
  seeks back to the current record and returns, which is how a not-yet-due
  retry is re-read later.
- A user `rebalance_cb` is awaited before the client applies the
  (un)assignment, and in-flight `eachMessage` calls finish before an
  unassignment is applied; the runtime relies on both for its hook ordering.
- `consumer.disconnect()` waits for in-flight `eachMessage` calls, then leaves
  the group.
