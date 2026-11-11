# notification

TypeScript consumer (consumer group `notification`) that tells a restaurant
(merchant) about its payments and about fraud alerts, and never tells it twice
about the same event or alert. Built on
[`@payments-kafka/consumer-runtime`](../../packages/consumer-runtime/README.md),
which provides consumption with manual commits, contract validation, the retry
topic, the DLQ and graceful shutdown. Shared conventions live in
[`docs/architecture.md`](../../docs/architecture.md).

## What is notified

| Source | Records | Notification `kind` | Dedupe key |
| --- | --- | --- | --- |
| `pix.payments.v1` | `payment.confirmed`, `payment.expired`, `payment.refunded` | the `event_type` | `event_id` |
| `pix.payments.v1` | `payment.created` | none: consumed, committed and skipped (an unpaid charge is not news) | none |
| `pix.fraud-alerts.v1` | every alert | `fraud_alert` (names the `rule`) | `alert_id` |

The channel is the console. Each notification is **one JSON line on stdout**
whose `type` is `notification`:

```json
{"type":"notification","time":"2026-11-11T21:11:10.199Z","service":"notification","kind":"payment.confirmed","merchant_id":"merchant-007","notification_key":"5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11","payment_id":"0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42","message":"Payment 0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42 of BRL 45.90 was confirmed."}
{"type":"notification","time":"...","service":"notification","kind":"fraud_alert","merchant_id":"merchant-012","notification_key":"payment_burst:5f0c...","payment_id":"0b9d...","rule":"payment_burst","message":"Fraud alert \"payment_burst\" raised for payment 0b9d...."}
```

This format is a contract with the experiments, which count notifications by
filtering container output for `"type":"notification"`. Nothing else the
service writes has a `type` field: its log lines are the runtime's pino lines
(`level`, `msg`...), for example `notification sent` and
`notification already claimed; not sent again` (both at `info`, with
`notification_key` and `kind`).

```
docker compose logs notification | grep '"type":"notification"' | wc -l
```

## Never twice: claim, then send

Delivery from Kafka is at-least-once: the same event arrives again after a
crash before the commit, a retry, a rebalance, or because a producer
published it twice. So the service keeps the keys it has notified in Postgres
(database `notification`):

```sql
CREATE TABLE sent_notifications (
    notification_key TEXT        PRIMARY KEY,  -- event_id or alert_id
    kind             TEXT        NOT NULL,
    merchant_id      TEXT        NOT NULL,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

For each record that notifies, the handler (`src/handler.ts`):

1. **claims** the key: `INSERT ... ON CONFLICT (notification_key) DO NOTHING`,
   committed on its own (`src/dedupe.ts`);
2. if a row was inserted, **sends** the notification; if not, the key was
   already claimed and it sends nothing;
3. returns, and the runtime commits the offset.

The primary key is also the concurrency control: two instances racing on the
same key both try the insert and exactly one of them inserts the row and
sends.

### The trade-off: at most once

A side effect outside the database (writing a line, calling a push API)
cannot share a transaction with the dedupe write, so one of the two orders
has to be picked:

| Order | Crash between the two steps | Guarantee |
| --- | --- | --- |
| send, then record | the redelivery sends it again | at least once (duplicates) |
| **record, then send** (chosen) | the redelivery finds the key and sends nothing | **at most once** (a lost notification) |

The requirement is "never twice", so the claim comes first. The price is a
window between the claim and the send: if the process dies there, or the send
itself fails, that notification is never sent. A send failure makes the
handler throw, so the record goes to the retry topic, but the retry finds the
key claimed and does nothing (`test/integration/trade-off.test.ts`
demonstrates it). The row in `sent_notifications` then says "sent" for a
notification nobody received; `sent_at` is really "claimed at".

**How a real channel closes the gap.** Real providers (email, push, SMS,
webhooks) accept an *idempotency key* and drop a second request with the same
key. With one, the service could send with `notification_key` as the
idempotency key and *then* record the key (or not record anything at all):
a crash before the record means the redelivery sends again, and the provider
discards the duplicate. That is at-least-once on our side plus dedupe on
theirs, which gives exactly one notification without a lost-notification
window. A console has no such key, so this service makes the at-most-once
choice explicit instead. An outbox of pending notifications drained by a
sender is the other common answer when the channel has no idempotency key.

### Failure handling

| Situation | Result |
| --- | --- |
| Postgres unreachable when claiming | handler throws before sending; record goes through the retry topic (`RETRY_BACKOFF_MS · 2^(n-1)`) and is notified once when the database is back; after `RETRY_MAX_ATTEMPTS` failed retries it goes to the DLQ (no notification) |
| Duplicate event / regenerated alert (same `alert_id`) | claim rejected, nothing sent, offset committed |
| Crash after the send, before the commit | redelivered, claim rejected, nothing sent |
| Crash or send failure after the claim, before the send | that notification is lost (the trade-off above) |
| Invalid record (not JSON, schema violation) | DLQ by the runtime, handler not called |

Startup fails (exit code 1, `startup failed` log line) if `DATABASE_URL` is
unreachable or a configuration value is invalid. The pool uses a 5 s
connection timeout and a 10 s query timeout, so a hanging database fails the
handler instead of blocking its partition.

Not done (out of scope for a local study project): a retention job for
`sent_notifications`, which grows forever.

## Configuration

| Variable | Default | Compose |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://pix:pix@localhost:5432/notification` | `postgres://pix:pix@postgres:5432/notification` |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `RETRY_MAX_ATTEMPTS`, `RETRY_BACKOFF_MS`, `LOG_LEVEL` | see the [runtime](../../packages/consumer-runtime/README.md#configuration) | `kafka:29092`, `notification`, `4`, `1000`, `info` |

Migrations are plain SQL files in `migrations/`, applied at startup in file
name order, each once (tracked in `schema_migrations`), in one transaction
under an advisory lock so instances starting together do not race.

## Running

With the Compose stack (from the repository root; the service is in the
`apps` profile and can be scaled):

```
docker compose --profile apps up -d --build notification
docker compose --profile apps up -d --scale notification=2
docker compose logs -f notification
```

Locally against the Compose infrastructure (`docker compose up -d`), from the
repository root:

```
npm install
npm run build
npm start -w @payments-kafka/notification
```

Building the image on its own (build context is the repository root):

```
docker build -f services/notification/Dockerfile .
```

## Tests

From the repository root (Docker must be running; the tests start their own
Kafka and Postgres with Testcontainers on random ports and never use the
Compose stack):

```
npm run build -w @payments-kafka/consumer-runtime
npm test -w @payments-kafka/notification                    # unit + integration
npm run test:unit -w @payments-kafka/notification
npm run test:integration -w @payments-kafka/notification
```

| File | What it proves |
| --- | --- |
| `test/unit/notifier.test.ts` | output line fields and message per kind; `payment.created` produces nothing |
| `test/unit/handler.test.ts` | claim then send, skip `payment.created`, a rejected claim emits nothing, a store failure rejects without sending |
| `test/integration/migrations.test.ts` | the table exists after startup; migrations are safe twice and concurrently |
| `test/integration/claim.test.ts` | first claim wins, second is rejected, concurrent claims have one winner |
| `test/integration/service.test.ts` | a confirmed event and an alert give one notification each; 10 confirmed events give 10 lines |
| `test/integration/never-twice.test.ts` | duplicate event, regenerated alert, full restart, `SIGKILL` between send and commit (child process), two instances of the group racing |
| `test/integration/storage-failure.test.ts` | Postgres stopped: record to the retry topic, nothing emitted; Postgres back: exactly one notification |
| `test/integration/trade-off.test.ts` | the send fails after the claim: the redelivery does not send it (at most once) |

Set `POSTGRES_TEST_IMAGE` to use another Postgres image (default
`postgres:18.6`, as in Compose).
