# Proposal

## Why

Telling a restaurant about a payment is a side effect that cannot be undone:
a duplicate notification is a visible bug. With at-least-once delivery,
duplicates will arrive, so the notification service is where "never notify
twice" has to be earned with an idempotent consumer.

## What Changes

- Add the TypeScript service `notification` (consumer group `notification`)
  built on `@payments-kafka/consumer-runtime`.
- Consume both `pix.payments.v1` and `pix.fraud-alerts.v1`.
- Emit a notification (a structured log line to the console) for confirmed,
  expired and refunded payments and for every fraud alert.
- Guarantee that a given event or alert is never notified twice, using a
  dedupe table in Postgres.

## Capabilities

### New Capabilities
- `notification`: merchant notifications for payment events and fraud alerts, emitted at most once per event.

### Modified Capabilities

None.

## Impact

- New directory `services/notification/` (TypeScript, Dockerfile).
- Owns the tables in Postgres database `notification`.
- Consumes two topics plus the retry topic; produces only to retry and DLQ.
- Depends on `packages/consumer-runtime`.
