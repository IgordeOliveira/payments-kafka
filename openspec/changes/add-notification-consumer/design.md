# Design

## Context

Consumption, validation, retry and DLQ come from
`@payments-kafka/consumer-runtime`. The notification channel is the console
(structured log), per the project scope. The dedupe keys are `event_id` for
payment events and `alert_id` for alerts, both guaranteed stable by the
contract.

## Goals / Non-Goals

**Goals:**
- Never emit the same notification twice, across duplicates, redeliveries,
  retries, restarts, rebalances and multiple instances.
- Make the unavoidable trade-off explicit and observable.

**Non-Goals:**
- Real channels (email, push, webhooks), templates, localization.
- Guaranteeing that every notification is emitted (see trade-off below).

## Decisions

- **Dedupe store in Postgres** (database `notification`), table
  `sent_notifications(notification_key TEXT PRIMARY KEY, kind, merchant_id,
  sent_at)`. In-memory dedupe would fail across restarts and instances.
  Migrations are plain SQL files applied at startup.
- **Claim, then send.** The handler first inserts the key
  (`INSERT ... ON CONFLICT DO NOTHING`) and commits. Only if a row was
  inserted does it emit the notification. A side effect outside the database
  cannot share a transaction with the dedupe write, so one of two failure
  modes must be chosen:
  - send then record: a crash in between sends twice (at-least-once);
  - **record then send: a crash in between sends zero times (at-most-once).**
  The requirement is "never twice", so record-then-send is chosen. The README
  explains this and how a real system would close the gap (an idempotency key
  honored by the downstream channel).
- **The primary key is the concurrency control.** Two instances racing on the
  same key both attempt the insert; exactly one inserts a row and sends.
- **Which events notify:** `payment.confirmed`, `payment.expired`,
  `payment.refunded` and every fraud alert. `payment.created` is consumed and
  skipped (a restaurant does not need to hear about an unpaid charge); the
  skip is still deduped by nothing because it has no side effect.
- **Notification output** is one JSON log line with `type: "notification"`,
  `kind`, `merchant_id`, `notification_key` and a human-readable `message`, so
  experiments can count notifications by grepping container logs.
- **Two source topics, one handler** dispatching on the topic. Ordering between
  the two topics is not guaranteed and not needed.
- **Database errors are retryable**: the handler throws and the record follows
  the retry path. Nothing has been sent at that point, so retrying is safe.
- **`pg` (node-postgres) with a small pool**; integration tests use
  Testcontainers Postgres and the runtime's Kafka test helpers.

## Risks / Trade-offs

- [A crash between claim and send loses one notification] → accepted,
  documented, and demonstrated by a test that fails the send after the claim.
- [The dedupe table grows forever] → acceptable locally; a retention job is
  noted as future work.
- [Postgres down means no notifications] → records flow to retry and then DLQ;
  visible in Kafka UI.
