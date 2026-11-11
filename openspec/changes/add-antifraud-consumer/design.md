# Design

## Context

Consumption, validation, retry and DLQ come from
`@payments-kafka/consumer-runtime`. The alert contract and the deterministic
`alert_id` are fixed in `docs/architecture.md` and `contracts/`. The open
question "state in memory or persisted" is resolved as in memory.

## Goals / Non-Goals

**Goals:**
- Rules that are pure and unit-testable without Kafka.
- Alerts that are safe under redelivery and replay.

**Non-Goals:**
- Statistical or ML-based detection.
- State that survives restarts or follows partitions across instances.

## Decisions

- **State in memory, per merchant.** Because the topic is keyed by
  `merchant_id`, one instance sees all events of the merchants in its assigned
  partitions, in order. No database is needed.
- **State is dropped when a partition is revoked** and starts empty when a
  partition is assigned. Persisting it is the "real" answer, but the trade-off
  is the lesson: after a rebalance or restart the rules need a warm-up, and a
  replay from the earliest offset rebuilds everything. Documented in the README.
- **Rules as pure functions** `(merchantState, event, config) → alerts[]`,
  evaluated on event time (`occurred_at`), not wall-clock time, so replay
  produces the same alerts as the live run.
  - `amount_outlier`: on `payment.created`, alert when the amount exceeds
    `ANTIFRAUD_OUTLIER_FACTOR` (default 5) times the merchant's running mean,
    once at least `ANTIFRAUD_OUTLIER_MIN_SAMPLES` (default 20) payments were
    seen. The outlier itself is not added to the mean.
  - `payment_burst`: on `payment.created`, alert when more than
    `ANTIFRAUD_BURST_LIMIT` (default 30) payments fall inside a sliding window
    of `ANTIFRAUD_BURST_WINDOW_MS` (default 10000).
  - `refund_excess`: on `payment.refunded`, alert when more than
    `ANTIFRAUD_REFUND_LIMIT` (default 5) refunds fall inside
    `ANTIFRAUD_REFUND_WINDOW_MS` (default 60000).
  - Burst and refund rules alert once per breach: after firing they stay quiet
    until the window count drops back to the limit or below.
- **Duplicate events must not double count.** A bounded per-merchant set of
  recently seen `event_id`s (LRU) makes rule state idempotent within its
  horizon.
- **Consume-then-produce without transactions.** The alert is produced and
  acknowledged (`acks=all`) inside the handler before it returns, so the
  offset is committed only after the alert is durable. A crash in between
  re-emits the same alert with the same `alert_id`; the notification service
  dedupes on it. This is the at-least-once + idempotent-consumer pattern that
  stands in for exactly-once.
- **Alert key is `merchant_id`**, preserving per-merchant order of alerts.
- **A failed alert publish is a retryable handler error** and follows the
  normal retry path.

## Risks / Trade-offs

- [Missed detections during warm-up after a rebalance] → accepted and
  documented; observable by scaling the service during an experiment.
- [Retried events arrive late and out of order] → rules use event time and the
  seen-id set; a late event can still shift a window slightly. Accepted.
- [LRU horizon lets a very old duplicate count twice] → bounded memory is
  preferred; horizon is configurable.
