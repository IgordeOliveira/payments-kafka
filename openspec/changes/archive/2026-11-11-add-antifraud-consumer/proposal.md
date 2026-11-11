# Proposal

## Why

Fraud signals come from the flow of events, not from single events: an amount
far from a merchant's normal, too many payments in a row, too many refunds.
The antifraud service is the stateful stream consumer of the project and the
only consumer that also produces, which makes it the place to study state,
replay and consume-then-produce under at-least-once delivery.

## What Changes

- Add the TypeScript service `antifraud` (consumer group `antifraud`) built on
  `@payments-kafka/consumer-runtime`.
- Apply three per-merchant rules to `pix.payments.v1`: amount outlier, payment
  burst and refund excess.
- Publish a fraud alert to `pix.fraud-alerts.v1` whenever a rule fires, with a
  deterministic `alert_id`.

## Capabilities

### New Capabilities
- `antifraud`: per-merchant fraud rules over the payment stream and the alerts they publish.

### Modified Capabilities

None.

## Impact

- New directory `services/antifraud/` (TypeScript, Dockerfile).
- Consumes `pix.payments.v1` and the retry topic; produces to `pix.fraud-alerts.v1`.
- Depends on `packages/consumer-runtime`.
