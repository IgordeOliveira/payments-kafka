# Proposal

## Why

Money is where at-least-once delivery hurts: a duplicated confirmation credits
a restaurant twice, a lost one never credits it. Reconciliation is the
consumer that turns the event stream into state (payment status and merchant
balance) and must produce the same balances whether events arrive once,
twice, out of order, or are replayed from the beginning.

## What Changes

- Add the TypeScript service `reconciliation` (consumer group
  `reconciliation`) built on `@payments-kafka/consumer-runtime`.
- Maintain in Postgres the current state of every payment, a ledger of balance
  movements and the balance of every merchant.
- Apply each event exactly once in effect, using a processed-events table in
  the same transaction as the state change.
- Handle out-of-order events through the retry path.
- Provide commands to check that the ledger matches the balances and to reset
  state for a full replay.

## Capabilities

### New Capabilities
- `reconciliation`: payment state, merchant balances and ledger maintained idempotently from the event stream, with consistency check and replay support.

### Modified Capabilities

None.

## Impact

- New directory `services/reconciliation/` (TypeScript, Dockerfile).
- Owns the tables in Postgres database `reconciliation`.
- Consumes `pix.payments.v1` plus the retry topic; produces only to retry and DLQ.
- Depends on `packages/consumer-runtime`.
