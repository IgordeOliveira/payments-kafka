# Design

## Context

Consumption, validation, retry and DLQ come from
`@payments-kafka/consumer-runtime`. The lifecycle and `sequence` rules are
fixed in `docs/architecture.md`. Unlike notification, the side effect here is
a database write, so the dedupe record and the effect can share one
transaction: this is the case where effectively-once is fully achievable.

## Goals / Non-Goals

**Goals:**
- Balances that are a pure function of the set of distinct events, independent
  of duplicates, arrival order across retries, and number of instances.
- A replay from offset zero that rebuilds identical balances.

**Non-Goals:**
- Reconciling against an external bank statement.
- Partial refunds, fees, multi-currency.

## Decisions

- **Schema** (database `reconciliation`, plain SQL migrations applied at startup):
  - `processed_events(event_id UUID PRIMARY KEY, processed_at)`
  - `payments(payment_id UUID PRIMARY KEY, merchant_id, amount_cents, status, last_sequence, updated_at)`
  - `ledger_entries(event_id UUID PRIMARY KEY, merchant_id, payment_id, entry_type, amount_cents, occurred_at)`
  - `merchant_balances(merchant_id TEXT PRIMARY KEY, balance_cents BIGINT, confirmed_count, refunded_count, updated_at)`
- **One transaction per event**: insert into `processed_events`
  (`ON CONFLICT DO NOTHING`); if no row was inserted the event is a duplicate
  and the transaction ends with no further effect. Otherwise apply the state
  transition, the ledger entry and the balance update, then commit. Dedupe
  and effect are atomic, so a crash anywhere leaves either everything or
  nothing applied, and redelivery is harmless.
- **Balance rules**: `payment.confirmed` credits `amount_cents`;
  `payment.refunded` debits `amount_cents`; `payment.created` and
  `payment.expired` change payment state only.
- **Ordering by `sequence`** against `payments.last_sequence` (0 if unknown):
  - `sequence == last + 1` and a valid transition → apply;
  - `sequence <= last` with a new `event_id` → a conflicting event for a step
    already taken (for example expired after confirmed). Non-retryable: DLQ;
  - `sequence > last + 1` → a gap: the predecessor has not arrived yet. Throw
    a retryable error so the runtime re-delivers it after backoff, by which
    time the predecessor has normally been applied. If it never arrives the
    event ends in the DLQ, which is the correct outcome for an orphan.
  The gap case rolls back the transaction, so the event is not marked
  processed.
- **Row-level locking for concurrency**: the payment row is locked
  (`SELECT ... FOR UPDATE`) and the balance is updated with an atomic
  `UPDATE ... SET balance_cents = balance_cents + $1`. Two instances can only
  touch the same merchant concurrently through the retry topic; locks make
  that safe.
- **Ledger as the audit trail.** Every balance movement has one ledger row
  keyed by `event_id`. The consistency check compares, per merchant,
  `SUM(ledger credits) - SUM(ledger debits)` with `merchant_balances`, and the
  counts of confirmed and refunded entries.
- **CLI commands in the same package** (`npm run cli -- <command>`, also
  runnable in the container):
  - `check`: runs the consistency check, prints mismatches, exits non-zero on any;
  - `balances`: prints all merchant balances as stable, sorted JSON (used to
    compare before and after a replay);
  - `reset-state`: truncates all four tables (refuses to run unless
    `--yes` is passed).
- **Replay procedure** (scripted in `add-validation-experiments`): stop the
  consumers, `reset-state`, reset the group offsets to earliest, start again.
  Because effects depend only on the set of events, the rebuilt balances
  match.
- **`pg` (node-postgres)**; integration tests use Testcontainers Postgres and
  the runtime's Kafka test helpers.

## Risks / Trade-offs

- [`processed_events` and `ledger_entries` grow forever] → acceptable locally;
  required for the replay and check experiments.
- [Out-of-order events add latency through retry backoff] → intended: it makes
  the retry path do real work and be observable.
- [A reset of offsets without `reset-state` is a no-op] → correct idempotent
  behavior, and worth demonstrating; documented in the README.
- [`reset-state` while consumers run gives inconsistent state] → the command
  documents that consumers must be stopped; the experiment script enforces it.
