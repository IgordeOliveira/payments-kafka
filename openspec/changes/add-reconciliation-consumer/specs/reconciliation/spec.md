# Spec Delta

## Purpose

Maintains the state of every payment and the balance of every merchant from the event stream, applying each event exactly once in effect and proving that the ledger matches the balances.

## ADDED Requirements

### Requirement: Payment state tracking
The service SHALL store, for every payment, its merchant, amount, current status and last applied `sequence`, updated from the events of `pix.payments.v1`.

#### Scenario: Full lifecycle
- **WHEN** created, confirmed and refunded events for one payment are consumed in order
- **THEN** the stored payment has status `REFUNDED` and last sequence 3

### Requirement: Merchant balance
The service SHALL increase a merchant's balance by `amount_cents` for each `payment.confirmed` event and decrease it by `amount_cents` for each `payment.refunded` event, and SHALL NOT change the balance for `payment.created` or `payment.expired`.

#### Scenario: Confirmed and refunded
- **WHEN** a merchant has three confirmed payments of 1000 cents and one of them is refunded
- **THEN** the merchant's balance is 2000 cents

#### Scenario: Expired payment
- **WHEN** a payment is created and then expires
- **THEN** the merchant's balance is unchanged

### Requirement: Idempotent application
The service SHALL apply each `event_id` at most once, recording the event as processed in the same database transaction as its effects, so that duplicates and redeliveries change nothing.

#### Scenario: Duplicated events
- **WHEN** every event of a stream is delivered twice
- **THEN** payment states and merchant balances equal those of the stream delivered once

#### Scenario: Crash before commit
- **WHEN** the service is killed after applying an event's effects but before committing its Kafka offset, and the event is redelivered
- **THEN** the balance reflects the event exactly once

#### Scenario: Crash during the transaction
- **WHEN** the service is killed before the database transaction commits
- **THEN** none of the event's effects are stored and the redelivered event is applied in full

### Requirement: Out-of-order events are deferred
When an event's `sequence` is more than one ahead of the payment's last applied sequence, the service SHALL NOT apply it and SHALL treat it as a retryable failure, so that it is applied after its predecessor arrives.

#### Scenario: Confirmation arrives before creation
- **WHEN** a `payment.confirmed` event is consumed before the `payment.created` event of the same payment, and the created event arrives shortly after
- **THEN** the confirmed event goes through the retry topic, is applied after the created event, and the final state is `CONFIRMED` with the balance credited once

#### Scenario: Predecessor never arrives
- **WHEN** a `payment.confirmed` event has no preceding created event and retries are exhausted
- **THEN** the event ends in the DLQ and the balance is unchanged

### Requirement: Conflicting events are rejected
When a new event carries a `sequence` at or below the payment's last applied sequence, or a transition the lifecycle does not allow, the service SHALL NOT apply it and SHALL send it to the DLQ as non-retryable.

#### Scenario: Expired after confirmed
- **WHEN** a payment is already `CONFIRMED` and a `payment.expired` event with a different `event_id` arrives for it
- **THEN** the event is sent to the DLQ and the payment remains `CONFIRMED`

### Requirement: Ledger of balance movements
The service SHALL record one ledger entry per applied `payment.confirmed` (credit) and `payment.refunded` (debit) event, identified by the `event_id`, in the same transaction as the balance update.

#### Scenario: Ledger matches events
- **WHEN** a merchant has two confirmed payments and one refund applied
- **THEN** the ledger has two credit entries and one debit entry for that merchant

### Requirement: Consistency check
The service SHALL provide a `check` command that compares, for every merchant, the balance and counters with the totals derived from the ledger, reports every mismatch and exits with a non-zero status if any is found.

#### Scenario: Consistent state
- **WHEN** `check` runs after a stream has been fully processed
- **THEN** it reports no mismatches and exits with status 0

#### Scenario: Tampered balance
- **WHEN** a merchant's balance is changed directly in the database and `check` runs
- **THEN** it reports that merchant and exits with a non-zero status

### Requirement: Deterministic rebuild
The service SHALL provide `balances` and `reset-state` commands such that clearing state and reprocessing the topic from the earliest offset reproduces exactly the same merchant balances.

#### Scenario: Replay rebuilds the same balances
- **WHEN** balances are exported, state is reset, the consumer group offsets are reset to earliest and the topic is reprocessed
- **THEN** the newly exported balances are identical to the first export

#### Scenario: Reset requires confirmation
- **WHEN** `reset-state` is run without the confirmation flag
- **THEN** it exits with an error and deletes nothing

### Requirement: Safe with multiple instances
The service SHALL produce the same balances when run as several instances of one consumer group as when run as a single instance.

#### Scenario: Scaled out during processing
- **WHEN** a stream is processed while instances are added and removed
- **THEN** final balances equal those of a single-instance run and `check` passes
