# Tasks

## 1. Service setup

- [x] 1.1 Scaffold `services/reconciliation` as a workspace depending on `@payments-kafka/consumer-runtime` and `pg` (TypeScript, Vitest, `start` and `cli` scripts); verify `npm run build` and `npm test` pass at the root
- [x] 1.2 Add SQL migrations for `processed_events`, `payments`, `ledger_entries`, `merchant_balances` and a startup migration runner reading `DATABASE_URL`; verify with a Testcontainers Postgres test that tables exist and migrations are re-runnable

## 2. Applying events

- [x] 2.1 Implement the sequence and transition decision (apply, duplicate, gap, conflict) as a pure function; verify with unit tests for every lifecycle step and every out-of-order and conflict case
- [x] 2.2 Implement the single-transaction apply: processed-event insert, payment row lock and update, ledger entry, atomic balance update; verify with integration tests for the full lifecycle, balances after confirm and refund, and no balance change on created or expired
- [x] 2.3 Verify idempotency with integration tests: a stream applied twice equals once, and an injected failure before commit leaves no partial effect
- [x] 2.4 Map decisions to runtime outcomes (gap → retryable error, conflict → `NonRetryableError`); verify with unit tests on the thrown error types

## 3. Kafka integration

- [x] 3.1 Wire the handler to the consumer runtime (group `reconciliation`, topic `pix.payments.v1`); verify with a Testcontainers test that a produced lifecycle yields the expected payment state and balance
- [x] 3.2 Verify out-of-order handling end to end: confirmed before created is applied via the retry topic, and an orphan confirmed event ends in the DLQ with the balance unchanged
- [x] 3.3 Verify redelivery safety: stop the consumer abruptly after the database commit and before the offset commit, restart, and assert the balance counts the event once
- [x] 3.4 Verify multi-instance safety: process a stream with two instances joining and leaving, and assert balances equal the single-instance result

## 4. Commands

- [x] 4.1 Implement `check` (ledger versus balances and counters, non-zero exit on mismatch); verify with integration tests for a consistent state and for a tampered balance
- [x] 4.2 Implement `balances` (stable sorted JSON) and `reset-state` (requires `--yes`); verify with integration tests including refusal without the flag
- [x] 4.3 Verify deterministic rebuild with an integration test: export balances, reset state, reprocess the topic from the earliest offset with a fresh group, export again and compare

## 5. Packaging and docs

- [x] 5.1 Add `services/reconciliation/Dockerfile` following the pattern in the consumer-runtime README (root build context, CLI runnable in the container); verify `docker build -f services/reconciliation/Dockerfile .` succeeds
- [x] 5.2 Write `services/reconciliation/README.md` (data model, idempotency transaction, sequence rules, commands, replay procedure and why an offset reset alone is a no-op, how to run and test); verify the documented commands run as written
