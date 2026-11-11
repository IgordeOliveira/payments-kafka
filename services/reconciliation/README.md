# reconciliation

TypeScript consumer (consumer group `reconciliation`) that turns
`pix.payments.v1` into state in Postgres: the status of every payment, a
ledger of balance movements and the balance of every merchant. Each event is
applied **exactly once in effect**, whether it is delivered once, twice, out
of order, by several instances or replayed from the beginning.

Consumption, contract validation, retry and DLQ come from
[`@payments-kafka/consumer-runtime`](../../packages/consumer-runtime/README.md).
Topics, the event contract, the retry/DLQ protocol and environment variables
are defined in [`docs/architecture.md`](../../docs/architecture.md).

Contents: [Data model](#data-model) · [Applying an event](#applying-an-event) ·
[Sequence rules](#sequence-rules) · [Concurrency](#concurrency-and-several-instances) ·
[Commands](#commands) · [Replay](#replay-procedure) · [Running](#running) ·
[Testing](#testing) · [Code map](#code-map)

## Data model

Database `reconciliation`. Plain SQL migrations in [`migrations/`](migrations)
are applied at startup by the service and by every CLI command (tracked in
`schema_migrations`, under an advisory lock, so several instances starting
together apply them once).

| Table | Key | Contents |
| --- | --- | --- |
| `processed_events` | `event_id` | One row per **applied** event. The idempotency record. |
| `payments` | `payment_id` | `merchant_id`, `amount_cents`, `status` (`CREATED`, `CONFIRMED`, `EXPIRED`, `REFUNDED`), `last_sequence`, `updated_at`. |
| `ledger_entries` | `event_id` | One row per balance movement: `credit` for `payment.confirmed`, `debit` for `payment.refunded`, with `merchant_id`, `payment_id`, `amount_cents`, `occurred_at`. `UNIQUE (payment_id, entry_type)`. |
| `merchant_balances` | `merchant_id` | `balance_cents`, `confirmed_count`, `refunded_count`, `updated_at`. A row exists once a merchant had a balance movement. |

Balance rules: `payment.confirmed` credits `amount_cents`, `payment.refunded`
debits `amount_cents` (refunds are always full), `payment.created` and
`payment.expired` change the payment status only.

## Applying an event

The handler applies each event in **one database transaction**
([`src/apply.ts`](src/apply.ts)):

1. `INSERT INTO processed_events ... ON CONFLICT DO NOTHING`. If no row was
   inserted the event was already applied: **duplicate**, nothing else
   happens, the offset is committed.
2. `SELECT ... FROM payments WHERE payment_id = $1 FOR UPDATE` (the payment's
   current state, locked).
3. Decide with the [sequence rules](#sequence-rules).
4. On apply: insert or update the payment; for money events insert the ledger
   entry and update the balance atomically
   (`INSERT ... ON CONFLICT (merchant_id) DO UPDATE SET balance_cents = balance_cents + delta, ...`).
5. `COMMIT`.

The dedupe row and the effects commit together, so a crash anywhere leaves
either all of an event or none of it:

| Crash point | State after the crash | On redelivery |
| --- | --- | --- |
| Before `COMMIT` | Postgres rolls the transaction back: nothing stored, event not marked processed | Applied in full |
| After `COMMIT`, before the Kafka offset commit | Event fully applied | Recognized as duplicate, offset committed |

Gaps and conflicts **roll back** too: an event that was not applied is never
marked processed.

## Sequence rules

Each event's `sequence` is compared with the payment's `last_sequence` (0 when
the payment is unknown) by a pure function, [`src/decision.ts`](src/decision.ts):

| Case | Decision | Handler | Runtime outcome |
| --- | --- | --- | --- |
| `event_id` already in `processed_events` | duplicate | returns | offset committed |
| `sequence == last + 1`, allowed transition, same merchant and amount | apply | returns | offset committed |
| `sequence > last + 1` (predecessor not applied yet) | gap | throws `OutOfOrderError` (retryable) | retry topic with backoff; DLQ `retries_exhausted` if the predecessor never arrives |
| `sequence <= last` with a new `event_id` (e.g. expired after confirmed) | conflict | throws `NonRetryableError` | DLQ `non_retryable` |
| `sequence == last + 1` but a transition the lifecycle does not allow (refunded after expired), or a different merchant or amount than the stored payment | conflict | throws `NonRetryableError` | DLQ `non_retryable` |

Any other error (database unreachable...) is thrown as is and is therefore
retried by the runtime.

Out-of-order events therefore take the retry path: a confirmation consumed
before its creation fails as a gap, waits `RETRY_BACKOFF_MS`, and is applied on
retry once the creation has been applied. An orphan (a confirmation whose
creation never arrives) ends in the DLQ after `RETRY_MAX_ATTEMPTS` retries with
the balance unchanged.

## Concurrency and several instances

Records of one partition are handled in order, so on the main topic a
merchant is only handled by one instance at a time. The retry topic breaks
this: a retried event can be handled by any instance, concurrently with the
merchant's newer events on the main topic. That is safe because:

- the payment row is locked (`FOR UPDATE`) before deciding, so two events of
  the same payment are serialized and the second one sees the first one's
  result;
- the first event of a payment inserts the row with `ON CONFLICT DO NOTHING`
  and decides again if another transaction created it concurrently;
- the balance is updated with an atomic `balance_cents = balance_cents + delta`
  under the row lock of the balance row;
- two deliveries of the same event serialize on the `processed_events`
  primary key: one applies, the other sees a duplicate;
- locks are always taken in the same order (processed event, payment,
  merchant balance), so concurrent transactions cannot deadlock.

The tests prove it with 60 concurrent transactions on one merchant, racing
deliveries of the same event, racing conflicting events, and a Kafka run in
which instances join and leave mid-stream (see [Testing](#testing)).

## Commands

The CLI ships in the same package and image. Every command applies pending
migrations first, writes **only its result to stdout** (diagnostics go to
stderr) and uses these exit codes:

| Exit code | Meaning |
| --- | --- |
| `0` | success (`check`: no mismatch) |
| `1` | `check` found at least one mismatch |
| `2` | usage error: unknown command or argument, `reset-state` without `--yes` (nothing changed) |
| `3` | operational error (database unreachable, query failed) |

| Command | What it does |
| --- | --- |
| `check` | Compares, for every merchant in `merchant_balances` or `ledger_entries`, the stored `balance_cents`, `confirmed_count` and `refunded_count` with the ledger (`SUM(credits) - SUM(debits)`, number of credits, number of debits). Prints a JSON report `{ ok, merchants, ledger_entries, processed_events, mismatches: [{ merchant_id, field, stored, ledger }] }`, one `MISMATCH ...` line per mismatch on stderr, and exits `1` if there is any. |
| `balances` | Prints every merchant balance as a JSON array sorted by `merchant_id` (code-point order), 2-space indentation, trailing newline. Timestamps are left out, so the output is a pure function of the applied events and two exports can be compared byte for byte. |
| `reset-state --yes` | Truncates `processed_events`, `payments`, `ledger_entries` and `merchant_balances` (the schema stays). Without `--yes` it refuses, exits `2` and deletes nothing. **Stop the consumers first**: running it while they consume gives inconsistent state. |

`balances` output:

```json
[
  {
    "merchant_id": "merchant-007",
    "balance_cents": 4590,
    "confirmed_count": 1,
    "refunded_count": 0
  }
]
```

### In the Compose stack

The image's working directory is `/app`; the CLI is
`node services/reconciliation/dist/cli.js`. Use `-T` (no TTY) so the output
is byte-exact (a TTY turns `\n` into `\r\n`):

```sh
# in a one-off container (no consumer needs to be running)
docker compose run --rm --no-deps -T reconciliation node services/reconciliation/dist/cli.js check
docker compose run --rm --no-deps -T reconciliation node services/reconciliation/dist/cli.js balances > balances-before.json
docker compose run --rm --no-deps -T reconciliation node services/reconciliation/dist/cli.js reset-state --yes

# or inside a running instance
docker compose exec -T reconciliation node services/reconciliation/dist/cli.js balances
```

### Counting applied events

Every applied event has exactly one row in `processed_events` (duplicates,
gaps and conflicts add none), so the number of distinct applied events is:

```sh
docker compose exec -T postgres psql -U pix -d reconciliation -tAc 'SELECT count(*) FROM processed_events'
```

`check` also reports it as `processed_events`. Events that ended in the DLQ
are not counted.

### Locally

With the stack's Postgres on `localhost:5432` (the default `DATABASE_URL`),
after `npm run build` at the repository root:

```sh
npm run -s cli -w @payments-kafka/reconciliation -- balances
npm run -s cli -w @payments-kafka/reconciliation -- check
npm run -s cli -w @payments-kafka/reconciliation -- reset-state --yes
```

`-s` keeps npm's own banner off stdout.

## Replay procedure

Because the effects depend only on the **set** of distinct events, rebuilding
from the topic reproduces the same balances (scripted by
`add-validation-experiments`):

```sh
docker compose run --rm --no-deps -T reconciliation node services/reconciliation/dist/cli.js balances > before.json
docker compose stop reconciliation                       # 1. stop every consumer of the group
docker compose run --rm --no-deps -T reconciliation \
  node services/reconciliation/dist/cli.js reset-state --yes   # 2. clear state
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --group reconciliation \
  --reset-offsets --to-earliest --topic pix.payments.v1 --execute   # 3. rewind (group must be inactive)
docker compose --profile apps up -d reconciliation      # 4. consume again
# wait until the lag is 0, then
docker compose run --rm --no-deps -T reconciliation node services/reconciliation/dist/cli.js balances > after.json
cmp before.json after.json
```

**An offset reset alone is a no-op.** Without step 2 every replayed event is
already in `processed_events`, so each one is ignored as a duplicate (logged
as `duplicate event ignored`) and nothing changes. That is the idempotency
working as intended.

The retry topic offsets need no reset: records already on it for this group
are either duplicates or are applied again in the right order.

## Running

Configuration (see `docs/architecture.md`): `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_GROUP_ID` (default `reconciliation`), `DATABASE_URL` (default
`postgres://pix:pix@localhost:5432/reconciliation`), `RETRY_MAX_ATTEMPTS`,
`RETRY_BACKOFF_MS`, `LOG_LEVEL`.

```sh
docker compose --profile apps up -d --build reconciliation
docker compose --profile apps up -d --scale reconciliation=3   # several instances

# or locally against the stack's Kafka and Postgres
npm run build && npm start -w @payments-kafka/reconciliation
```

Logs are JSON lines on stdout. Per event: `payment event applied` (with
`status` and `ledger_entry`) or `duplicate event ignored`; gaps and conflicts
appear as the runtime's `handler failed; record sent to retry topic` and
`record sent to DLQ` with the reason in `err`.

The image follows the
[runtime's Dockerfile pattern](../../packages/consumer-runtime/README.md#dockerfile-pattern)
(build context: repository root): `docker build -f services/reconciliation/Dockerfile .`

## Testing

Requires Docker: Kafka (through the runtime's `/testing` helpers) and
Postgres (`postgres:18.6`, through `@testcontainers/postgresql`) run in
Testcontainers on random ports; one of each per run, each test isolates
itself with its own topics, consumer group and database.

```sh
npm test                                               # repository root: build + every workspace
npm test -w @payments-kafka/reconciliation             # this service (build the runtime first)
npm run test:unit -w @payments-kafka/reconciliation
npm run test:integration -w @payments-kafka/reconciliation
```

| File | Proves |
| --- | --- |
| `test/unit/decision.test.ts` | every lifecycle step, duplicate, gap and conflict case of the decision |
| `test/unit/handler.test.ts` | gap → retryable `OutOfOrderError`, conflict → `NonRetryableError` |
| `test/integration/migrate.test.ts` | tables created, migrations re-runnable and safe when instances start together |
| `test/integration/apply.test.ts` | lifecycle, balance rules, ledger, stream applied twice equals once, failure before `COMMIT` leaves nothing, concurrent transactions on one merchant/payment |
| `test/integration/kafka.test.ts` | lifecycle through Kafka, confirmation before creation via the retry topic, orphan to the DLQ, conflict to the DLQ |
| `test/integration/redelivery.test.ts` | consumer process SIGKILLed after the DB commit and before the offset commit (counted once), and inside the DB transaction (applied in full on redelivery) |
| `test/integration/multi-instance.test.ts` | instances joining and leaving during a stream with duplicates and out-of-order events: same state as a single instance and as the expected balances, `check` passes, nothing in the DLQ |
| `test/integration/commands.test.ts` | `check` (consistent, tampered balance and counters), `balances` (sorting and byte-stable output), `reset-state` (refusal without `--yes`), exit codes, the CLI as a real process |
| `test/integration/rebuild.test.ts` | export, offset reset alone is a no-op, reset, replay from earliest with a fresh group, identical export |

## Code map

| File | Role |
| --- | --- |
| `src/main.ts` | Service entry point: config, pool, migrations, runtime |
| `src/service.ts` | `createReconciliationRuntime`: the consumer runtime wiring |
| `src/handler.ts` | Handler and the mapping of results to runtime outcomes |
| `src/apply.ts` | The single-transaction apply |
| `src/decision.ts` | Sequence and transition rules (pure) |
| `src/commands.ts` | Queries behind `check`, `balances`, `reset-state` |
| `src/run-cli.ts`, `src/cli.ts` | CLI logic and entry point |
| `src/migrate.ts`, `migrations/` | Migration runner and SQL |
