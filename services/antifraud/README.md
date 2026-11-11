# antifraud

Stateful stream consumer of the pipeline. It reads `pix.payments.v1` with
consumer group `antifraud`, applies three per-merchant rules, and publishes a
fraud alert to `pix.fraud-alerts.v1` whenever one fires. It is the only
consumer that also produces, which makes it the place to study in-memory
state, rebalances, replay and consume-then-produce under at-least-once
delivery.

Consumption, contract validation, retry/DLQ, commits and graceful shutdown
come from [`@payments-kafka/consumer-runtime`](../../packages/consumer-runtime/README.md).
Topics, the alert contract and the shared environment variables are defined
in [`docs/architecture.md`](../../docs/architecture.md).

## Rules

All rules are evaluated per merchant, on **event time** (`occurred_at`), never
on the wall clock, so reprocessing the same ordered stream yields the same
alerts. Every alert has `alert_id = <rule>:<triggered_by_event_id>`, is keyed
by `merchant_id` and validates against
[`contracts/fraud-alert.v1.schema.json`](../../contracts/fraud-alert.v1.schema.json).

| Rule | Looks at | Fires when | `details` |
| --- | --- | --- | --- |
| `amount_outlier` | `payment.created` | amount > `ANTIFRAUD_OUTLIER_FACTOR` x the merchant's running mean, once the mean has at least `ANTIFRAUD_OUTLIER_MIN_SAMPLES` payments. The outlier is **not** added to the mean. | `amount_cents`, `merchant_mean_cents`, `factor`, `samples` |
| `payment_burst` | `payment.created` | more than `ANTIFRAUD_BURST_LIMIT` payments in a sliding window of `ANTIFRAUD_BURST_WINDOW_MS` | `count`, `limit`, `window_ms` |
| `refund_excess` | `payment.refunded` | more than `ANTIFRAUD_REFUND_LIMIT` refunds in a sliding window of `ANTIFRAUD_REFUND_WINDOW_MS` | `count`, `limit`, `window_ms` |

`payment_burst` and `refund_excess` fire **once per breach**: the event that
takes the window count above the limit triggers the alert; the rule then
stays quiet until the count is back at the limit or below, and fires again on
the next breach. The window ends at the newest event time seen for the
merchant; a late (retried) event is placed in order, and an event older than
the whole window is ignored by the window rules.

`payment.confirmed` and `payment.expired` do not feed any rule.

## Configuration

Shared variables (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`,
`RETRY_MAX_ATTEMPTS`, `RETRY_BACKOFF_MS`, `LOG_LEVEL`) are described in
`docs/architecture.md`. Thresholds:

| Variable | Default | Valid values |
| --- | --- | --- |
| `ANTIFRAUD_OUTLIER_FACTOR` | `5` | number >= 1 |
| `ANTIFRAUD_OUTLIER_MIN_SAMPLES` | `20` | integer >= 1 |
| `ANTIFRAUD_BURST_LIMIT` | `30` | integer >= 1 |
| `ANTIFRAUD_BURST_WINDOW_MS` | `10000` | integer >= 1 |
| `ANTIFRAUD_REFUND_LIMIT` | `5` | integer >= 1 |
| `ANTIFRAUD_REFUND_WINDOW_MS` | `60000` | integer >= 1 |
| `ANTIFRAUD_SEEN_EVENT_IDS_PER_MERCHANT` | `10000` | integer >= 1 |

The defaults are tuned to the simulator's fraud scenarios; keep them unless an
experiment says otherwise. An unset variable takes its default; an invalid
value stops the service at startup with a message naming the variable. The
effective thresholds are logged at startup (`antifraud thresholds loaded`).

## State: in memory, and what that costs

The topic is keyed by `merchant_id`, so the instance that owns a partition
sees every event of its merchants, in order. Each merchant has its running
mean, the two sliding windows and a bounded set of recently applied
`event_id`s, all in memory. There is no database.

- **State follows partition ownership.** When partitions are revoked the
  service drops the state of the merchants whose events come from them and
  logs `merchant state dropped for revoked partitions` (with
  `merchants_dropped`); newly assigned partitions start empty. The group uses
  the eager protocol, so every rebalance revokes everything: **any**
  rebalance (scaling, a restart, a crashed member) resets all state.
- **Warm-up after a rebalance or restart.** Until enough events arrive again,
  rules can miss fraud: `amount_outlier` needs `ANTIFRAUD_OUTLIER_MIN_SAMPLES`
  fresh payments, and a burst that straddles the rebalance is counted only
  from the moment the new owner starts. This is the accepted trade-off of the
  design (persisting state is the "real" answer); you can observe it by
  scaling the service in the middle of a simulated burst.
- **Replay rebuilds everything.** Resetting the group's offsets to the
  earliest position and reprocessing the topic rebuilds the same state and
  re-publishes alerts with the same `alert_id`s, which downstream consumers
  dedupe.

## Delivery: consume-then-produce without transactions

- Alerts are published with the runtime's producer (idempotent, `acks=all`)
  and **awaited inside the handler**: the offset is committed only after every
  alert of that event is durable.
- A failed publish throws, so the event follows the normal retry path
  (`pix.payments.retry.v1`, then the DLQ after `RETRY_MAX_ATTEMPTS`).
- **Duplicates do not double count.** An event whose `event_id` was already
  applied for the merchant does not touch the rules again; the alerts it
  triggered the first time are published again with the same `alert_id`.
  That is what makes a retry after a failed publish deliver the alert, and a
  crash between publish and commit at worst re-emits the same alert. The set
  is LRU-bounded by `ANTIFRAUD_SEEN_EVENT_IDS_PER_MERCHANT`; a duplicate older
  than that horizon counts again (bounded memory is preferred).
- Retried events arrive late and out of order; rules use event time and the
  seen-id set, so a late event can shift a window slightly. Accepted.

Every alert published is logged at `info` as `fraud alert published` with
`alert_id`, `rule`, `merchant_id` and `details`.

## Code layout

| File | Purpose |
| --- | --- |
| `src/config.ts` | `ANTIFRAUD_*` thresholds and their defaults |
| `src/rules.ts` | Merchant state and the three rules, deterministic and free of I/O |
| `src/service.ts` | Handler (evaluate, publish alerts) and the partition hooks that drop state |
| `src/main.ts` | Process wiring: config, logger, consumer runtime |

## Run

Inside the full stack (from the repository root; Kafka and the topics come
from Compose, see `docs/architecture.md`):

```sh
docker compose --profile apps up -d --build antifraud
docker compose logs -f antifraud
docker compose --profile apps up -d --scale antifraud=2   # observe a rebalance and the warm-up
```

Locally against the Compose Kafka on `localhost:9092`:

```sh
npm install
npm run build -w @payments-kafka/consumer-runtime -w @payments-kafka/antifraud
npm start -w @payments-kafka/antifraud
```

Build the image on its own (build context is the repository root):

```sh
docker build -f services/antifraud/Dockerfile .
```

## Test

From the repository root. Integration tests need Docker: they start their own
Kafka with Testcontainers on a random port and never use the Compose stack.

```sh
npm run build -w @payments-kafka/consumer-runtime        # the runtime resolves to dist/
npm test -w @payments-kafka/antifraud                    # unit + integration
npm run test:unit -w @payments-kafka/antifraud           # rules and config, no Docker
npm run test:integration -w @payments-kafka/antifraud    # Kafka in Testcontainers
```

- `test/unit/`: thresholds and invalid values; each rule (warm-up, firing,
  once per breach, re-arming, normal pace, per-merchant isolation); a stream
  delivered twice and a replay from empty state yield the same `alert_id`s.
- `test/integration/`: a burst of 40 payments yields exactly one schema-valid
  alert with the expected `alert_id` and key; two instances in one group split
  the partitions and drop state on revocation; a failed alert publish sends
  the event to the retry topic and the alert is published on retry.
