# simulator

Spring Boot service that generates the lifecycle of Pix payments for many
merchants at a configurable rate. Shared conventions (topic, key, event
contract, payment API, ports) are in [docs/architecture.md](../../docs/architecture.md);
the design is in [openspec/changes/archive/2026-11-11-add-payment-simulator](../../openspec/changes/archive/2026-11-11-add-payment-simulator/).

Java 21, Spring Boot 3.5, Spring Kafka, Maven wrapper. HTTP port `8082`.

## What it generates

Each payment belongs to one of `SIMULATOR_MERCHANTS` merchants
(`merchant-001`, `merchant-002`, ...) and goes through:

```
payment.created (sequence 1) ─┬─► payment.confirmed (2) ──(SIMULATOR_REFUND_RATE)──► payment.refunded (3)
                               └─► payment.expired (2)     (SIMULATOR_EXPIRE_RATE)
```

A single-threaded ticker runs `SIMULATOR_RATE_PER_SECOND` **slots** per second.
Each slot performs one lifecycle step: a pending step that is due (confirm or
expire a few slots after creation, refund later), otherwise a new payment.
With `SIMULATOR_MAX_PAYMENTS` set, the simulator stops creating payments at the
limit, finishes the lifecycles in progress, logs its final stats and stays up
idle so `/stats` can still be read. If the sink falls behind (for example while
the API is down), at most one second of backlog is caught up; older backlog is
dropped.

All decisions come from a `Random` seeded with `SIMULATOR_SEED` and use
logical time (slots), so the same seed and configuration always produce the
same steps and the same counts per event type. Only `occurred_at`, and in
normal mode the API-assigned `payment_id`, differ between runs.

### Fraud scenarios (both modes)

| Scenario | Rate variable | Shape |
| --- | --- | --- |
| Amount outlier | `SIMULATOR_FRAUD_OUTLIER_RATE`, per created payment | amount is 10x to 30x the merchant's typical amount (an ordinary payment is 0.5x to 1.5x) |
| Burst | `SIMULATOR_FRAUD_BURST_RATE`, per slot | 10 to 20 payments created for one merchant in the same slot (on top of the rate, never beyond the payment limit) |
| Refund spree | `SIMULATOR_FRAUD_REFUND_SPREE_RATE`, per slot | 3 to 5 confirmed payments of one merchant refunded in the same slot |

## Modes

### `chaos` (default): directly to Kafka

Publishes each event to `pix.payments.v1` with key `merchant_id` and a JSON
value that follows `contracts/payment-event.v1.schema.json`. The producer uses
`acks=all` and idempotence; faults are injected on purpose at the application
level, each with its own rate and counter:

| Fault | Rate variable | What is published |
| --- | --- | --- |
| Duplicate | `SIMULATOR_CHAOS_DUPLICATE_RATE`, per step | one of the last 100 published events again, byte for byte (same `event_id`, same key) |
| Out of order | `SIMULATOR_CHAOS_OUT_OF_ORDER_RATE`, per created payment | `payment.created` is held back and published right after that payment's `sequence` 2 event (same key, so same partition) |
| Invalid | `SIMULATOR_CHAOS_INVALID_RATE`, per step | an **extra** record keyed by a real merchant: either not JSON (truncated or plain text) or a schema violation (missing `event_id`, string `amount_cents`, wrong `sequence`, unknown `event_type`, `currency` `USD`) |

Invalid records never replace lifecycle events, so the topic holds exactly
`events_sent_total + chaos.duplicates + chaos.invalid` records.

### `normal`: through the payment API

Performs every step over HTTP, as documented in docs/architecture.md:
`POST /payments` with `{"merchant_id", "amount_cents"}`, then
`POST /payments/{id}/confirm|expire|refund` using the `payment_id` the API
returned. No Kafka producer is created in this mode. Events reach Kafka
through the API's outbox and Debezium.

- Connection errors and `5xx`: retried with exponential backoff (200 ms
  doubling up to 5 s) until the API answers; the run pauses meanwhile.
- `409`: a lost race. It is counted in `api.conflicts` and dropped.
- Any other `4xx` is counted in `api.errors` and dropped. Later steps of a
  payment whose creation was dropped are skipped (`api.skipped`).

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `SIMULATOR_MODE` | `chaos` | `chaos` or `normal` |
| `SIMULATOR_RATE_PER_SECOND` | `50` | lifecycle steps (slots) per second, 1 to 10000 |
| `SIMULATOR_MERCHANTS` | `20` | number of merchants (`merchant-001`...), 1 to 999 |
| `SIMULATOR_MAX_PAYMENTS` | `0` | stop after N payments; `0` means unlimited |
| `SIMULATOR_SEED` | `42` | random seed |
| `SIMULATOR_EXPIRE_RATE` | `0.15` | share of created payments that expire |
| `SIMULATOR_REFUND_RATE` | `0.05` | share of confirmed payments refunded |
| `SIMULATOR_CHAOS_DUPLICATE_RATE` | `0.05` | chaos only |
| `SIMULATOR_CHAOS_OUT_OF_ORDER_RATE` | `0.02` | chaos only |
| `SIMULATOR_CHAOS_INVALID_RATE` | `0.01` | chaos only |
| `SIMULATOR_FRAUD_OUTLIER_RATE` | `0.002` | share of payments with an outlier amount |
| `SIMULATOR_FRAUD_BURST_RATE` | `0.001` | chance per slot of a merchant burst |
| `SIMULATOR_FRAUD_REFUND_SPREE_RATE` | `0.001` | chance per slot of a refund spree |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | chaos mode only (`kafka:29092` in Compose) |
| `PAYMENT_API_BASE_URL` | `http://localhost:8081` | normal mode only (`http://payment-api:8081` in Compose) |
| `SERVER_PORT` | `8082` | HTTP port for `/stats` and `/actuator/health` |

Every rate must be between 0 and 1. The application refuses to start on an
out-of-range or unknown value.

Advanced settings without a dedicated variable, which can be set as Spring
properties (for example `--simulator.stats-log-interval=30s`):
`simulator.kafka.topic` (`pix.payments.v1`), `simulator.stats-log-interval`
(`10s`), `simulator.payment-api.initial-backoff` (`200ms`),
`simulator.payment-api.max-backoff` (`5s`) and
`simulator.payment-api.request-timeout` (`5s`).

## Statistics

`GET /stats` returns every counter. The stats are also logged every 10 seconds
and once more when the run finishes or stops (`simulator stats {...}`,
`simulator finished: payment limit reached {...}`). A counter only moves once
Kafka acknowledged the record (chaos) or the API answered `2xx` (normal), so
experiments can use the counters as expected values.

```json
{
  "mode": "chaos",
  "running": false,
  "finished": true,
  "steps": 1180,
  "events_sent": {
    "payment.created": 500,
    "payment.confirmed": 428,
    "payment.expired": 72,
    "payment.refunded": 22
  },
  "events_sent_total": 1022,
  "chaos": { "duplicates": 51, "out_of_order": 9, "invalid": 10, "invalid_non_json": 3, "invalid_schema": 7, "send_errors": 0 },
  "api": { "retries": 0, "conflicts": 0, "errors": 0, "skipped": 0 },
  "fraud": { "outliers": 1, "bursts": 1, "burst_payments": 14, "refund_sprees": 1, "refund_spree_refunds": 4 }
}
```

| Counter | Meaning |
| --- | --- |
| `events_sent.<type>` | distinct lifecycle events delivered (duplicates and invalid records are not included) |
| `chaos.duplicates` | extra copies of already published events |
| `chaos.out_of_order` | payments whose `payment.created` was published after their `sequence` 2 event |
| `chaos.invalid` | invalid records (`invalid_non_json` + `invalid_schema`) |
| `chaos.send_errors` | records Kafka did not acknowledge (not counted anywhere else) |
| `api.retries` | failed attempts (connection error or `5xx`) that were retried |
| `api.conflicts` | `409` answers, dropped |
| `api.errors` | other non-`2xx` answers, dropped |
| `api.skipped` | steps skipped because the payment's creation was dropped |
| `fraud.*` | outliers, bursts (and payments in them), refund sprees (and their refunds) |
| `steps` | slots run by the ticker |

`GET /actuator/health` answers `200` on the same port, so the Compose
healthcheck (`curl -fsS http://localhost:8082/actuator/health`) works. The
runtime image (`eclipse-temurin:21-jre`) includes `curl`.

## Running

The examples run against the local stack (`docker compose up -d` at the
repository root) with the default host ports. Run them from `services/simulator`.

A clean run: normal mode through the payment API, no chaos (the chaos rates are
ignored in this mode), and an exact number of payments:

```bash
SIMULATOR_MODE=normal SIMULATOR_MAX_PAYMENTS=1000 SIMULATOR_RATE_PER_SECOND=100 ./mvnw spring-boot:run
curl -s http://localhost:8082/stats
```

A chaos run: straight to Kafka with heavier fault injection:

```bash
SIMULATOR_MODE=chaos SIMULATOR_MAX_PAYMENTS=5000 \
SIMULATOR_CHAOS_DUPLICATE_RATE=0.1 SIMULATOR_CHAOS_OUT_OF_ORDER_RATE=0.05 SIMULATOR_CHAOS_INVALID_RATE=0.02 \
./mvnw spring-boot:run
```

On Windows PowerShell, set the variables first:

```powershell
$env:SIMULATOR_MODE = "chaos"; $env:SIMULATOR_MAX_PAYMENTS = "5000"
.\mvnw.cmd spring-boot:run
```

The same with Docker, on the Compose network:

```bash
docker build -t payments-kafka-simulator .
docker run --rm --network payments-kafka_default -p 8082:8082 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e SIMULATOR_MAX_PAYMENTS=5000 \
  payments-kafka-simulator
```

Inside Compose the service is started by `docker compose --profile apps up -d --build`.

## Tests

```bash
./mvnw verify          # Linux/macOS
.\mvnw.cmd verify      # Windows
```

Docker must be running. The Kafka tests start their own broker with
Testcontainers on random ports, and normal mode is tested against an
in-process stub of the payment API, so the Compose stack is not needed. Tests
read the shared contract from `../../contracts`. The image build skips tests
and does not need that directory.

| Test | Covers |
| --- | --- |
| `SimulatorPropertiesTest` | defaults, environment overrides, rejection of out-of-range values |
| `LifecycleGeneratorTest` | lifecycle shape, rates, reproducibility per seed, payment limit |
| `SimulationRunnerTest` | rate with a fake clock, backlog cap, payment limit, a 10 s real-time run, periodic stats log |
| `FraudScenariosTest` | each scenario present at rate 1 and absent at rate 0 |
| `PaymentEventContractTest` | serialized events validate against the contract |
| `KafkaSinkTest` | keyed and valid records; topic contents equal the counters for each fault (Testcontainers) |
| `ApiSinkTest` | API calls, retry with backoff on 5xx and connection errors, 409, other 4xx |
| `NormalModeApplicationTest` | no Kafka producer in normal mode; `/stats` and health |
| `ChaosModeApplicationTest` | full application: `/stats` equals what is on the topic |
