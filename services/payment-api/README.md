# payment-api

Spring Boot service (Java 21) exposing a minimal HTTP API for the lifecycle of
a simulated Pix payment: create, confirm, expire, refund.

Every state change writes the `payments` row **and** an `outbox` row in the
same database transaction (the transactional outbox pattern). The service has
no Kafka dependency at all: Debezium reads the `outbox` table from the
Postgres WAL and publishes each row to `pix.payments.v1`. Payments keep being
accepted while Kafka is down; their events are published once Debezium
catches up.

Shared conventions (HTTP contract, payment representation, outbox DDL, event
contract, ports, environment variables) are defined in
[`docs/architecture.md`](../../docs/architecture.md); the event schema is
[`contracts/payment-event.v1.schema.json`](../../contracts/payment-event.v1.schema.json).

## How a request flows

```
POST /payments/{id}/confirm
  └─ one transaction
       SELECT ... FROM payments WHERE id = ? FOR UPDATE   -- serializes concurrent transitions
       validate transition (CREATED -> CONFIRMED)         -- else 409, nothing written
       UPDATE payments SET status, sequence, updated_at
       INSERT INTO outbox (id = event_id, ..., payload = full payment event)
     COMMIT
```

Lifecycle rules live in `PaymentTransition`: `confirm` and `expire` only from
`CREATED`, `refund` only from `CONFIRMED`; each event carries `sequence`
1 (created), 2 (confirmed/expired) or 3 (refunded).

## Endpoints

| Method and path | Success | Errors |
| --- | --- | --- |
| `POST /payments` | `201` payment (+ `Location`) | `400` invalid body |
| `POST /payments/{id}/confirm` | `200` payment | `404`, `409` invalid transition |
| `POST /payments/{id}/expire` | `200` payment | `404`, `409` |
| `POST /payments/{id}/refund` | `200` payment | `404`, `409` |
| `GET /payments/{id}` | `200` payment | `404` |
| `GET /actuator/health` | `200` | |

Errors are [problem details](https://www.rfc-editor.org/rfc/rfc9457)
(`application/problem+json`). A `400` lists the failing fields in `errors`; a
`409` carries `current_status` and `transition`. `amount_cents` must be a
positive JSON integer: `45.5` and `"4590"` are rejected. An `{id}` that is not
a UUID is reported as `404`.

### curl examples

Against a running instance on the default port:

```bash
# Create a payment
curl -s -X POST http://localhost:8081/payments \
  -H 'Content-Type: application/json' \
  -d '{"merchant_id": "merchant-007", "amount_cents": 4590}'
# {"payment_id":"0b9d6c0e-...","merchant_id":"merchant-007","amount_cents":4590,"currency":"BRL",
#  "status":"CREATED","sequence":1,"created_at":"...","updated_at":"..."}

PAYMENT_ID=<payment_id from the response>

# Move it through the lifecycle
curl -s -X POST http://localhost:8081/payments/$PAYMENT_ID/confirm   # status CONFIRMED, sequence 2
curl -s -X POST http://localhost:8081/payments/$PAYMENT_ID/refund    # status REFUNDED, sequence 3
# or, instead of confirm:
curl -s -X POST http://localhost:8081/payments/$PAYMENT_ID/expire    # status EXPIRED, sequence 2

# Read it
curl -s http://localhost:8081/payments/$PAYMENT_ID

# An invalid transition is a 409 problem
curl -s -X POST http://localhost:8081/payments/$PAYMENT_ID/confirm
# {"type":"about:blank","title":"Invalid transition","status":409,
#  "detail":"Cannot confirm a payment in status REFUNDED", ...}

# Health
curl -s http://localhost:8081/actuator/health
# {"status":"UP"}
```

Inspect the outbox (inside the Compose stack):

```bash
docker compose exec postgres psql -U pix -d payments \
  -c "SELECT event_type, merchant_id, payload FROM outbox ORDER BY created_at DESC LIMIT 5;"
```

## Configuration

Defaults are in `src/main/resources/application.yaml`; environment variables
override them through Spring's relaxed binding.

| Variable | Default (local run) | Value in Compose |
| --- | --- | --- |
| `SERVER_PORT` | `8081` | `8081` |
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/payments` | `jdbc:postgresql://postgres:5432/payments` |
| `SPRING_DATASOURCE_USERNAME` | `pix` | `pix` |
| `SPRING_DATASOURCE_PASSWORD` | `pix` | `pix` |

Flyway creates `payments` and `outbox` on startup
(`src/main/resources/db/migration`). Only `/actuator/health` is exposed.

## Running

Requires JDK 21 or newer (the build targets Java 21 with
`maven.compiler.release`). Use the Maven wrapper (`mvnw.cmd` on Windows).

```bash
cd services/payment-api

# Unit and integration tests (needs Docker for Testcontainers; no fixed ports used)
./mvnw verify

# Run locally against a Postgres on localhost:5432 (e.g. the Compose one)
./mvnw spring-boot:run
```

Container image (build context is this directory; tests are skipped inside the
image build because they need Docker and the repository's `contracts/`):

```bash
docker build -t payment-api services/payment-api        # from the repository root
docker compose --profile apps up -d --build payment-api  # as part of the stack
```

## Tests

| Test | What it proves |
| --- | --- |
| `PaymentTransitionTest` | every valid and invalid lifecycle transition (unit) |
| `OutboxSchemaTest` | `outbox` columns and types match the DDL in `docs/architecture.md` |
| `PaymentServiceTest` | payment and outbox rows commit together; invalid transitions write nothing |
| `OutboxFailureRollbackTest` | a failing outbox insert rolls back the payment change (service and HTTP `500`) |
| `OutboxPayloadContractTest` | created/confirmed/expired/refunded payloads validate against the shared JSON Schema, read from `../../contracts` |
| `PaymentControllerTest` | HTTP success paths, representation, `400`/`404`/`409` problem details |
| `ConcurrentTransitionTest` | 8 simultaneous confirms: exactly one `200`, one `payment.confirmed` row |
| `NoKafkaDependencyTest` | no Kafka client class or package on the classpath |

Integration tests start `postgres:17-alpine` with Testcontainers on a random port.

## Known limitations

- **The outbox table grows forever.** Rows are inserted and never deleted:
  Debezium only needs the insert in the WAL, and keeping rows makes the pattern
  easy to inspect. A production system would delete published rows (or delete
  right after insert in the same transaction) or partition the table. A cleanup
  job is out of scope for this study project.
- No authentication and no HTTP idempotency keys: retrying `POST /payments`
  creates a second payment. Refunds are always for the full amount.
