# Design

## Context

Topic, key, event contract and the payment API HTTP contract are fixed in
`docs/architecture.md` and `contracts/`. The API is built in parallel
(`add-payment-api-outbox`), so normal mode is developed against the documented
contract and tested with a stubbed HTTP server.

## Goals / Non-Goals

**Goals:**
- Deterministic, configurable traffic: same seed, same sequence of decisions.
- Faults that are countable, so experiments can assert on them.

**Non-Goals:**
- Realistic Pix payloads (keys, QR codes, end-to-end ids).
- Load testing beyond a few hundred events per second.

## Decisions

- **One generator, two sinks.** A lifecycle generator decides what happens
  next (create, confirm, expire, refund) and hands it to a `PaymentSink`:
  `KafkaSink` (chaos) or `ApiSink` (normal). Mode only selects the sink, so
  both modes share lifecycle logic and tests.
- **Rate control by a scheduled ticker** emitting `SIMULATOR_RATE_PER_SECOND`
  lifecycle steps per second; pending payments wait in an in-memory queue until
  their next step is due.
- **Chaos producer settings: `acks=all`, idempotence enabled, key =
  `merchant_id`.** Duplicates are injected on purpose at the application level
  (same `event_id` sent twice), never by sloppy producer settings.
- **Fault injection is chaos-only and probability-driven**, each fault with its
  own rate and its own counter:
  - duplicate: resend an already sent event unchanged;
  - out of order: send a payment's later event before its earlier one;
  - invalid: send non-JSON bytes or an event that violates the schema.
- **Fraud scenarios work in both modes** because they are just traffic shapes:
  an outlier multiplies the amount; a burst emits many creates for one merchant
  at once; a refund spree refunds several confirmed payments of one merchant.
- **Seeded `Random`** (`SIMULATOR_SEED`) for reproducible runs; optional
  `SIMULATOR_MAX_PAYMENTS` to stop after N payments so experiments can compare
  exact totals.
- **Counters exposed via a small `GET /stats` endpoint and logged
  periodically** (sent per event type, duplicates, out-of-order, invalid, API
  errors). Experiments use these as the expected values.
- **Normal mode retries API calls on connection errors and 5xx** with backoff;
  a 409 is counted and dropped (it means a lost race, not a failure).

Configuration (environment variables, with defaults):

| Variable | Default | Meaning |
| --- | --- | --- |
| `SIMULATOR_MODE` | `chaos` | `chaos` or `normal` |
| `SIMULATOR_RATE_PER_SECOND` | `50` | lifecycle steps emitted per second |
| `SIMULATOR_MERCHANTS` | `20` | number of merchants (`merchant-001`...) |
| `SIMULATOR_MAX_PAYMENTS` | `0` | stop after N payments; `0` means unlimited |
| `SIMULATOR_SEED` | `42` | random seed |
| `SIMULATOR_EXPIRE_RATE` | `0.15` | share of created payments that expire |
| `SIMULATOR_REFUND_RATE` | `0.05` | share of confirmed payments refunded |
| `SIMULATOR_CHAOS_DUPLICATE_RATE` | `0.05` | chaos only |
| `SIMULATOR_CHAOS_OUT_OF_ORDER_RATE` | `0.02` | chaos only |
| `SIMULATOR_CHAOS_INVALID_RATE` | `0.01` | chaos only |
| `SIMULATOR_FRAUD_OUTLIER_RATE` | `0.002` | share of payments with an outlier amount |
| `SIMULATOR_FRAUD_BURST_RATE` | `0.001` | chance per tick of a merchant burst |
| `SIMULATOR_FRAUD_REFUND_SPREE_RATE` | `0.001` | chance per tick of a refund spree |

## Risks / Trade-offs

- [Normal mode cannot be verified end to end until the API merges] → stubbed
  HTTP tests now; the end-to-end run belongs to `add-validation-experiments`.
- [In-memory pending queue is lost on restart] → acceptable: unfinished
  payments simply stay `CREATED`, which is itself a realistic condition.
