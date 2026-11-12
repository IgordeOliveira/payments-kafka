# Experiment 5: load on the payment API, every accepted request reaches the end

```sh
docker compose --profile experiments run --rm experiments load
```

**Property.** Under concurrent load the API may slow down or reject requests,
but it never loses or duplicates one it accepted. Each state change writes the
`payments` row **and** an `outbox` row in one database transaction, so a `2xx`
answer means exactly one outbox row was committed and a failure before the
transaction began wrote nothing. Debezium then carries every outbox row to
`pix.payments.v1`, and the consumers apply it once. This experiment puts that
chain under real concurrency and also measures the API itself: request rate,
p95 latency and error rate.

**How it breaks things on purpose.** It does not stop anything. It applies
load: a [k6](https://k6.io) script ramps up to 200 iterations per second,
holds that plateau for two minutes and ramps down. An iteration creates a
payment, then confirms it (80 %) or expires it (20 %), and refunds about 10 %
of the confirmed ones. That is about 2.1 requests per iteration, so 200
iterations/s is about 420 requests/s against the API.

This experiment is **not part of `experiments all`**: its numbers depend on
the machine, and it is the longest run (about 4 minutes).

## What the script does

1. Resets the pipeline and picks a `RUN_ID` (a UTC timestamp). Every merchant
   of the run is named `load-<RUN_ID>-NN` (`NN` from 1 to `MERCHANTS`), and
   every query on the API database and the outbox filters on that prefix, so
   older data in the API database does not matter.
2. Runs k6 from the `grafana/k6:2.3.0` image as a sibling container on the
   Compose network `payments-kafka_default`. The script
   (`experiments/k6/load.js`) is fed on stdin, so there is no bind mount. The
   image is pulled on the first run. k6's progress goes to your terminal; its
   summary is captured as JSON.
3. When k6 ends, notes the time and waits until the distinct `event_id`s on the
   topic equal the outbox rows (Debezium caught up), then until the three
   groups are drained. Both waits are timed.
4. Compares the sources below and prints `PASS` or `FAIL`.

| Assertion | Sources compared |
| --- | --- |
| `http_req_failed` < 1 % | k6 threshold |
| p95 of `http_req_duration` < 500 ms | k6 threshold |
| outbox rows = successful state changes | outbox count vs the sum of k6's `created`, `confirmed`, `expired`, `refunded` counters |
| outbox rows = events implied by the payment states | outbox count vs the API's `payments` (CREATED 1 event, CONFIRMED or EXPIRED 2, REFUNDED 3); independent of k6 |
| distinct `event_id`s on the topic = outbox ids | the two sorted id lists, compared byte for byte |
| groups drained, each event applied once | lag, `processed_events` = outbox rows |
| reconciliation holds every API payment with the same status and amount | `payments` of the API database vs reconciliation's `payments` |
| `check` passes, nothing dead-lettered | `check`, DLQ |

Only measured, never asserted: requests per second, p95 latency, error rate,
the time Debezium needs to catch up after the load ends, and the time the
groups need to drain.

Variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `RATE` | 200 | target iterations per second at the plateau |
| `RAMP` | 30s | time to ramp up, and again to ramp down |
| `DURATION` | 2m | length of the plateau |
| `MERCHANTS` | 50 | number of merchants (`load-<RUN_ID>-01` ...) |
| `CONFIRM_RATIO` | 0.8 | share of payments confirmed; the rest expire |
| `REFUND_RATIO` | 0.1 | share of confirmed payments that are refunded |
| `CATCHUP_TIMEOUT` | 600 | seconds to wait for Debezium to catch up |
| `DRAIN_TIMEOUT` | 600 | seconds to wait for the groups to drain |
| `SABOTAGE` | 0 | `1` locks the outbox table in bursts while k6 runs (see below) |
| `SABOTAGE_LOCK_SECONDS` | 1 | how long each sabotage lock is held |
| `SABOTAGE_PAUSE_SECONDS` | 1 | pause between two sabotage locks |

Set them with `-e`, for example `-e RATE=100 -e DURATION=1m`.

## Measured (clean stack, defaults)

On a desktop with Docker Desktop (WSL 2), defaults. The API was far from
saturated. The lag of the groups stayed small, and the pipeline drained
7 seconds after the last request:

```text
load: measured values
  k6 HTTP requests / requests per second           62414 / 346.74
  k6 error rate (http_req_failed)                  0
  k6 latency ms: avg / p95 / p99                   1.69 / 2.56 / 3.6
  k6 transitions: created / confirmed / expired / refunded 29999 / 23981 / 6018 / 2416
  outbox rows of the run                           62414
  events implied by the API payment states         62414
  distinct event_ids on pix.payments.v1            62414
  payments in API database / reconciliation        29999 / 29999
  reconciliation processed_events                  62414
  seconds after load end until the topic caught up with the outbox 1
  seconds after load end until every group had zero lag 7
  check report                                     {"ok":true,"merchants":50,"ledger_entries":26397,"processed_events":62414,"mismatches":0}

load: assertions
  PASS  k6 threshold http_req_failed rate<0.01 passed (error rate 0) (expected true, measured true)
  PASS  k6 threshold http_req_duration p(95)<500 passed (p95 2.56 ms) (expected true, measured true)
  PASS  outbox rows = successful state changes reported by k6 (expected 62414, measured 62414)
  PASS  outbox rows = events implied by the API payment states (expected 62414, measured 62414)
  PASS  Debezium caught up after the load (expected 0, measured 0)
  PASS  distinct event_ids on the topic = outbox ids (identical (2309318 bytes, ...))
  PASS  consumer groups drained after the load (expected 0, measured 0)
  PASS  every distinct event applied once (processed_events = outbox rows) (expected 62414, measured 62414)
  PASS  reconciliation holds every API payment with the same status and amount (identical (...))
  PASS  reconciliation check passes (exit code) (expected 0, measured 0)
  PASS  nothing dead-lettered (expected 0, measured 0)

PASS load
```

The requests per second (~347) are below the 420 target because of the ramps:
the average covers the ramp up and the ramp down too.

## Tuning for your machine

The thresholds are the only part that depends on your hardware. If the two k6
threshold assertions fail on a slow machine (p95 above 500 ms, or more than 1 %
of requests failing) while every consistency assertion passes, the machine is
saturated, not the pipeline broken. Lower the rate:

```sh
docker compose --profile experiments run --rm -e RATE=100 experiments load
```

Halve `RATE` until the thresholds pass; `DURATION=1m` shortens the run. Close
other heavy programs and check that Docker has the documented 6 GB. You can
also raise the rate to find where your machine starts to fail, but finding the
breaking point is not what this experiment is for. Because the result changes
from machine to machine, `load` is not in `experiments all`.

## What to watch while it runs

- **Topics → pix.payments.v1 → Consumers** in Kafka UI (http://localhost:8080):
  the lag of the three groups during the plateau. If the API and Debezium
  produce faster than the consumers apply, it grows, then drains after the
  load ends; the time that takes is the drain time the script prints. Raise
  `RATE` to see the backlog build up.
- **Kafka Connect → payments-outbox** stays `RUNNING`; Debezium is the stage to
  watch when the topic falls behind the outbox.
- **Topics → pix.payments.v1 → Messages**: the messages are spread over the six
  partitions by merchant (key `merchant_id`), because the 50 merchants hash to
  different partitions.
- The outbox grows steadily (from the host):
  `docker compose exec postgres psql -U pix -d payments -c "SELECT count(*) FROM outbox"`.
- `docker stats`: CPU and memory of `payment-api`, `postgres` and
  `kafka-connect`, to see which one saturates first.
- `docker compose logs -f reconciliation`: the consumer applying the backlog.

## Seeing it fail

```sh
docker compose --profile experiments run --rm -e SABOTAGE=1 experiments load
```

While k6 runs, the sabotage takes an `EXCLUSIVE` lock on the `outbox` table
for one second, releases it for one second, and repeats, from a background
`psql` session. Every state change inserts an outbox row, so every API write
that arrives during a lock waits until the lock is released. Reads are not
blocked, so Debezium keeps streaming. About half of the requests wait up to a
second. p95 goes past 500 ms, so the latency threshold prints `FAIL` and the
experiment ends with `FAIL load` and exit status 1. Measured with the
defaults: p95 812 ms, p99 972 ms, error rate 0.

The consistency assertions still `PASS`. That is the lesson: contention in the
database slows the API down, but it never loses or duplicates an accepted
request, because the payment row and the outbox row commit in one
all-or-nothing transaction. Every request k6 saw succeed has its outbox row,
topic record and reconciliation entry.

The lock loop stops when k6 ends, and an exit trap also stops it if the script
fails, so no lock outlives the run. To verify it:
`docker compose exec postgres psql -U pix -d payments -c "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE c.relname = 'outbox' AND l.mode = 'ExclusiveLock'"`
prints 0.
