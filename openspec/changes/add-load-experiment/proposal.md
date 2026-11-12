# Proposal

## Why

The four existing experiments prove correctness at low, fixed rates. None of
them puts real concurrent load on the payment API: only `kafka-outage` uses
the outbox path, at 25 steps/s, and the simulator's single-threaded ticker
sends its HTTP calls one at a time, so its throughput is capped by the API's
latency. Nothing measures capacity: request latency and error rate under load,
or how long the outbox → Debezium → Kafka → consumers path takes to drain a
burst. A load experiment closes that gap. It also shows whether the
end-to-end guarantees still hold when the API is under real concurrency.

## What Changes

- Add a fifth experiment, `load`, run like the others:
  `docker compose --profile experiments run --rm experiments load`.
- A k6 script under `experiments/k6/` drives the payment API with concurrent
  virtual users. Each iteration creates a payment, then confirms (~80 %) or
  expires (~20 %) it, and refunds ~10 % of the confirmed ones. Merchants come
  from a fixed set (~50) so that every partition is exercised. The
  proportions can be changed through environment variables.
- Load profile: `ramping-arrival-rate`, which ramps up, holds a plateau and
  ramps down. The default target is moderate (200 iterations/s for 2 min) so
  it runs on the documented 6 GB Docker machine. Rate and duration can be set
  with `-e RATE=… -e DURATION=…`.
- k6 runs from the `grafana/k6` image on the Compose network. Nothing needs to
  be installed on the host.
- The experiment ends with `PASS`/`FAIL` like the others and judges two
  things:
  - **API under load** (k6 thresholds): `http_req_failed` < 1 % and p95
    latency < 500 ms.
  - **Pipeline consistency**: the outbox rows of the run equal the successful
    state changes reported by k6; the distinct `event_id`s on
    `pix.payments.v1` equal those outbox rows; reconciliation's
    `processed_events` matches them; reconciliation `check` passes; and the
    pipeline drains within a timeout.
- The time it takes to drain the pipeline after the load ends is measured
  and reported. It is not a failure criterion.
- `SABOTAGE=1` adds database contention: while the load runs, the outbox
  table is locked in bursts, so the API's writes block. The k6 latency
  threshold then fails while pipeline consistency still passes: a slowed-down
  API never loses or duplicates an accepted request. (Changed during
  implementation from a starved connection pool, which either kept the API
  from starting or did not slow it down on a fast machine.)
- Documentation: `experiments/05-load.md`, and the experiment tables in
  `experiments/README.md` and the root `README.md`.

### Decisions recorded during the grilling

| # | Question | Decision |
| --- | --- | --- |
| Q1 | What "volume" means | End-to-end under load: k6 thresholds for the API plus the pipeline consistency assertions |
| Q2 | Where it fits | Experiment 05 `experiments load`, script in `experiments/k6/`, k6 from the `grafana/k6` image on the Compose network |
| Q3 | Iteration flow | Create, then confirm ~80 % / expire ~20 %, refund ~10 % of the confirmed; ~50 fixed merchants; proportions configurable |
| Q4 | Load profile | `ramping-arrival-rate` ramp, plateau and ramp-down, moderate default (200 it/s, 2 min plateau), tunable; a stress-to-break profile is out of scope |
| Q5 | PASS criteria | `http_req_failed` < 1 %, p95 < 500 ms, outbox = topic = reconciliation, `check` passes, drained within timeout; drain time reported only |
| Q6 | `SABOTAGE=1` | Make the k6 thresholds fail while consistency still holds. First a starved connection pool; replaced during implementation, with the user's agreement, by bursts of an `EXCLUSIVE` lock on the outbox table |

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `validation-experiments`: adds the requirement that a load experiment
  drives the payment API concurrently, judges its latency and error rate, and
  shows that every accepted request reaches the topic and reconciliation.

## Impact

- New: `experiments/scripts/load.sh`, `experiments/k6/load.js`,
  `experiments/05-load.md`.
- Changed: `experiments/bin/experiment` (new command, help text),
  `experiments/README.md`, root `README.md`.
- New external image `grafana/k6` (pinned version), pulled on the first run.
- During `SABOTAGE=1` the experiment locks the `outbox` table in bursts and
  releases it on exit. No service code or container configuration changes.
- Not added to `experiments all`: that command stays the four failure
  experiments, and load results depend on the machine.
