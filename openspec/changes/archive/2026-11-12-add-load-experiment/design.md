# Design

## Context

See proposal.md for the motivation. The experiments run in the `experiments`
tooling container ([experiments/README.md](../../../experiments/README.md),
[docs/architecture.md](../../../docs/architecture.md) "Docker Compose
conventions"). That container drives the host's Docker daemon through the
mounted socket, and the repository is mounted read-only at `/workspace`. Two
consequences shape the design:

- A container the experiment starts with `docker run` resolves bind mounts on
  the **host**, where `/workspace/...` does not exist. A k6 container
  therefore cannot get its script through a bind mount.
- The `dc` helper may recreate only services that have no bind mounts.
  `payment-api` has none, so the experiment can recreate it.

The payment API writes the payment row and its outbox row in one
`@Transactional` method. A `2xx` answer therefore means exactly one outbox
row was committed, and a failure before the transaction began wrote nothing.

## Goals / Non-Goals

**Goals:**
- Reuse `lib/common.sh` (reset, `wait_drained`, `measure`/`assert_*`/`finish`)
  so the load experiment reads like the other four.
- Compare independent sources: the k6 counters, the API database, the outbox,
  the topic and reconciliation.

**Non-Goals:**
- Finding the breaking point (stress or soak profiles), dashboards
  (InfluxDB/Grafana) and k6 Cloud.
- Asserting antifraud or notification counts. Antifraud's windows are in
  memory, and the notification count is already covered by `duplicates`.
- Tuning the API or the consumers for throughput.

## Decisions

### k6 runs in a sibling container, script on stdin
`load.sh` runs `docker run --rm -i --network payments-kafka_default
grafana/k6:<pinned> run - < /workspace/experiments/k6/load.js` and passes the
parameters with `-e`. `k6 run -` reads the script from stdin, which avoids the
host-path bind-mount problem.
*Alternatives:* installing k6 in the tooling image adds a binary to maintain
and does not use the official image. A `k6` service in `docker-compose.yml`
would need a bind mount, which `dc` must not recreate, and adds a service for
a single experiment. `docker cp` into a created container works but takes
more steps.

### k6 summary as JSON on stdout
The script's `handleSummary` returns `{ stdout: JSON.stringify(data) }`.
`load.sh` captures stdout into `$WORK_DIR/k6-summary.json` and reads it with
`jq`; k6's progress output goes to stderr, where the user sees it. Using the
built-in summary avoids importing `textSummary` from jslib, which would need
network access at runtime. A k6 exit code of 99 (threshold crossed) is
recorded but does not abort the script, so the consistency checks still run.

### Iteration and counters
- Executor `ramping-arrival-rate`: stages are ramp to `RATE` over `RAMP`
  (default 30 s), hold for `DURATION` (default 2 m), ramp down over `RAMP`.
  `preAllocatedVUs`/`maxVUs` are derived from `RATE`.
- Each iteration: `POST /payments` (merchant `load-<RUN_ID>-NN`, NN in
  `1..MERCHANTS`, default 50; random amount). Then, with probability
  `CONFIRM_RATIO` (0.8) `/confirm`, else `/expire`. A confirmed payment is
  refunded with probability `REFUND_RATIO` (0.1).
- Custom k6 `Counter`s per successful transition (`created`, `confirmed`,
  `expired`, `refunded`), so that "successful state changes" = their sum.
- Thresholds: `http_req_failed: rate<0.01`,
  `http_req_duration: p(95)<500`.
- The request timeout is generous (30 s). A client-side timeout on a request
  the server later commits would make the k6 count and the outbox disagree.
  The sabotage makes requests fail fast server-side instead (see below).

### Isolating the run's data
The topics and consumer state are reset by `reset_pipeline`, but the payment
API database is not. Each run uses a unique merchant prefix
`load-<RUN_ID>-` (`RUN_ID` = UTC timestamp), and every API database and
outbox query filters on it. This is more precise than the time filter used by
`kafka-outage`, because the default simulator is stopped but other writers
could exist.

### Assertions
1. k6: thresholds passed (from the summary's `metrics.*.thresholds`), with
   each threshold reported as its own assertion.
2. Outbox rows for the prefix = sum of the k6 transition counters.
3. Outbox rows = events implied by the API's payment states (CREATED 1,
   CONFIRMED/EXPIRED 2, REFUNDED 3). This check does not depend on k6.
4. Distinct `event_id`s on `pix.payments.v1` = outbox ids (file comparison,
   as in `kafka-outage`).
5. Groups drained (`wait_drained`); `processed_events` = outbox rows;
   reconciliation payments = API payments (id, status, amount); `check`
   passes; nothing dead-lettered.

### Drain time
`load.sh` records `T_END` when k6 exits. It then polls until the distinct
events on the topic equal the outbox rows (Debezium caught up) and until
`wait_drained` returns, and records `elapsed` at each point. Both values are
`measure`d only, not asserted.

### Sabotage: outbox lock contention
With `SABOTAGE=1`, `load.sh` starts a background `psql` loop just before k6.
Each pass runs `BEGIN; LOCK TABLE outbox IN EXCLUSIVE MODE;
SELECT pg_sleep(SABOTAGE_LOCK_SECONDS); COMMIT;` and then pauses for
`SABOTAGE_PAUSE_SECONDS` (both 1 s by default). `EXCLUSIVE` conflicts with the
`ROW EXCLUSIVE` lock that every API write takes when it inserts its outbox
row, but not with reads, so Debezium keeps streaming. Requests that arrive
during a lock wait up to a second: p95 goes past 500 ms, and each
transaction still commits whole. The loop stops when k6 exits. An EXIT
`trap` also stops it, so no lock outlives the run.
*Rejected during implementation:* starving the Hikari pool. With 1 or 2
connections the API does not start, because Flyway takes the connections at
boot and the 250 ms timeout fails it. With 3 connections, ~2 ms transactions
serve ~420 req/s with no visible effect. A `pg_sleep` trigger on the outbox
would also work, but it changes the API's schema during the test.

### Not part of `experiments all`
`all` stays the four failure experiments. Load results depend on the machine,
and the load run is the longest one.

## Risks / Trade-offs

- [The default thresholds fail on a slow machine] → The defaults are
  calibrated during implementation on the documented 6 GB setup, and
  `05-load.md` explains how to lower `RATE`.
- [The sabotage does not cross a threshold at the default rate] → The lock
  share of each cycle sets the p95: about half of the requests wait. Calibrated
  at the default rate, it gave p95 ≈ 810 ms. `SABOTAGE_LOCK_SECONDS` raises it.
- [Timeouts: a request commits server-side after the client gave up] → The
  30 s client timeout (above the 1 s lock) makes this unlikely, and
  assertion 3 (API states vs outbox) does not depend on the k6 counts.
- [The Compose network name is hard-coded] → It is derived from the project
  `name: payments-kafka`, which `dc` already relies on.
- [Debezium becomes a bottleneck and the topic lags far behind] → This is
  what the drain-time measurement shows. The catch-up timeout scales with
  the run size.
