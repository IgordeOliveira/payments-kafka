# Experiments

The pipeline is only interesting if it survives being broken on purpose. Each
experiment here provokes one failure against the running Compose stack,
measures what happened with numbers from independent sources, and ends with an
explicit `PASS` or `FAIL` (exit status `0` or `1`). The walkthroughs are
guided tours for the lessons that are about *observing* Kafka rather than
asserting a property.

| Learning goal | Experiment or walkthrough | Command / document |
| --- | --- | --- |
| At-least-once delivery and idempotent consumers | duplicated events are applied once | `experiments duplicates` · [01-duplicates.md](01-duplicates.md) |
| Offsets and commits, consumer groups | a consumer killed mid-processing loses nothing | `experiments kill-consumer` · [02-kill-consumer.md](02-kill-consumer.md) |
| The outbox pattern | Kafka down while the API keeps accepting payments | `experiments kafka-outage` · [03-kafka-outage.md](03-kafka-outage.md) |
| Reprocessing history by resetting offsets | replay rebuilds identical balances | `experiments replay` · [04-replay.md](04-replay.md) |
| Throughput, latency and backpressure through the outbox | k6 load on the API; every accepted request reaches the topic and reconciliation (not in `all`) | `experiments load` · [05-load.md](05-load.md) |
| Partitioning by key and per-merchant ordering | walkthrough | [walkthrough-partitioning.md](walkthrough-partitioning.md) |
| Consumer groups, lag and rebalancing | walkthrough | [walkthrough-consumer-groups.md](walkthrough-consumer-groups.md) |
| Retry with backoff and the DLQ | walkthrough | [walkthrough-retry-dlq.md](walkthrough-retry-dlq.md) |

## Running

The pipeline must be running (from the repository root):

```sh
docker compose --profile apps up -d --build --wait
```

Every experiment is one command. The scripts run inside a small tooling
container (service `experiments`, profile `experiments`), so they behave the
same on Windows, macOS and Linux and need nothing on the host but Docker:

```sh
docker compose --profile experiments run --rm experiments help
docker compose --profile experiments run --rm experiments duplicates
docker compose --profile experiments run --rm experiments kill-consumer
docker compose --profile experiments run --rm experiments kafka-outage
docker compose --profile experiments run --rm experiments replay
docker compose --profile experiments run --rm experiments all        # the four in sequence
docker compose --profile experiments run --rm experiments load       # not part of "all"
```

The first run builds the image (a minute). Each experiment takes one to three
minutes. `load` takes about four, pulls the pinned `grafana/k6` image on its
first run and is deliberately **not part of `all`**: its latency numbers depend
on your machine (see [05-load.md](05-load.md#tuning-for-your-machine)). While
one runs, watch it in Kafka UI at http://localhost:8080; each experiment's
document says what to look at.

Other commands: `selftest` exercises every helper with a tiny run; `reset`
empties the topics and the consumers' state; `shell` opens a shell with the
tooling, used by the walkthroughs.

### What an experiment does to your stack

- It **stops the default simulator** (the unbounded chaos-mode one started by
  `--profile apps`), because exact expectations need a bounded run. Start it
  again with `docker compose --profile apps start simulator`.
- It **resets the pipeline** first: stops the three consumers, deletes their
  consumer groups, deletes and recreates the four `pix.*` topics (so they are
  empty), clears reconciliation's state (`reset-state --yes`) and
  notification's `sent_notifications`, and starts the consumers again. The
  payment API database is not touched; the outage experiment filters it by
  time.
- It runs a **bounded simulator pass** in a container named `exp-simulator`
  (the simulator image with `SIMULATOR_MAX_PAYMENTS` and a fixed
  `SIMULATOR_SEED`), so the expected values are exact.
- The tooling container mounts the **Docker socket** so it can stop Kafka or
  kill a consumer. That gives it full control of your Docker daemon: fine for
  a local study project, never do it on a shared machine.

### Seeing it fail

An experiment that can only print `PASS` proves nothing. Every experiment
accepts `SABOTAGE=1`, which breaks the property it checks in a realistic way
(described in each document) so you can see the assertions catch it:

```sh
docker compose --profile experiments run --rm -e SABOTAGE=1 experiments duplicates
```

The size of each run can be changed the same way (`-e PAYMENTS=2000`,
`-e SEED=7`, ...); the variables are listed in each document.

## How they measure

All helpers are in [`lib/common.sh`](lib/common.sh); the experiments are in
[`scripts/`](scripts/).

- **Clean, known state.** `reset_pipeline` (above) before every run.
- **Bounded runs.** The simulator's `GET /stats` counters move only when Kafka
  acknowledged a record (chaos mode) or the API answered `2xx` (normal mode),
  so `events_sent_total` is the exact number of distinct valid events produced.
- **Drained, not slept.** `wait_drained` polls
  `kafka-consumer-groups.sh --describe` until every listed group has lag 0 on
  all its partitions **including `pix.payments.retry.v1`**, on two
  observations in a row, with a timeout. A retry record that is not due yet is
  not committed, so it counts as lag: an out-of-order event waiting for its
  predecessor keeps the group "not drained" until it is applied.
- **Independent sources.** Assertions compare the simulator counters, the
  topic contents (read with `kcat`, deduplicated by `event_id`), the payment
  API database and outbox, reconciliation's `processed_events`, `check` and
  `balances`, notification's stdout lines and the DLQ headers.
- **At-least-once aware.** The outbox connector and the simulator's chaos mode
  can put the same event on the topic more than once, so counts compare
  *distinct* `event_id`s.
- **No exact antifraud counts** where consumers are killed or scaled:
  antifraud keeps its windows in memory and drops them on rebalance.

## Troubleshooting

- `container 'kafka' is not running` / `service ... has no container`: start
  the pipeline first (`docker compose --profile apps up -d --build --wait`).
- `the Debezium connector payments-outbox is not RUNNING`: see
  [infra/README.md](../infra/README.md#the-outbox-connector).
- A run stopped half-way (Ctrl+C) can leave Kafka stopped, reconciliation
  scaled to 3 or `exp-simulator` running. Fix with `docker start kafka` (if
  needed), `docker compose --profile apps up -d --scale reconciliation=1` and
  `docker rm -f exp-simulator`; the next experiment resets everything else.
- `FAIL` with `not drained after ...s`: the measured lag is printed on every
  poll; look at the consumer logs (`docker compose logs reconciliation`) for
  errors.
