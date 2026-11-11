# Experiment 3: Kafka down, payments still accepted, nothing lost

```sh
docker compose --profile experiments run --rm experiments kafka-outage
```

**Property.** The payment API never talks to Kafka. Each state change writes
the `payments` row **and** an `outbox` row in one database transaction, and
Debezium reads the outbox from the Postgres write-ahead log and publishes it
to `pix.payments.v1` (the **transactional outbox** pattern). So when the
broker is down the API keeps accepting payments, the outbox rows pile up, and
once the broker is back every one of them reaches the topic and the
consumers. Compare with a "dual write" (update the database, then publish):
during the outage it would either fail the request or lose the event.

**How it breaks things on purpose.** `docker stop kafka`, for 20 seconds,
while the simulator in **normal mode** keeps calling the API over HTTP.

## What the script does

1. Resets the pipeline and notes the database time `T0` (the payment API
   database is not reset, so everything is filtered by `created_at >= T0`).
2. Starts a normal-mode run (seed `303`, 600 payments, 25 steps/s): simulator
   → `payment-api` → `payments` + `outbox` → Debezium → topic → consumers.
3. Once 100 outbox rows exist, **stops the broker**. For 20 seconds it
   polls the API's health and counts outbox rows and simulator answers.
4. Starts the broker, waits until it is healthy, waits for the simulator to
   finish, then waits until the distinct `event_id`s on the topic equal the
   outbox rows (Debezium caught up) and the three groups are drained.

| Assertion | Sources compared |
| --- | --- |
| outbox rows accumulated while the broker was down | outbox count at stop and at start |
| the API accepted requests during the outage | simulator `events_sent_total` delta (counts `2xx` answers only) |
| no failed health probe, no API error or retry | health probes, simulator `api` counters |
| every API call wrote one outbox row | `events_sent_total` = outbox rows |
| distinct `event_id`s on the topic = outbox ids | the two sorted id lists, compared byte for byte |
| groups drained, each event applied once | lag, `processed_events` = outbox rows |
| reconciliation holds every API payment with the same status and amount | `payments` of the API database vs reconciliation's `payments`, compared byte for byte |
| `check` passes, nothing dead-lettered | `check`, DLQ |

Variables: `SEED` (303), `PAYMENTS` (600), `RATE` (25), `OUTAGE_SECONDS`
(20), `STOP_AFTER_ROWS` (100).

## Measured (clean stack, defaults)

```
outbox rows when the broker stopped              182
outbox rows when the broker started              703
outbox rows written during the outage            521
API 2xx answers during the outage                521
API health probes during the outage (failed / total) 0 / 10
simulator events_sent_total / api counters       1227 / {"retries":0,"conflicts":0,"errors":0,"skipped":0}
outbox rows of the run                           1227
records on pix.payments.v1 (incl. at-least-once republishes) 1291
distinct event_ids on pix.payments.v1            1227
payments in API database / reconciliation        600 / 600
reconciliation processed_events                  1227
check report                                     {"ok":true,"merchants":20,"ledger_entries":552,"processed_events":1227,"mismatches":0}
PASS kafka-outage
```

Debezium had published everything about 15 seconds after the broker was
healthy again; the whole run takes about 2 minutes.

Note `1291` records for `1227` distinct events: in that run **64 events were
published twice**. Kafka Connect commits the connector's source offsets every
5 seconds (`OFFSET_FLUSH_INTERVAL_MS`); if the producer's last batch before the
outage was written but not acknowledged, or the source offsets were not yet
committed, the task produces those records again when it resumes. The number
depends on timing: in another run on a clean stack it was 0 (1227 records for
1227 events). That is the at-least-once side of the outbox pattern, and the
reason every consumer dedupes by `event_id` and every assertion here compares
*distinct* ids.

## What to watch while it runs

- Kafka UI cannot reach the cluster while the broker is down; afterwards,
  **Topics → pix.payments.v1** shows the message count jumping as Debezium
  publishes the backlog, and **Kafka Connect → payments-outbox** stays
  `RUNNING`.
- The outbox grows while Kafka is down (from the host):
  `docker compose exec postgres psql -U pix -d payments -c "SELECT count(*) FROM outbox"`.
- The replication slot retains WAL while Debezium cannot publish:
  `docker compose exec postgres psql -U pix -d payments -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained FROM pg_replication_slots"`
  ([infra/README.md](../infra/README.md#caveat-the-replication-slot-retains-wal)).
- `docker compose logs -f kafka-connect`: the producer's connection errors
  during the outage and the recovery afterwards. The connector and its task
  stay `RUNNING` throughout; no manual restart is needed.
- `docker compose logs -f reconciliation`: the consumers reconnect on their
  own (librdkafka retries) and catch up.

## Seeing it fail

```sh
docker compose --profile experiments run --rm -e SABOTAGE=1 experiments kafka-outage
```

After the drain, the sabotage deletes one payment from reconciliation's
`payments` table, as if its events had been lost. Measured: the API/
reconciliation payment comparison fails (600 payments in the API database,
599 in reconciliation; the diff shows the missing one) and the experiment prints `FAIL kafka-outage` with exit
status 1. The other assertions still pass, which shows which property broke.
