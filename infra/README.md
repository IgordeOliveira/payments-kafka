# Local environment

Docker Compose stack for the Pix payment event pipeline: Kafka (KRaft, single
broker), Kafka UI, Postgres with logical replication and Kafka Connect with
Debezium. Topic names, ports, databases and service names follow
[`docs/architecture.md`](../docs/architecture.md).

All commands run from the repository root. They work as written in PowerShell,
cmd and macOS/Linux shells. In **Git Bash on Windows**, prefix `docker exec`
commands that contain container paths (such as `/opt/kafka/...`) with
`MSYS_NO_PATHCONV=1`, or Git Bash rewrites the paths. In Windows PowerShell
5.1, `curl` is an alias for `Invoke-WebRequest`: type `curl.exe` instead.

## Contents

| Path | Purpose |
| --- | --- |
| `../docker-compose.yml` | the stack |
| `kafka/create-topics.sh` | run by `kafka-init`: creates the four topics, idempotent |
| `postgres/init/01-create-databases.sh` | creates `payments`, `reconciliation`, `notification` on an empty volume |
| `connect/outbox-connector.json` | Debezium outbox connector config (outbox event router) |
| `connect/register-connector.sh` | run by `connector-init`: idempotent `PUT /connectors/payments-outbox/config` |

## Start and stop

```sh
# Infrastructure only: kafka, kafka-init, kafka-ui, postgres, kafka-connect
docker compose up -d --wait
docker compose ps -a

# Whole pipeline (builds the application images)
docker compose --profile apps up -d --build

# Scale a consumer
docker compose --profile apps up -d --scale reconciliation=3

# Stop, keeping data
docker compose --profile apps stop

# Remove containers, keeping volumes
docker compose --profile apps down
```

`kafka-init` and `connector-init` are one-shot services: `Exited (0)` is their
healthy final state.

## Reset

```sh
docker compose --profile apps down -v
```

Deletes the `kafka-data` and `postgres-data` volumes, so the next start
recreates the topics (empty), the databases (no tables) and drops every
consumer group offset, the Connect offsets and the replication slot.

## Ports

| Component | URL / address from the host | Inside the Compose network |
| --- | --- | --- |
| Kafka | `localhost:9092` | `kafka:29092` |
| Kafka UI | http://localhost:8080 | |
| Kafka Connect REST | http://localhost:8083 | `kafka-connect:8083` |
| Postgres (`pix` / `pix`) | `localhost:5432` | `postgres:5432` |
| Payment API (`apps`) | http://localhost:8081 | `payment-api:8081` |
| Simulator (`apps`) | http://localhost:8082 | |

## Inspecting topics

Kafka UI at http://localhost:8080 shows topics, messages, consumer groups (with
lag) and the Debezium connector. From the command line:

```sh
# Topics and partitions
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic pix.payments.v1

# Read a topic from the beginning, with keys and headers (Ctrl+C to stop)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic pix.payments.v1 --from-beginning --formatter-property print.key=true --formatter-property print.headers=true
```

Topics are never created implicitly (`auto.create.topics.enable=false`):
producing to an unknown topic fails. New topics go into
`kafka/create-topics.sh`.

## Consumer lag

```sh
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation
```

`LAG` per partition is `LOG-END-OFFSET - CURRENT-OFFSET`; `--describe --members`
shows which consumer instance owns which partitions.

## The outbox connector

`connector-init` (profile `apps`) registers the connector once `payment-api` is
healthy, because the API's migrations create the `outbox` table. To register it
by hand (it is idempotent, so running it again is safe):

```sh
docker compose --profile apps run --rm --no-deps connector-init
```

Status and config:

```sh
curl -s http://localhost:8083/connectors?expand=status
curl -s http://localhost:8083/connectors/payments-outbox/status
curl -s http://localhost:8083/connectors/payments-outbox/config
curl -s -X POST "http://localhost:8083/connectors/payments-outbox/restart?includeTasks=true"
```

How it works: Debezium reads the Postgres WAL of database `payments` through
the logical replication slot `outbox_slot` (publication `outbox_publication`,
table `public.outbox` only). The outbox event router turns each inserted row
into one record on `pix.payments.v1`: key `merchant_id` (string), value the
`payload` column as bare JSON (no CDC envelope, no schema). Rows that exist when
the connector is first registered are published by an initial snapshot. Debezium
adds an `id` header (the row id) plus `__debezium.context.*` headers; consumers
rely on the payload only.

Delivery is **at-least-once**. Connect commits its source offsets every 5
seconds (`OFFSET_FLUSH_INTERVAL_MS`). If Connect or Kafka stops before a flush,
the records since the last flush are published again, so consumers dedupe by
`event_id`. The value is the JSONB column re-serialized by Postgres, so key
order and whitespace can differ from the JSON the API inserted; the content is
the same.

## Caveat: the replication slot retains WAL

The replication slot keeps every WAL segment Debezium has not confirmed yet.
While `kafka-connect` is stopped, or the connector is failed or deleted, Postgres
keeps accumulating WAL for the slot and the `postgres-data` volume grows. Writes
to the other databases count too, because WAL is shared by the whole instance.
That is harmless for short experiments. Check the retained size with:

```sh
docker exec postgres psql -U pix -d payments -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots"
```

To release it, start Connect again, or reset the environment with
`docker compose --profile apps down -v`. If you delete the connector on purpose,
also drop the slot:
`docker exec postgres psql -U pix -d payments -c "SELECT pg_drop_replication_slot('outbox_slot')"`.

## Postgres

```sh
docker exec -it postgres psql -U pix -d payments
docker exec postgres psql -U pix -d payments -c "SHOW wal_level"
```

Databases are created only when the data volume is empty. Each service creates
its own tables through its migrations.

## Troubleshooting

- **Postgres exits saying there is data in `/var/lib/postgresql/data`**: the
  volume was created by an older Postgres major version (the image here is
  Postgres 18, which keeps data in `/var/lib/postgresql/18/docker`). Run
  `docker compose --profile apps down -v`.
- **Container name already in use** (`kafka`, `postgres`, ...): a container
  with that name exists from another Compose project. Remove it with
  `docker rm -f <name>`.
- **Connector `FAILED`**: check
  `curl -s http://localhost:8083/connectors/payments-outbox/status` for the
  trace. If the `outbox` table did not exist yet, restart the connector with the
  `restart` call above once it exists.
