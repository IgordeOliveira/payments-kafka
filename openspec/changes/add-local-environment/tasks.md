# Tasks

## 1. Kafka and Kafka UI

- [ ] 1.1 Add `kafka` (KRaft, single broker, listeners `kafka:29092` and `localhost:9092`, auto-create disabled, healthcheck) to `docker-compose.yml`; verify `docker compose up -d kafka` reaches `healthy`
- [ ] 1.2 Add one-shot `kafka-init` that creates the four topics idempotently with the partition counts from `docs/architecture.md`; verify with `kafka-topics --describe` and by running it twice
- [ ] 1.3 Add `kafka-ui` on port 8080 pointing at the broker and at Kafka Connect; verify the UI lists the four topics

## 2. Postgres

- [ ] 2.1 Add `postgres` with `wal_level=logical`, user `pix`, a named volume and a healthcheck; verify `SHOW wal_level` returns `logical`
- [ ] 2.2 Add `infra/postgres/` init script creating databases `payments`, `reconciliation`, `notification`; verify `pix` can connect to each on a fresh volume

## 3. Kafka Connect and the outbox connector

- [ ] 3.1 Add `kafka-connect` (Debezium Connect image, pinned) with internal topics configured for a single broker and a healthcheck; verify `GET http://localhost:8083/connector-plugins` lists the Postgres connector
- [ ] 3.2 Write `infra/connect/outbox-connector.json` using the outbox event router as described in design.md; verify by creating the `outbox` table manually from the DDL in `docs/architecture.md`, registering the connector, inserting a row and reading the record (key `merchant_id`, bare JSON value) from `pix.payments.v1`
- [ ] 3.3 Add one-shot `connector-init` (profile `apps`) that waits for Connect and `payment-api`, then registers the connector with an idempotent `PUT`; verify running it twice leaves one `RUNNING` connector
- [ ] 3.4 Verify outage recovery: stop `kafka`, insert outbox rows, start `kafka`, and confirm every row arrives on the topic

## 4. Application services in Compose

- [ ] 4.1 Declare `payment-api` and `simulator` under profile `apps` with build contexts, environment, ports, healthchecks and `depends_on` per `docs/architecture.md`; verify `docker compose --profile apps config` is valid
- [ ] 4.2 Declare `antifraud`, `notification`, `reconciliation` under profile `apps` (root build context, per-service Dockerfile path, no container names or host ports); verify `docker compose --profile apps config` is valid and `--scale reconciliation=3` is accepted by `config`

## 5. Documentation and reset

- [ ] 5.1 Write `infra/README.md` covering start, stop, reset (`down -v`), ports, how to inspect topics, lag and the connector, and the replication-slot caveat; verify every documented command runs as written
- [ ] 5.2 Verify a full reset: `docker compose --profile apps down -v` then `docker compose up -d` yields empty topics and fresh databases

## Workflow follow-up

- Once all application changes are merged, the coordinator verifies `docker compose --profile apps up -d --build` end to end as part of `add-validation-experiments`.
