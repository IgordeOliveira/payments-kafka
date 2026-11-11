# Proposal

## Why

Nothing can be observed or broken on purpose without a running cluster. The
project needs a one-command local environment with Kafka, Kafka Connect
(Debezium), Kafka UI and Postgres, with topics already created, so every other
component has somewhere to run.

## What Changes

- Add `docker-compose.yml` with Kafka in KRaft mode, Kafka UI, Postgres with
  logical replication, and Kafka Connect with the Debezium Postgres connector.
- Create the four topics automatically on startup.
- Create the three Postgres databases on first startup.
- Register the Debezium outbox connector automatically once the outbox table exists.
- Declare every application service under the `apps` Compose profile so the
  whole pipeline starts with one command.

## Capabilities

### New Capabilities
- `local-environment`: the Docker Compose stack, topic provisioning, database provisioning and outbox connector registration.

### Modified Capabilities

None.

## Impact

- New files: `docker-compose.yml`, `infra/**`.
- Uses host ports 9092, 8080, 8083, 5432, 8081, 8082.
- Application Dockerfiles are owned by the other changes; this change only
  references them from Compose.
