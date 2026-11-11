# local-environment Specification

## Purpose
Provides the one-command local environment (Kafka, Kafka Connect with Debezium, Kafka UI and Postgres) in which every component of the pipeline runs and can be observed.

## Requirements

### Requirement: Infrastructure starts with one command
Running `docker compose up -d` from the repository root SHALL start Kafka in KRaft mode, Kafka UI, Postgres and Kafka Connect, and all of them SHALL become healthy without manual steps.

#### Scenario: Fresh start
- **WHEN** `docker compose up -d` is run on a machine with no existing volumes
- **THEN** Kafka accepts connections on `localhost:9092`, Kafka UI responds on `http://localhost:8080`, Kafka Connect responds on `http://localhost:8083/connectors` and Postgres accepts connections on `localhost:5432` as user `pix`

### Requirement: Topics are provisioned
The environment SHALL create `pix.payments.v1`, `pix.fraud-alerts.v1` and `pix.payments.retry.v1` with 6 partitions each and `pix.payments.dlq.v1` with 1 partition, and SHALL NOT auto-create topics on first use.

#### Scenario: Topics exist after startup
- **WHEN** the stack has started and topics are listed
- **THEN** the four topics exist with the specified partition counts

#### Scenario: Unknown topic is not auto-created
- **WHEN** a client produces to a topic that was not provisioned
- **THEN** the produce fails and the topic is not created

### Requirement: Databases are provisioned
Postgres SHALL run with logical replication enabled and SHALL contain the databases `payments`, `reconciliation` and `notification`, accessible to user `pix`.

#### Scenario: Databases exist on first start
- **WHEN** the stack starts with an empty data volume
- **THEN** user `pix` can connect to each of the three databases and `SHOW wal_level` returns `logical`

### Requirement: Outbox rows are published to Kafka
Once the outbox connector is registered, every row inserted into `public.outbox` in database `payments` SHALL be published to `pix.payments.v1` as a record with the row's `merchant_id` as string key and the row's `payload` as the bare JSON value. Publication is at-least-once: after a Kafka Connect or broker restart a row MAY be published more than once, so consumers dedupe by `event_id`.

#### Scenario: Inserted row becomes an event
- **WHEN** a row with `merchant_id` `merchant-007` and a valid payment event as `payload` is inserted into `outbox`
- **THEN** a record with key `merchant-007` and a value equal to that payload, with no CDC or schema envelope, appears on `pix.payments.v1`

#### Scenario: Rows written while Kafka is down are delivered later
- **WHEN** rows are inserted into `outbox` while the Kafka broker is stopped, and the broker is then started again
- **THEN** every such row is published to `pix.payments.v1` in insertion order per merchant, with none missing (rows published shortly before the outage may appear a second time)

### Requirement: Connector registration is automatic and repeatable
Starting the `apps` profile SHALL register the outbox connector after `payment-api` is healthy, and repeating the registration SHALL succeed without creating a duplicate connector.

#### Scenario: Registration runs twice
- **WHEN** the connector registration step is executed a second time
- **THEN** it exits successfully and exactly one outbox connector exists in state `RUNNING`

### Requirement: Whole pipeline starts with the apps profile
`docker compose --profile apps up -d --build` SHALL build and start `payment-api`, `simulator`, `antifraud`, `notification` and `reconciliation` in addition to the infrastructure, and consumer services SHALL be scalable with `--scale`.

#### Scenario: Scaling a consumer
- **WHEN** `docker compose --profile apps up -d --scale reconciliation=3` is run
- **THEN** three `reconciliation` containers run and Kafka UI shows three members in consumer group `reconciliation`

### Requirement: Environment can be reset
The environment SHALL be fully removable so that a subsequent start behaves like a fresh start.

#### Scenario: Reset
- **WHEN** `docker compose --profile apps down -v` is run and the stack is started again
- **THEN** topics are empty, databases are recreated and no consumer group offsets remain
