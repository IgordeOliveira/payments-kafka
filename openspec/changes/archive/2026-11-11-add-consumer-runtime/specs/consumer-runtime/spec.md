# Spec Delta

## Purpose

Provides the shared consumer behavior every service relies on: at-least-once consumption with manual commits, contract validation, retry with backoff, dead letter routing and graceful shutdown.

## ADDED Requirements

### Requirement: At-least-once consumption with manual commits
The runtime SHALL commit a record's offset only after the record's handler has completed or the record has been handed off to the retry topic or the DLQ, and SHALL NOT use automatic offset commits.

#### Scenario: Successful handling
- **WHEN** a handler completes for a record
- **THEN** the committed offset of that partition advances past the record

#### Scenario: Consumer killed mid-processing
- **WHEN** a consumer process is killed while a handler is running and another instance of the same group takes over
- **THEN** the unfinished record is delivered again and no record is skipped

### Requirement: Per-partition ordering
The runtime SHALL invoke the handler for records of the same partition one at a time and in offset order.

#### Scenario: Ordered delivery within a partition
- **WHEN** several records for one merchant are published in order
- **THEN** the handler receives them in the same order and never concurrently

### Requirement: Contract validation
The runtime SHALL validate each record value against the JSON Schema configured for its topic before invoking the handler, and SHALL route a record that is not valid JSON or violates the schema to the DLQ with reason `invalid_message` without invoking the handler.

#### Scenario: Invalid record
- **WHEN** a record whose value is not JSON, or fails the schema, is consumed
- **THEN** the handler is not invoked, the record is published to `pix.payments.dlq.v1` with `x-dlq-reason` `invalid_message`, and its offset is committed

#### Scenario: Valid record
- **WHEN** a record that satisfies the schema is consumed
- **THEN** the handler is invoked with the parsed value

### Requirement: Retry with exponential backoff
When a handler throws a retryable error, the runtime SHALL publish the record to `pix.payments.retry.v1` with its original key and value and the retry headers defined in `docs/architecture.md`, with `x-retry-not-before` set by exponential backoff, and then commit the source offset.

#### Scenario: First failure
- **WHEN** a handler throws for a record on first delivery
- **THEN** the record appears on the retry topic with `x-retry-attempt` `1`, `x-retry-group` equal to the consumer group and the original topic, partition and offset headers

#### Scenario: Backoff grows
- **WHEN** a record fails on consecutive retries
- **THEN** the delay between `x-retry-not-before` values doubles with each attempt

#### Scenario: Retry succeeds
- **WHEN** a record that failed once is retried after its delay and the handler completes
- **THEN** the record is not republished and does not reach the DLQ

### Requirement: Retry delay is honored
The runtime SHALL NOT invoke the handler for a retry record before the time in `x-retry-not-before`, and SHALL remain an active group member while waiting.

#### Scenario: Waiting does not cause a rebalance
- **WHEN** a retry record is not yet due
- **THEN** the handler runs only after the due time and the consumer is not removed from the group while waiting

### Requirement: Retry ownership
The runtime SHALL process only retry records whose `x-retry-group` equals its own group id, and SHALL skip and commit all others.

#### Scenario: Foreign retry record
- **WHEN** the `notification` group reads a retry record owned by `reconciliation`
- **THEN** the notification handler is not invoked and the offset is committed

### Requirement: Dead letter after exhausted retries
When a record has failed `RETRY_MAX_ATTEMPTS` retries, or its handler throws a non-retryable error, the runtime SHALL publish it to `pix.payments.dlq.v1` with the DLQ headers defined in `docs/architecture.md` and commit its offset.

#### Scenario: Retries exhausted
- **WHEN** a record keeps failing until the maximum number of retries is reached
- **THEN** it is published to the DLQ with `x-dlq-reason` `retries_exhausted`, `x-dlq-group` set to the consumer group and the last error message, and is not retried again

#### Scenario: Non-retryable error
- **WHEN** a handler throws a non-retryable error on first delivery
- **THEN** the record goes to the DLQ without passing through the retry topic

### Requirement: No loss on hand-off
The runtime SHALL confirm that a retry or DLQ record was written before committing the source offset.

#### Scenario: Retry topic unavailable
- **WHEN** publishing to the retry topic fails
- **THEN** the source offset is not committed and the record is delivered again later

### Requirement: Graceful shutdown and rebalance visibility
On a termination signal the runtime SHALL finish in-flight records, commit their offsets and leave the consumer group, and it SHALL log every partition assignment and revocation.

#### Scenario: Graceful stop
- **WHEN** the process receives `SIGTERM` while handling a record
- **THEN** the handler completes, the offset is committed, and the next instance does not receive that record again

#### Scenario: Rebalance is logged
- **WHEN** a second instance joins the group
- **THEN** both instances log the partitions revoked and assigned

### Requirement: Configuration from environment
The runtime SHALL read `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `RETRY_MAX_ATTEMPTS`, `RETRY_BACKOFF_MS` and `LOG_LEVEL` with the defaults in `docs/architecture.md`, and SHALL fail at startup with a clear message on an invalid value.

#### Scenario: Invalid configuration
- **WHEN** `RETRY_MAX_ATTEMPTS` is set to a negative number
- **THEN** startup fails with an error naming the variable
