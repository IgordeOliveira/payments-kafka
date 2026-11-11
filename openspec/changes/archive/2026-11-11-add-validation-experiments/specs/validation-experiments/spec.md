# Spec Delta

## Purpose

Provides repeatable experiments that deliberately break the pipeline and demonstrate, with measured results, that each reliability property holds.

## ADDED Requirements

### Requirement: Experiments are one command and self-judging
Each experiment SHALL be runnable with a single documented command against the local Compose stack, SHALL print the values it measured, and SHALL end with an explicit `PASS` or `FAIL` and a matching exit status.

#### Scenario: Passing experiment
- **WHEN** an experiment's assertions all hold
- **THEN** it prints `PASS` and exits with status 0

#### Scenario: Failing experiment
- **WHEN** an assertion does not hold
- **THEN** it prints `FAIL` with the expected and measured values and exits with a non-zero status

### Requirement: Duplicate events are applied once
The duplicates experiment SHALL publish a bounded stream containing deliberately duplicated events and SHALL show that merchant balances equal those of the same stream without duplicates and that no notification was emitted twice.

#### Scenario: Duplicates have no effect
- **WHEN** the experiment runs with a duplicate rate above zero
- **THEN** the simulator reports duplicates sent, the reconciliation consistency check passes, balances match the duplicate-free expectation and the notification count equals the number of distinct notifiable events

### Requirement: Killing a consumer loses nothing
The killed-consumer experiment SHALL forcibly kill a reconciliation instance while it is processing and SHALL show that every distinct event is still applied exactly once.

#### Scenario: Consumer killed mid-run
- **WHEN** a reconciliation container is killed during a bounded run and the group continues with the remaining or restarted instances
- **THEN** after lag reaches zero the number of distinct applied events equals the number of distinct valid events produced and the consistency check passes

### Requirement: Kafka outage does not lose payments
The Kafka-outage experiment SHALL stop the Kafka broker while the simulator keeps creating payments through the API and SHALL show that every payment accepted during the outage reaches the topic and the consumers after the broker returns.

#### Scenario: Payments accepted during the outage arrive later
- **WHEN** the broker is stopped, payments are created through the API, and the broker is started again
- **THEN** the API accepted requests throughout, and after lag reaches zero the distinct events on `pix.payments.v1` equal the outbox rows and reconciliation holds every payment the API holds

### Requirement: Replay rebuilds the same balances
The replay experiment SHALL reset reconciliation state and the group's offsets to the earliest position and SHALL show that reprocessing reproduces identical merchant balances.

#### Scenario: Balances before and after replay
- **WHEN** balances are exported, state and offsets are reset, and the topic is reprocessed to zero lag
- **THEN** the balances exported after the replay are identical to those exported before it

### Requirement: Observation walkthroughs
The project SHALL document guided walkthroughs, with the exact commands and what to observe in Kafka UI, for partitioning and per-merchant ordering, consumer groups with lag and rebalance, and a message travelling through retry to the DLQ.

#### Scenario: Rebalance walkthrough
- **WHEN** a reader follows the consumer-group walkthrough and scales a consumer from one to three instances
- **THEN** the documented steps show where to see the partition reassignment and the lag per partition

#### Scenario: DLQ walkthrough
- **WHEN** a reader follows the retry and DLQ walkthrough and publishes an invalid message
- **THEN** the documented steps show the record in the DLQ and how to read its headers

### Requirement: Project entry point
The root `README.md` SHALL let a new reader start the whole pipeline, understand the architecture, and find the experiment for each learning goal.

#### Scenario: Quick start works
- **WHEN** a reader with Docker installed follows the quick start on a clean checkout
- **THEN** the full pipeline is running and events are visible in Kafka UI without undocumented steps

### Requirement: End-to-end path through the outbox
In normal mode a payment created by the simulator SHALL be observable at every stage: in the API database, in the outbox, on `pix.payments.v1` and in the reconciliation state.

#### Scenario: One payment traced end to end
- **WHEN** the pipeline runs in normal mode and a payment is confirmed
- **THEN** the payment exists in the API database, its events exist in the outbox and on the topic keyed by `merchant_id`, and reconciliation shows it as `CONFIRMED` with the merchant credited
