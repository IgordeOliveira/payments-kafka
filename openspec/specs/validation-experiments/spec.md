# validation-experiments Specification

## Purpose
Provides repeatable experiments that deliberately break the pipeline and demonstrate, with measured results, that each reliability property holds.

## Requirements

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

### Requirement: Load experiment drives the payment API concurrently
The load experiment SHALL drive the payment API with concurrent clients following a ramp-up, plateau and ramp-down arrival-rate profile. Each iteration SHALL create a payment and then move it through a valid lifecycle (confirm or expire, with some confirmed payments refunded) for merchants drawn from a fixed set. Rate, duration and lifecycle proportions SHALL be configurable.

#### Scenario: Default run
- **WHEN** a reader runs the load experiment without overrides on the documented machine
- **THEN** it applies the default arrival rate for the default plateau duration, spreads payments over the fixed merchant set, and reports the number of requests and of successful state changes per kind

#### Scenario: Overridden run
- **WHEN** the experiment runs with a different rate, duration or lifecycle proportion
- **THEN** the load applied and the reported counts reflect the overridden values

### Requirement: Load experiment judges the API under load
The load experiment SHALL fail when more than 1 % of HTTP requests fail or when the 95th percentile request latency is 500 ms or more. It SHALL report the measured error rate, p95 latency and request throughput.

#### Scenario: API within thresholds
- **WHEN** the error rate stays below 1 % and p95 latency stays below 500 ms
- **THEN** the API assertions pass and the measured values are printed

#### Scenario: API overloaded
- **WHEN** the error rate or p95 latency crosses its threshold
- **THEN** the experiment prints `FAIL` naming the threshold, with the measured value

### Requirement: Every accepted request reaches the pipeline under load
After the load ends, the load experiment SHALL wait until the pipeline is drained and SHALL show that the outbox rows written by the run equal the successful state changes the load generator reported, that the distinct events on `pix.payments.v1` equal those outbox rows, that reconciliation processed exactly those events, and that the reconciliation consistency check passes.

#### Scenario: Consistent pipeline after load
- **WHEN** the load ends and every consumer group reaches zero lag within the timeout
- **THEN** successful state changes, outbox rows, distinct topic events and reconciliation's processed events are all equal and the consistency check passes

#### Scenario: Pipeline does not drain
- **WHEN** the consumer groups do not reach zero lag within the timeout
- **THEN** the experiment prints `FAIL` with the remaining lag

### Requirement: Load experiment reports drain time
The load experiment SHALL measure and report how long the pipeline takes, after the last request, until every event of the run is on the topic and until every consumer group reaches zero lag. Drain time SHALL NOT be a failure criterion.

#### Scenario: Drain time reported
- **WHEN** a load run completes
- **THEN** the report includes the time from the end of the load to the topic catching up with the outbox, and to zero consumer lag

### Requirement: Load experiment sabotage adds database contention
With `SABOTAGE=1` the load experiment SHALL repeatedly block the payment API's writes with database lock contention while the load runs. The API thresholds SHALL then fail while the pipeline consistency assertions still pass. No sabotage lock SHALL remain when the experiment ends, whatever its outcome.

#### Scenario: Sabotaged run
- **WHEN** the experiment runs with `SABOTAGE=1`
- **THEN** it prints `FAIL` for the latency or error-rate threshold, the consistency assertions pass for every request the API accepted, and no sabotage lock is held afterwards
