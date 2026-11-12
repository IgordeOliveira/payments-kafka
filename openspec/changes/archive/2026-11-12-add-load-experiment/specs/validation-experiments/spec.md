# Spec Delta

## ADDED Requirements

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
