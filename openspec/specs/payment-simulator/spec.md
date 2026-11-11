# payment-simulator Specification

## Purpose
Generates simulated Pix payment traffic for many merchants at a configurable rate, either through the payment API or directly to Kafka with deliberate fault injection.

## Requirements

### Requirement: Lifecycle generation
The simulator SHALL generate payments for a configurable number of merchants, where each payment is created and then either confirmed or expired, and a configurable share of confirmed payments is later refunded.

#### Scenario: Lifecycle shape
- **WHEN** the simulator runs to completion with a fixed payment limit and no fault injection
- **THEN** every payment has exactly one created step, at most one of confirmed or expired, and a refund only after a confirmation

#### Scenario: Reproducible run
- **WHEN** the simulator is run twice with the same seed and configuration
- **THEN** both runs produce the same counts per event type

### Requirement: Configurable rate
The simulator SHALL emit lifecycle steps at the configured rate per second, defaulting to 50.

#### Scenario: Rate is respected
- **WHEN** the simulator runs for 10 seconds at a configured rate of 50 per second
- **THEN** the number of emitted steps is within 20 percent of 500

### Requirement: Chaos mode publishes directly to Kafka
In chaos mode the simulator SHALL publish payment events directly to `pix.payments.v1` with `merchant_id` as the record key, and events that are not deliberately invalid SHALL validate against the payment event schema.

#### Scenario: Events are keyed and valid
- **WHEN** the simulator runs in chaos mode with all fault rates set to zero
- **THEN** every record on `pix.payments.v1` has its `merchant_id` as key and a value that validates against `contracts/payment-event.v1.schema.json`

### Requirement: Duplicate injection
In chaos mode the simulator SHALL resend already published events unchanged at the configured duplicate rate, and SHALL count how many duplicates it sent.

#### Scenario: Duplicates share the event id
- **WHEN** the simulator runs in chaos mode with a duplicate rate above zero
- **THEN** the topic contains records with identical `event_id` and payload, and the reported duplicate count equals the number of extra records

### Requirement: Out-of-order injection
In chaos mode the simulator SHALL, at the configured rate, publish a payment's later event before its earlier event, and SHALL count how many times it did so.

#### Scenario: Later event arrives first
- **WHEN** the simulator runs in chaos mode with an out-of-order rate above zero
- **THEN** for some payments a record with a higher `sequence` precedes the record with the lower `sequence` in the same partition

### Requirement: Invalid message injection
In chaos mode the simulator SHALL, at the configured rate, publish messages that are not valid payment events, including non-JSON payloads and schema violations, and SHALL count them.

#### Scenario: Invalid messages are produced
- **WHEN** the simulator runs in chaos mode with an invalid rate above zero
- **THEN** the topic contains records that fail schema validation, and their number equals the reported invalid count

### Requirement: Normal mode drives the payment API
In normal mode the simulator SHALL perform every lifecycle step by calling the payment API and SHALL NOT publish to Kafka.

#### Scenario: Steps become API calls
- **WHEN** the simulator runs in normal mode against the payment API
- **THEN** each created payment results in one `POST /payments` call and each later step in the matching transition call, and the simulator opens no Kafka connection

#### Scenario: API temporarily unavailable
- **WHEN** the payment API returns connection errors or `5xx` responses for a period
- **THEN** the simulator retries with backoff and continues once the API is available, without crashing

### Requirement: Fraud scenarios
The simulator SHALL be able to generate amount outliers, bursts of payments for a single merchant and refund sprees for a single merchant, each at its own configurable rate.

#### Scenario: Outlier amount
- **WHEN** the outlier rate is above zero
- **THEN** some payments have an amount at least ten times the merchant's typical amount

#### Scenario: Scenarios disabled
- **WHEN** all fraud scenario rates are zero
- **THEN** no outlier, burst or refund spree is generated

### Requirement: Run statistics
The simulator SHALL expose its counters (events per type, duplicates, out-of-order, invalid, API errors) over HTTP and log them periodically.

#### Scenario: Statistics endpoint
- **WHEN** `GET /stats` is called during or after a run
- **THEN** the response contains the current value of every counter
