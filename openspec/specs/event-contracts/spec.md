# event-contracts Specification

## Purpose
Defines the versioned JSON contracts for payment events and fraud alerts, which are the only coupling between producers and consumers of the pipeline.

## Requirements

### Requirement: Payment event schema
The project SHALL publish a JSON Schema for the value of `pix.payments.v1` requiring `event_id`, `event_type`, `schema_version`, `occurred_at`, `payment_id`, `merchant_id`, `sequence`, `amount_cents` and `currency`.

#### Scenario: Valid lifecycle events are accepted
- **WHEN** each event in `contracts/examples/payment-events.valid.json` is validated against the payment event schema
- **THEN** every event validates successfully

#### Scenario: Malformed events are rejected
- **WHEN** each event in `contracts/examples/payment-events.invalid.json` is validated against the payment event schema
- **THEN** every event fails validation

### Requirement: Lifecycle sequence
The payment event schema SHALL bind `sequence` to `event_type`: `payment.created` is 1, `payment.confirmed` and `payment.expired` are 2, and `payment.refunded` is 3.

#### Scenario: Sequence does not match the event type
- **WHEN** a `payment.confirmed` event with `sequence` 1 is validated
- **THEN** validation fails

### Requirement: Forward compatibility
The schemas SHALL allow properties they do not define, so that adding an optional field does not break existing consumers.

#### Scenario: Event with an unknown field
- **WHEN** an otherwise valid payment event containing an extra property is validated
- **THEN** validation succeeds

### Requirement: Fraud alert schema
The project SHALL publish a JSON Schema for the value of `pix.fraud-alerts.v1` requiring `alert_id`, `schema_version`, `rule`, `merchant_id`, `payment_id`, `triggered_by_event_id`, `occurred_at` and `details`, where `rule` is one of `amount_outlier`, `payment_burst` or `refund_excess`.

#### Scenario: Valid alerts are accepted
- **WHEN** each alert in `contracts/examples/fraud-alerts.valid.json` is validated against the fraud alert schema
- **THEN** every alert validates successfully

#### Scenario: Unknown rule is rejected
- **WHEN** an alert whose `rule` is `magic` is validated
- **THEN** validation fails

### Requirement: Record key
Every record on `pix.payments.v1` and `pix.fraud-alerts.v1` SHALL use the `merchant_id` of its value as the UTF-8 string record key.

#### Scenario: Events of one merchant share a partition
- **WHEN** two payment events with the same `merchant_id` are published with that `merchant_id` as key
- **THEN** both records are written to the same partition in publish order
