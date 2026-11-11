# Spec Delta

## Purpose

Detects suspicious per-merchant payment patterns in the event stream and publishes fraud alerts that are safe to reprocess.

## ADDED Requirements

### Requirement: Amount outlier rule
The service SHALL publish an `amount_outlier` alert for a `payment.created` event whose amount exceeds the configured factor times the merchant's mean payment amount, once the configured minimum number of payments has been seen for that merchant.

#### Scenario: Outlier after warm-up
- **WHEN** a merchant has 20 payments of about 5000 cents and then a payment of 990000 cents is created, with factor 5 and minimum samples 20
- **THEN** one `amount_outlier` alert is published for that payment

#### Scenario: Not enough history
- **WHEN** a large payment is created for a merchant with fewer payments than the minimum samples
- **THEN** no alert is published

### Requirement: Payment burst rule
The service SHALL publish a `payment_burst` alert when the number of `payment.created` events for one merchant inside the configured sliding window exceeds the configured limit, and SHALL NOT repeat the alert until the count has fallen back to the limit or below.

#### Scenario: Burst detected once
- **WHEN** 40 payments are created for one merchant within one second, with limit 30 and a 10 second window
- **THEN** exactly one `payment_burst` alert is published for that merchant

#### Scenario: Normal pace
- **WHEN** a merchant's payments stay at or below the limit in every window
- **THEN** no `payment_burst` alert is published

### Requirement: Refund excess rule
The service SHALL publish a `refund_excess` alert when the number of `payment.refunded` events for one merchant inside the configured sliding window exceeds the configured limit, and SHALL NOT repeat the alert until the count has fallen back to the limit or below.

#### Scenario: Refund spree
- **WHEN** 8 refunds occur for one merchant within 10 seconds, with limit 5 and a 60 second window
- **THEN** exactly one `refund_excess` alert is published for that merchant

### Requirement: Rules are evaluated per merchant
The service SHALL keep rule state separately for each merchant, so that activity of one merchant never triggers or suppresses an alert for another.

#### Scenario: Two merchants below the limit
- **WHEN** two merchants each create 20 payments in the same window, with limit 30
- **THEN** no `payment_burst` alert is published

### Requirement: Alert contract
Every alert SHALL be published to `pix.fraud-alerts.v1` with the merchant's `merchant_id` as key, SHALL validate against `contracts/fraud-alert.v1.schema.json`, and SHALL have `alert_id` equal to `<rule>:<triggered_by_event_id>`.

#### Scenario: Alert shape
- **WHEN** a rule fires for an event
- **THEN** the published record has key `merchant_id`, validates against the schema, and its `alert_id` is the rule name, a colon and the triggering `event_id`

### Requirement: Duplicate events do not double count
The service SHALL ignore a payment event whose `event_id` it has already applied for that merchant, so that redelivered events neither change rule state nor produce additional alerts.

#### Scenario: Duplicated stream
- **WHEN** every event of a stream is delivered twice
- **THEN** the set of published `alert_id` values is the same as for the stream delivered once

### Requirement: Deterministic replay
The service SHALL evaluate rules on the event's `occurred_at` rather than processing time, so that reprocessing the same ordered stream from the beginning produces the same alerts.

#### Scenario: Replay from the earliest offset
- **WHEN** the service processes a topic, its offsets are reset to the earliest position and it processes the topic again from empty state
- **THEN** the second pass publishes alerts with the same `alert_id` values as the first

### Requirement: Alert is durable before the offset is committed
The service SHALL treat an event as handled only after every alert it triggered has been acknowledged by Kafka, and SHALL treat a failed alert publish as a retryable failure.

#### Scenario: Alert publish fails
- **WHEN** publishing an alert fails
- **THEN** the triggering event follows the retry path and the alert is published when the retry succeeds

### Requirement: State follows partition ownership
The service SHALL discard the in-memory state of merchants in partitions it no longer owns and SHALL start from empty state for newly assigned partitions.

#### Scenario: Partition revoked
- **WHEN** a second instance joins and takes over some partitions
- **THEN** the first instance drops the state for merchants in those partitions and the takeover is logged
