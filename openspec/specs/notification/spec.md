# notification Specification

## Purpose
Notifies restaurants about payment outcomes and fraud alerts, guaranteeing that the same event or alert never produces more than one notification.

## Requirements

### Requirement: Payment notifications
The service SHALL emit one notification for each `payment.confirmed`, `payment.expired` and `payment.refunded` event, addressed to the event's merchant, and SHALL NOT emit a notification for `payment.created`.

#### Scenario: Payment confirmed
- **WHEN** a `payment.confirmed` event for `merchant-007` is consumed
- **THEN** one notification of kind `payment.confirmed` for `merchant-007` is written to the console

#### Scenario: Payment created
- **WHEN** a `payment.created` event is consumed
- **THEN** no notification is emitted and the record is committed

### Requirement: Fraud alert notifications
The service SHALL emit one notification for each fraud alert consumed from `pix.fraud-alerts.v1`, addressed to the alert's merchant and naming the rule.

#### Scenario: Alert received
- **WHEN** a `payment_burst` alert for `merchant-012` is consumed
- **THEN** one notification of kind `fraud_alert` naming `payment_burst` for `merchant-012` is written to the console

### Requirement: Never notify twice
The service SHALL emit at most one notification per payment `event_id` and per `alert_id`, regardless of duplicates on the topic, redelivery, retries, restarts or the number of running instances.

#### Scenario: Duplicate event on the topic
- **WHEN** the same `payment.confirmed` event is published twice
- **THEN** exactly one notification is emitted

#### Scenario: Redelivery after a crash
- **WHEN** the service is killed after emitting a notification but before committing the offset, and is restarted
- **THEN** the redelivered event does not produce a second notification

#### Scenario: Two instances race
- **WHEN** two instances process the same event concurrently
- **THEN** exactly one of them emits the notification

#### Scenario: Regenerated alert
- **WHEN** the antifraud service republishes an alert with an `alert_id` that was already notified
- **THEN** no second notification is emitted

### Requirement: Dedupe survives restarts
The record of sent notifications SHALL be stored durably so that it is still honored after every instance of the service has been stopped and started again.

#### Scenario: Full restart
- **WHEN** all instances are stopped, and an already notified event is delivered again after restart
- **THEN** no notification is emitted

### Requirement: Notification output format
Each notification SHALL be written to standard output as a single JSON line containing `type` equal to `notification`, the `kind`, the `merchant_id`, the dedupe key and a human-readable `message`.

#### Scenario: Countable output
- **WHEN** 10 distinct confirmed events are consumed
- **THEN** standard output contains exactly 10 lines whose `type` is `notification` and whose `kind` is `payment.confirmed`

### Requirement: Storage failures are retried
When the dedupe store is unavailable the service SHALL NOT emit the notification and SHALL treat the record as a retryable failure.

#### Scenario: Database unavailable
- **WHEN** an event is consumed while Postgres is unreachable
- **THEN** no notification is emitted and the record is sent to the retry topic

#### Scenario: Database recovers
- **WHEN** Postgres becomes reachable before retries are exhausted
- **THEN** the retried event produces exactly one notification
