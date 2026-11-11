# Proposal

## Why

Writing to the database and publishing to Kafka are two separate systems; doing
both directly loses or invents events when one of them fails. The payment API
must record a payment and its event atomically, and never talk to Kafka, so
that events survive a Kafka outage.

## What Changes

- Add a Spring Boot service `payment-api` exposing a minimal HTTP API to create
  a payment and move it through its lifecycle (confirm, expire, refund).
- Every state change writes the payment row and an `outbox` row in the same
  database transaction.
- The service has no Kafka dependency at all; Debezium publishes the outbox.

## Capabilities

### New Capabilities
- `payment-api`: the HTTP API for the payment lifecycle and its transactional outbox.

### Modified Capabilities

None.

## Impact

- New directory `services/payment-api/` (Java 21, Spring Boot, Maven, Dockerfile).
- Owns tables `payments` and `outbox` in database `payments` via migrations.
- The simulator (normal mode) is its only client.
