# Proposal

## Why

The pipeline needs realistic traffic and, more importantly, realistic trouble.
A simulator must generate the payment lifecycle for many merchants at a
configurable rate, and be able to inject what a real system would block:
duplicates, out-of-order events and invalid messages.

## What Changes

- Add a Spring Boot service `simulator` that generates Pix payment lifecycles
  (created, then confirmed or expired, and sometimes refunded) for a
  configurable set of merchants at a configurable rate.
- **Chaos mode:** publishes events directly to `pix.payments.v1` with Spring
  Kafka and injects duplicates, out-of-order events and invalid messages.
- **Normal mode:** drives the payment API over HTTP, so every event flows
  through API, outbox, Debezium and Kafka.
- Fraud scenarios (amount outliers, payment bursts, refund sprees) so the
  antifraud rules have something to detect.

## Capabilities

### New Capabilities
- `payment-simulator`: traffic generation in normal and chaos modes with configurable rate and fault injection.

### Modified Capabilities

None.

## Impact

- New directory `services/simulator/` (Java 21, Spring Boot, Spring Kafka, Maven, Dockerfile).
- Produces to `pix.payments.v1` (chaos mode) and calls `payment-api` (normal mode).
