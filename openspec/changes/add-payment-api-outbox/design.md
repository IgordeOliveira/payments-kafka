# Design

## Context

The HTTP contract, the payment representation, the outbox DDL and the event
contract are fixed in `docs/architecture.md` and `contracts/`. Debezium (from
`add-local-environment`) reads the outbox; this service only writes rows.

## Goals / Non-Goals

**Goals:**
- Atomic write of payment state and its event.
- Events that validate against `contracts/payment-event.v1.schema.json`.
- Keep accepting payments while Kafka is down.

**Non-Goals:**
- Authentication, idempotency keys on HTTP requests, partial refunds.
- Publishing to or reading from Kafka.

## Decisions

- **Spring Boot 3, Java 21, Maven, Spring Web + Spring Data JDBC (or
  JdbcTemplate) + Flyway.** JPA adds little for two tables; plain SQL keeps the
  transaction boundary obvious, which is the point of the exercise.
- **No Kafka client on the classpath.** The outbox pattern is only honest if
  the service cannot publish. A test asserts the dependency is absent.
- **One `@Transactional` service method per state change**: lock the payment
  row (`SELECT ... FOR UPDATE`), validate the transition, update the payment,
  insert the outbox row. Concurrent transitions on one payment are serialized
  by the row lock, so `sequence` cannot be duplicated.
- **`outbox.id` equals the event's `event_id`**, and `payload` is the complete
  event JSON, so what Debezium publishes is exactly what was committed.
- **Outbox rows are kept**, not deleted after insert. Debezium only needs the
  insert in the WAL, but keeping rows makes the pattern inspectable. A cleanup
  job is out of scope.
- **Transition rules live in one small domain type** (status + allowed next
  statuses + resulting event type and sequence), unit tested in isolation.
- **Errors as RFC 7807 problem details**: 400 validation, 404 unknown payment,
  409 invalid transition.
- **Integration tests with Testcontainers Postgres**; the generated outbox
  payloads are validated against the shared JSON Schema in tests.
- **Multi-stage Dockerfile** (Maven build, JRE 21 runtime), actuator health on
  `/actuator/health`.

## Risks / Trade-offs

- [The outbox table grows forever] → acceptable for a study project; noted in
  the service README.
- [Schema drift between Flyway DDL and the DDL in `docs/architecture.md`] → a
  test asserts the outbox column names and types match the documented contract.
