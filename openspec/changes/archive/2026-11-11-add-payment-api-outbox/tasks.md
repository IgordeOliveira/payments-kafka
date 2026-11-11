# Tasks

## 1. Project setup

- [x] 1.1 Scaffold `services/payment-api` (Spring Boot 3, Java 21, Maven wrapper, Web, JDBC, Flyway, Actuator, Validation, Postgres driver; no Kafka dependency); verify `./mvnw -q verify` passes on the empty project
- [x] 1.2 Add configuration from environment variables per `docs/architecture.md` (`SPRING_DATASOURCE_*`, `SERVER_PORT` default 8081); verify the context-load test passes against Testcontainers Postgres

## 2. Schema

- [x] 2.1 Add Flyway migration creating `payments` and `outbox` (outbox exactly as in `docs/architecture.md`); verify with an integration test that asserts the outbox column names and types

## 3. Domain and outbox

- [x] 3.1 Implement the lifecycle transition rules (status, allowed transitions, resulting event type and sequence); verify with unit tests covering every valid and invalid transition
- [x] 3.2 Implement the transactional service: row lock, transition, payment update, outbox insert with the full event payload; verify with integration tests that payment and outbox rows commit together and that a forced outbox failure rolls back the payment
- [x] 3.3 Validate generated payloads against `contracts/payment-event.v1.schema.json` in a test covering created, confirmed, expired and refunded

## 4. HTTP API

- [x] 4.1 Implement `POST /payments`, `POST /payments/{id}/confirm|expire|refund`, `GET /payments/{id}` with the representation from `docs/architecture.md`; verify with MockMvc or integration tests for each success path
- [x] 4.2 Implement error handling as problem details (400, 404, 409); verify with tests for invalid body, unknown id and invalid transition
- [x] 4.3 Add a concurrency test sending simultaneous confirms for one payment; verify exactly one succeeds and one outbox row is written
- [x] 4.4 Add a test asserting no Kafka client class is on the classpath

## 5. Packaging and docs

- [x] 5.1 Add a multi-stage `Dockerfile` (build context `services/payment-api`) and expose actuator health; verify `docker build services/payment-api` succeeds and the container reports healthy against a Postgres container
- [x] 5.2 Write `services/payment-api/README.md` (purpose, endpoints with curl examples, configuration, how to run tests, the outbox-growth note); verify the documented commands run as written
