# Tasks

## 1. Project setup

- [x] 1.1 Scaffold `services/simulator` (Spring Boot 3, Java 21, Maven wrapper, Web, Actuator, Spring Kafka); verify `./mvnw -q verify` passes on the empty project
- [x] 1.2 Bind all `SIMULATOR_*` variables from design.md plus `KAFKA_BOOTSTRAP_SERVERS`, `PAYMENT_API_BASE_URL`, `SERVER_PORT` (default 8082) to a validated configuration class; verify with a test for defaults and for rejection of out-of-range rates

## 2. Lifecycle generator

- [x] 2.1 Implement the seeded lifecycle generator (merchants, amounts, created then confirmed or expired, optional refund) behind a `PaymentSink` interface; verify with unit tests on lifecycle shape and on identical output for identical seeds
- [x] 2.2 Implement rate control and the optional payment limit; verify with a test using a fake clock that the emitted step count matches the configured rate
- [x] 2.3 Implement fraud scenarios (outlier, burst, refund spree); verify with unit tests that each appears when its rate is 1 and never when it is 0

## 3. Chaos mode

- [x] 3.1 Implement `KafkaSink` (key `merchant_id`, `acks=all`, idempotent producer, JSON value built from the contract); verify with a Testcontainers Kafka test that records are keyed and validate against `contracts/payment-event.v1.schema.json`
- [x] 3.2 Implement duplicate, out-of-order and invalid injection with one counter each; verify with Testcontainers tests that topic contents match the counters for each fault

## 4. Normal mode

- [x] 4.1 Implement `ApiSink` calling the payment API per `docs/architecture.md`, with retry and backoff on connection errors and 5xx, and 409 counted and dropped; verify with a stubbed HTTP server test covering success, retry and 409
- [x] 4.2 Verify with a test that no Kafka producer is created when `SIMULATOR_MODE=normal`

## 5. Observability, packaging, docs

- [x] 5.1 Implement counters, `GET /stats` and periodic logging; verify with a test asserting the endpoint reflects emitted events
- [x] 5.2 Add a multi-stage `Dockerfile` (build context `services/simulator`); verify `docker build services/simulator` succeeds
- [x] 5.3 Write `services/simulator/README.md` (modes, every variable, examples for a clean run and a chaos run, how to run tests); verify the documented commands run as written
