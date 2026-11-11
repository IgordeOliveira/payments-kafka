# Tasks

## 1. Workspace setup

- [ ] 1.1 Create the root `package.json` (npm workspaces: `packages/*`, `services/antifraud`, `services/notification`, `services/reconciliation`; scripts `build`, `test`, `lint`), `tsconfig.base.json` (strict, ES modules, Node 22) and `.nvmrc`; verify `npm install` succeeds with only `packages/consumer-runtime` present
- [ ] 1.2 Scaffold `packages/consumer-runtime` (`@payments-kafka/consumer-runtime`, Vitest, build to `dist/`, exports for the main and `/testing` entry points); verify `npm run build` and `npm test` pass at the root

## 2. Contracts, config, logging

- [ ] 2.1 Implement schema loading and validation (Ajv 2020 + formats) with TypeScript types for `PaymentEvent` and `FraudAlert`; verify with unit tests that every example in `contracts/examples/` is accepted or rejected as labelled
- [ ] 2.2 Implement `loadConfig()` for the environment variables in `docs/architecture.md`; verify with unit tests for defaults and for a clear error on each invalid value
- [ ] 2.3 Implement the JSON logger with standard record fields; verify with a unit test on the emitted fields

## 3. Consumer loop

- [ ] 3.1 Implement test helpers under `/testing` (Kafka in Testcontainers, topic creation, produce and collect); verify with a smoke test that produces and consumes one record
- [ ] 3.2 Implement the consumer loop with manual commits and per-partition sequential handling; verify with integration tests for offset advance after success and in-order, non-concurrent delivery within a partition
- [ ] 3.3 Implement validation routing to the DLQ; verify with an integration test that an invalid record reaches the DLQ with the correct headers and the handler is not called
- [ ] 3.4 Verify redelivery: an integration test that stops a consumer abruptly mid-handler and asserts a second instance receives the same record

## 4. Retry and DLQ

- [ ] 4.1 Implement publishing to the retry topic with headers and exponential backoff, awaited before commit; verify with integration tests for headers on first failure and doubling delays
- [ ] 4.2 Implement retry-topic consumption with ownership filtering and partition pause until due; verify with integration tests that foreign records are skipped and committed, and that a due record is handled only after its delay without a rebalance
- [ ] 4.3 Implement DLQ routing for exhausted retries and `NonRetryableError`; verify with integration tests for both paths and their headers
- [ ] 4.4 Verify no loss on hand-off: a test where the retry produce fails and the source offset stays uncommitted

## 5. Lifecycle

- [ ] 5.1 Implement graceful shutdown on `SIGTERM`/`SIGINT` and rebalance logging; verify with an integration test that a stopped consumer's in-flight record is not redelivered and that assign/revoke are logged when a second instance joins

## 6. Documentation

- [ ] 6.1 Write `packages/consumer-runtime/README.md`: public API with a complete sample service, error types, configuration, the retry/DLQ flow, test helpers, and the Dockerfile pattern services must copy; verify the sample compiles as part of the test suite
