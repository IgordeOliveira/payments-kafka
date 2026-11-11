# Tasks

## 1. Contract files

- [x] 1.1 Write `contracts/payment-event.v1.schema.json`; verified by parsing it as JSON and checking the required fields against the spec
- [x] 1.2 Write `contracts/fraud-alert.v1.schema.json`; verified by parsing it as JSON and checking the required fields against the spec
- [x] 1.3 Add valid and invalid example files under `contracts/examples/`; verified by parsing each as JSON
- [x] 1.4 Document the contract rules in `contracts/README.md`; verified by the file listing every schema and example

## 2. Shared documentation

- [x] 2.1 Write `docs/architecture.md` with topics, keys, lifecycle, retry/DLQ headers, outbox table, HTTP API, ports and environment variables; verified by each downstream change referencing it without redefining a convention
