# Proposal

## Why

All three consumers need the same hard parts: manual offset commits,
validation against the contract, retry with backoff, a dead letter queue and
clean behavior during rebalances. Implementing them once, in a shared library,
keeps each service focused on its own logic and keeps the reliability
behavior identical and testable in one place.

## What Changes

- Add the npm workspaces root (`package.json`, `tsconfig.base.json`).
- Add the shared TypeScript package `@payments-kafka/consumer-runtime` that:
  - runs a consumer loop with manual commits after handling (at-least-once);
  - validates records against the JSON Schema contracts;
  - routes handler failures to the retry topic with exponential backoff, and
    to the DLQ when attempts are exhausted or the message is invalid;
  - consumes the retry topic, handling only the records owned by its group;
  - shuts down gracefully and logs rebalances;
  - exposes typed events, a JSON logger and configuration loading.

## Capabilities

### New Capabilities
- `consumer-runtime`: the shared consumer loop with manual commits, contract validation, retry with backoff, DLQ routing and graceful shutdown.

### Modified Capabilities

None.

## Impact

- New files: root `package.json`, `package-lock.json`, `tsconfig.base.json`, `packages/consumer-runtime/**`.
- The three consumer changes depend on this package's public API.
- Adds `@confluentinc/kafka-javascript`, Ajv, Vitest and Testcontainers as dependencies.
