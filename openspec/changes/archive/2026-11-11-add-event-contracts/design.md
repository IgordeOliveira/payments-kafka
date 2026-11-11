# Design

## Context

Schema Registry and Avro are out of scope. The contract still has to be
enforceable from both Java and TypeScript. See `docs/architecture.md`.

## Goals / Non-Goals

**Goals:**
- One contract definition, readable by both stacks, with shared test fixtures.
- Make ordering and idempotency possible from the payload alone.

**Non-Goals:**
- Runtime schema distribution or compatibility checks by a registry.
- Partial refunds or any payment state beyond the four lifecycle events.

## Decisions

- **JSON Schema draft 2020-12 files in `contracts/`** over Avro: no registry
  needed, and both Ajv (TypeScript) and networknt json-schema-validator (Java)
  support it.
- **`event_id` as the idempotency key and `sequence` for ordering.** Consumers
  can dedupe and detect gaps without relying on Kafka offsets, which change
  across topics (retry) and replays.
- **`sequence` is tied to `event_type` in the schema** (`if/then`), so an event
  with an impossible sequence is rejected as invalid rather than treated as
  out of order.
- **Unknown properties allowed.** Forward compatibility: adding an optional
  field is not a breaking change. Breaking changes get a new `v2` file and topic.
- **Deterministic `alert_id` (`<rule>:<triggered_by_event_id>`)** so replaying
  the payments topic regenerates identical alerts that downstream can dedupe.

## Risks / Trade-offs

- [Schemas can drift from code] → every component validates the shared
  examples in its own test suite.
- [No registry means nothing stops a producer from publishing garbage] → that
  is intentional: chaos mode does exactly this and consumers must route invalid
  messages to the DLQ.
