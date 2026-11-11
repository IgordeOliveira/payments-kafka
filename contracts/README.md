# Event contracts

The JSON Schemas in this directory are the only coupling between producers and
consumers. See [docs/architecture.md](../docs/architecture.md) for topics, keys
and lifecycle rules.

| File | Describes |
| --- | --- |
| `payment-event.v1.schema.json` | value of `pix.payments.v1` |
| `fraud-alert.v1.schema.json` | value of `pix.fraud-alerts.v1` |
| `examples/payment-events.valid.json` | array of events that must validate |
| `examples/payment-events.invalid.json` | array of `{reason, event}` that must be rejected |
| `examples/fraud-alerts.valid.json` | array of alerts that must validate |

Rules:

- Schemas use JSON Schema draft 2020-12. Validators must enable `format`
  assertions (`uuid`, `date-time`).
- Unknown properties are allowed. Consumers ignore fields they do not know.
- A breaking change means a new schema file and a new topic (`...v2`), never an
  edit to a published `v1` schema.
- Producers and consumers load these files in their tests so the examples stay
  the shared truth.
