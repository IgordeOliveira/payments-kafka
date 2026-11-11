# Experiment 1: duplicated events are applied once

```sh
docker compose --profile experiments run --rm experiments duplicates
```

**Property.** Kafka delivery here is *at-least-once*: a producer retry, a
connector restart or a consumer crash before its commit puts the same event
in front of a consumer twice. A consumer that applies money movements must
therefore be **idempotent**: applying an event twice must have the same
effect as applying it once. The notification service has the stricter rule
"never tell a merchant twice".

**How it breaks things on purpose.** The simulator, in chaos mode, publishes
extra byte-for-byte copies of recent events (same `event_id`, same key) at a
20 % rate, plus late `payment.created` events (out of order) and invalid
records. Nothing in the consumers is told which records are copies.

## What the script does

1. Resets the pipeline (empty topics, empty state; see
   [README.md](README.md#what-an-experiment-does-to-your-stack)).
2. **Run A**: seed `101`, 400 payments, 100 steps/s, duplicate rate `0.2`,
   out-of-order `0.05`, invalid `0.02`. Waits for the simulator to finish and
   for the three groups to drain (lag 0 on the main, alert and retry topics).
3. Measures run A: simulator counters, records and distinct valid `event_id`s
   on `pix.payments.v1`, reconciliation's `processed_events`, `check` and
   `balances`, the DLQ headers, and every `"type":"notification"` line the
   notification instances wrote since the reset.
4. Resets again and runs **run B**: the same seed with every chaos rate at 0,
   the duplicate-free stream. The simulator's lifecycle decisions and ids come
   from a random generator seeded independently of the chaos generator, so
   both runs produce the same events (checked by an assertion).
5. Exports B's balances and compares.

| Assertion | Sources compared |
| --- | --- |
| the simulator injected duplicates | `chaos.duplicates` > 0 |
| A and B produced the same lifecycle events | `events_sent` of both runs |
| each distinct event applied once | `processed_events` = `events_sent_total` |
| reconciliation `check` passes | stored balances and counters vs the ledger |
| A's balances = B's balances, byte for byte | `balances` export of both runs |
| payment notifications = distinct notifiable events | notification lines vs `events_sent` confirmed + expired + refunded |
| no notification key emitted twice | distinct `notification_key` = lines |
| fraud notifications = distinct alerts | notification lines vs distinct `alert_id` on `pix.fraud-alerts.v1` |
| DLQ holds only the invalid records | `x-dlq-reason=invalid_message` records of group reconciliation = `chaos.invalid`, nothing else |

Variables: `SEED` (101), `PAYMENTS` (400), `RATE` (100), `DUPLICATE_RATE` (0.2).

## Measured (clean stack, defaults)

```
A: simulator events_sent_total (distinct events) 821
A: simulator chaos.duplicates                    153
A: simulator chaos.out_of_order / invalid        21 / 14
A: records on pix.payments.v1                    988      (821 + 153 + 14)
A: distinct valid event_ids on pix.payments.v1   821
A: reconciliation processed_events               821
A: reconciliation 'duplicate event ignored' log lines 153
A: DLQ records (reconciliation)                  invalid_message 14
A: check report                                  {"ok":true,"merchants":20,"ledger_entries":353,"processed_events":821,"mismatches":0}
A: notification lines (payment / fraud_alert)    423 (421 / 2)
A: distinct alert_ids on pix.fraud-alerts.v1     2
B: simulator events_sent_total / duplicates      821 / 0
B: reconciliation processed_events               821
A balances = B balances                          identical (2475 bytes)
PASS duplicates
```

153 copies reached each consumer; reconciliation logged exactly 153
`duplicate event ignored` lines and stored 821 events, and notification sent
421 payment notifications for 332 confirmed + 68 expired + 21 refunded events.
The run takes about 90 seconds.

## What to watch while it runs

- Kafka UI → **Topics → pix.payments.v1 → Messages**: sort by key, open two
  records with the same `event_id` at different offsets. Kafka has no idea
  they are the same; deduplication is the consumer's job.
- `docker compose logs -f reconciliation`: `payment event applied` and, for
  the copies, `duplicate event ignored`. Out-of-order confirmations show up
  as `handler failed; record sent to retry topic` and are applied a second
  later.
- `docker compose logs -f notification`: `notification already claimed; not
  sent again` for every copy of a notifiable event.

## Why it holds

- Reconciliation inserts the `event_id` into `processed_events` **in the same
  database transaction** as the balance change
  ([services/reconciliation/README.md](../services/reconciliation/README.md#applying-an-event)).
  A copy finds the row and changes nothing. Because the effects depend only on
  the *set* of distinct events, run A ends with the same balances as run B.
- Notification **claims** the `event_id` (or `alert_id`) with an
  `INSERT ... ON CONFLICT DO NOTHING` before writing the line
  ([services/notification/README.md](../services/notification/README.md#never-twice-claim-then-send)).
- Antifraud derives `alert_id` from the triggering `event_id`, so even if it
  raised an alert twice, notification would drop the second one.

## Seeing it fail

```sh
docker compose --profile experiments run --rm -e SABOTAGE=1 experiments duplicates
```

After run A, the sabotage applies one duplicate a second time directly in
`merchant_balances` (adds one credit's amount and increments
`confirmed_count`), which is what a consumer without the `processed_events`
check would have done. Measured: `check` exits `1` with 2 mismatches
(`balance_cents` and `confirmed_count` of one merchant), the A/B balance
comparison fails, and the experiment prints `FAIL duplicates` with exit
status 1.
