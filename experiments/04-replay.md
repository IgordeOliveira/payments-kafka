# Experiment 4: replaying the topic rebuilds identical balances

```sh
docker compose --profile experiments run --rm experiments replay
```

**Property.** A topic is a log, not a queue: consuming a record does not
remove it. A consumer group's position is just a committed offset, and it can
be moved back. If a consumer's state is a pure function of the events it
applied, you can throw the state away, rewind the offsets to the beginning and
rebuild exactly the same state. That is how a bug fix is rolled out to
historical data, or a new read model is built from history.

**How it breaks things on purpose.** It deletes reconciliation's whole state
and makes the group read the topic again from offset 0 — duplicates,
out-of-order events, invalid records and all.

## What the script does

1. Resets the pipeline and builds state with a chaos run (seed `404`, 800
   payments, the simulator's default duplicate, out-of-order and invalid
   rates). Waits until reconciliation is drained.
2. Exports `balances` (**before**) and runs `check`.
3. The replay procedure from
   [services/reconciliation/README.md](../services/reconciliation/README.md#replay-procedure):
   1. stop every reconciliation instance and wait until the group is `Empty`
      (offsets can only be reset on an inactive group);
   2. `reset-state --yes` (truncates `processed_events`, `payments`,
      `ledger_entries`, `merchant_balances`);
   3. `kafka-consumer-groups.sh --group reconciliation --reset-offsets
      --to-earliest --topic pix.payments.v1 --execute`;
   4. start reconciliation and wait until it is drained, retry topic included
      (out-of-order events are deferred through it again).
4. Exports `balances` (**after**), runs `check`, compares byte for byte.

| Assertion | Sources compared |
| --- | --- |
| the export before is not empty | merchants in the export |
| before and after: every distinct event applied | `processed_events` = `events_sent_total` |
| before and after: `check` passes | balances vs ledger |
| the replay drained | lag 0 on `pix.payments.v1` and the retry topic |
| balances after = balances before, byte for byte | `cmp` of the two exports |

Variables: `SEED` (404), `PAYMENTS` (800), `RATE` (100).

## Measured (clean stack, defaults)

```
simulator events_sent_total                      1642
before: processed_events / merchants             1642 / 20
before: export sha256                            7380df935f16e0d6
committed offsets after the rewind (sum over pix.payments.v1) 0
after: processed_events / merchants              1642 / 20
after: export sha256                             7380df935f16e0d6
PASS replay
```

The replay itself (rewind to drained) takes about 10 seconds; the whole
experiment about a minute.

## Why it holds, and the trap

The `balances` export leaves out timestamps and sorts by `merchant_id`, so it
is a pure function of the set of applied events. Each event is applied once
whatever the delivery order (sequence rules plus the retry topic), so
replaying the same log gives the same set and the same export.

**Resetting offsets alone is a no-op.** Without step 2, every replayed event
is already in `processed_events` and is ignored as a duplicate: idempotency
doing its job. A rebuild needs both the state reset and the rewind, in that
order, with the consumers stopped.

## What to watch while it runs

- Kafka UI → **Consumers → reconciliation**: during step 3 the group is
  `EMPTY` and, right after the rewind, its lag jumps to the size of the topic;
  then it drains to 0.
- `docker compose logs -f reconciliation`: a burst of `payment event applied`
  and, for the duplicated records in the topic, `duplicate event ignored`.

## Seeing it fail

```sh
docker compose --profile experiments run --rm -e SABOTAGE=1 experiments replay
```

The sabotage rewinds to offset 10 of each partition instead of the earliest
offset, a realistic operator mistake. Measured: 60 records were skipped
(committed offsets after the rewind sum to 60 instead of 0), only 1563 of
1642 events were applied again (a later event whose predecessor was skipped
is a gap: it goes through its retries and then to the DLQ), the `sha256` of the export changes and the
experiment prints `FAIL replay` with exit status 1.
