# Experiment 2: a consumer killed mid-processing loses nothing

```sh
docker compose --profile experiments run --rm experiments kill-consumer
```

**Property.** A consumer commits its offset only after a record is fully
handled (manual commits, never auto-commit). If it dies at any point, the
records after its last commit are delivered again, to itself after a restart
or to another member of the group after a rebalance. Nothing is skipped
(**at-least-once**), and because reconciliation applies each event in one
database transaction together with its `processed_events` row, nothing is
applied twice either (**exactly once in effect**).

**How it breaks things on purpose.** Three reconciliation instances share the
partitions. While a fast chaos run is being consumed, one instance gets
`SIGKILL` (`docker kill`): no shutdown hook, no offset commit, no "leave
group". It is started again five seconds later and rejoins as a new member.

## What the script does

1. Resets the pipeline with reconciliation scaled to **3 instances**.
2. Starts a chaos run: seed `202`, 2000 payments at 300 steps per second, the
   simulator's default duplicate, out-of-order and invalid rates. At this
   rate the consumers have a backlog.
3. As soon as reconciliation has applied 500 events, `docker kill` a random
   instance, measure the lag 5 s later, and `docker start` it again.
4. Waits for the simulator to finish and for the group to drain (lag 0 on
   `pix.payments.v1` and the retry topic). The dead member's partitions are
   stuck until the group coordinator gives up on it (session timeout, 45 s by
   default), then the group rebalances and they drain.
5. Measures and asserts, then scales reconciliation back to 1.

| Assertion | Sources compared |
| --- | --- |
| the instance was killed mid-run | events applied at the kill > 0 and < total |
| the group drained after the kill | lag 0, retry topic included |
| the topic holds every event produced | distinct valid `event_id`s on the topic = `events_sent_total` |
| every distinct event applied exactly once | `processed_events` = `events_sent_total` |
| `check` passes | balances vs ledger |
| no valid event dead-lettered | DLQ of group reconciliation holds only the `invalid_message` records (= `chaos.invalid`) |

No antifraud or notification count is asserted here: antifraud keeps its
windows in memory and drops them on every rebalance, so its alerts depend on
timing.

Variables: `SEED` (202), `PAYMENTS` (2000), `RATE` (300), `INSTANCES` (3),
`KILL_AFTER_EVENTS` (500).

## Measured (three consecutive runs on a clean stack, defaults)

| | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| events applied when killed | 698 | 663 | 719 |
| simulator still running when killed | true | true | true |
| reconciliation lag 5 s after the kill | 865 | 931 | 931 |
| simulator `events_sent_total` | 4103 | 4103 | 4103 |
| injected duplicates / out-of-order / invalid | 202 / 33 / 31 | 202 / 33 / 31 | 202 / 33 / 31 |
| distinct valid `event_id`s on the topic | 4103 | 4103 | 4103 |
| `processed_events` | 4103 | 4103 | 4103 |
| `duplicate event ignored` lines | 202 | 202 | 202 |
| DLQ (reconciliation) | 31 `invalid_message` | 31 `invalid_message` | 31 `invalid_message` |
| `check` | ok | ok | ok |
| result | PASS | PASS | PASS |

Each run takes about 90 seconds, most of it waiting for the session timeout.

The script also reports *redelivered after their DB commit*: `duplicate event
ignored` lines beyond the injected duplicates. A record redelivered after the
kill shows up there only if the kill landed between the database commit and
the offset commit, a window of a millisecond or so; a record whose transaction
the kill interrupted is rolled back by Postgres and simply applied on
redelivery, invisible in the counts. It was 0 in the three runs above and 1 in
an earlier run: both outcomes are correct, and the assertions hold either way.

## What to watch while it runs

- Kafka UI → **Consumers → reconciliation**: three members, then for about
  45 seconds the killed member's partitions keep their old consumer id while
  their lag grows; then the group rebalances and the lag drains.
- In the experiment's own output, the `lag:` lines stay at the same non-zero
  value until the session timeout expires, then drop to 0.
- `docker compose logs reconciliation | grep partitions` shows the
  `partitions revoked` / `partitions assigned` pairs of each rebalance (one
  when the killed container comes back, one when the dead member expires).

## Seeing it fail

```sh
docker compose --profile experiments run --rm -e SABOTAGE=1 experiments kill-consumer
```

After the drain, the sabotage deletes one row from `processed_events`, which
is what a lost event looks like from the outside: the consumer "forgot" it.
Measured: `processed_events` 4102 for 4103 events produced, the experiment
prints `FAIL kill-consumer` with exit status 1. (A consumer that committed
offsets before processing, or used auto-commit, would lose the records it had
in flight in the same way.)
