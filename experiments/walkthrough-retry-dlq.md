# Walkthrough: retry with backoff and the dead letter queue

**Lesson.** A consumer that cannot handle a record has three options: block
the partition until it can (everything behind it waits), drop the record
(data loss), or move it aside. This pipeline moves it aside, following the
protocol in [docs/architecture.md](../docs/architecture.md#retry-and-dlq-protocol):

- a record that can never succeed (not JSON, violates the contract, or a
  handler throws a non-retryable error) goes straight to
  `pix.payments.dlq.v1`;
- a record whose handler throws is republished to `pix.payments.retry.v1`
  with headers saying who owns the retry, which attempt it is and when it may
  run again (1 s, 2 s, 4 s, 8 s); after the last attempt it goes to the DLQ.

Either way the original offset is committed and the partition keeps flowing.
Every DLQ record carries headers that explain where it came from and why it
gave up. You will produce three bad records by hand and follow each one.

Time: 15 minutes. You look; nothing is asserted.

## 1. Start from a known state

From the repository root, with the pipeline running:

```sh
docker compose --profile experiments run --rm experiments reset
docker compose --profile experiments run --rm experiments shell
```

`reset` empties the topics (so the DLQ only holds what you produce here) and
stops the default simulator. **Every command below runs in that shell.**
Two helpers to build events that follow the contract:

```sh
uuid() { cat /proc/sys/kernel/random/uuid; }
event() {  # event <event_type> <sequence> <payment_id>
  jq -cn --arg id "$(uuid)" --arg type "$1" --argjson seq "$2" --arg pid "$3" \
    '{event_id: $id, event_type: $type, schema_version: 1, occurred_at: (now | todate),
      payment_id: $pid, merchant_id: "merchant-dlq", sequence: $seq, amount_cents: 1000, currency: "BRL"}'
}
publish() { kcat -P -b kafka:29092 -t pix.payments.v1 -k merchant-dlq 2>/dev/null; }
```

`publish` sends each line it reads as one record with key `merchant-dlq`.

## 2. An invalid message: straight to the DLQ

```sh
echo 'this is not json' | publish
sleep 3
kcat -C -b kafka:29092 -t pix.payments.dlq.v1 -o beginning -e -q \
  -f '--- offset %o, key %k\nheaders: %h\nvalue:   %s\n' 2>/dev/null
```

Three DLQ records with the same key and value, one per consumer group: each
group validates on its own and dead-letters on its own. Read the headers of
one of them:

| Header | Value | Meaning |
| --- | --- | --- |
| `x-dlq-group` | `antifraud`, `notification` or `reconciliation` | who gave up |
| `x-dlq-reason` | `invalid_message` | not JSON or schema violation: never retried |
| `x-dlq-attempts` | `0` | it never went through the retry topic |
| `x-original-topic` / `-partition` / `-offset` | `pix.payments.v1`, its partition and offset | where to find the original |
| `x-error-message` | `value is not valid JSON: ...` | the last error |

A schema violation looks the same, with the failing rule in
`x-error-message`:

```sh
event payment.created 1 "$(uuid)" | jq -c '.currency = "USD"' | publish
```

In Kafka UI (http://localhost:8080): **Topics → pix.payments.dlq.v1 →
Messages**, click a record and open its **Headers**.

## 3. A failing handler: retried with backoff, then dead-lettered

Publish a confirmation for a payment that was never created. Reconciliation
cannot apply sequence 2 without sequence 1 (a "gap"), so its handler throws a
retryable error, hoping the creation is only late:

```sh
event payment.confirmed 2 "$(uuid)" | publish
```

Follow the retry topic live for about 20 seconds (Ctrl+C to stop):

```sh
kcat -C -b kafka:29092 -t pix.payments.retry.v1 -o beginning -q \
  -f '%T  %h\n' 2>/dev/null | tr ',' '\n' | grep -E '^[0-9]|x-retry-(group|attempt|not-before)'
```

Four retry records, all with `x-retry-group=reconciliation`, attempts 1 to 4,
and `x-retry-not-before` 1, 2, 4 and 8 seconds after each failure. The other
groups read these records too and skip them (they are not theirs), which is
why every group shows a little lag on the retry topic in Kafka UI. Then:

```sh
kcat -C -b kafka:29092 -t pix.payments.dlq.v1 -o beginning -e -q -f '%h\n' 2>/dev/null \
  | tr ',' '\n' | grep -E 'x-dlq-(group|reason|attempts)' | paste - - - | tail -1
```

`x-dlq-group=reconciliation  x-dlq-reason=retries_exhausted  x-dlq-attempts=4`.
The reconciliation logs show each step:

```sh
docker logs payments-kafka-reconciliation-1 2>&1 | grep -E 'retry topic|sent to DLQ' | tail -5 | jq -c '{msg, attempt, reason, err}'
```

Notification, on the other hand, handled the same record successfully (it
notifies a confirmation without needing the creation): retries are per group.
Had the `payment.created` arrived within those 15 seconds, the retry would
have applied the confirmation, which is exactly how reconciliation absorbs
the simulator's out-of-order events.

## 4. A non-retryable error: no retries

Reconciliation refuses a transition that contradicts what it already applied
(a payment confirmed and then expired, with a new `event_id`). Retrying cannot
fix that, so the handler throws a non-retryable error:

```sh
P=$(uuid)
{ event payment.created 1 "$P"; event payment.confirmed 2 "$P"; event payment.expired 2 "$P"; } | publish
sleep 3
kcat -C -b kafka:29092 -t pix.payments.dlq.v1 -o beginning -e -q -f '%h\n' 2>/dev/null \
  | tr ',' '\n' | grep -E 'x-dlq-(group|reason|attempts)' | paste - - - | tail -1
```

`x-dlq-reason=non_retryable`, `x-dlq-attempts=0`, and `x-error-message`
explains the conflict.

## 5. What to do with a DLQ

Nothing consumes `pix.payments.dlq.v1` automatically, on purpose: a record
there needs a human (or a dedicated tool) to decide. The headers give
everything needed to find the original record and the reason. Once the cause
is fixed, the value can be republished to `pix.payments.v1` with its original
key; every consumer is idempotent on `event_id`, so republishing an event
that was partly handled is safe.

## Clean up

```sh
exit
```

Run `docker compose --profile experiments run --rm experiments reset` to
empty the topics again, or `docker compose --profile apps start simulator` to
resume the default traffic.
