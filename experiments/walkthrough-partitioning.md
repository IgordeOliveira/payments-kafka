# Walkthrough: keys, partitions and per-merchant order

**Lesson.** A Kafka topic is split into partitions, and order exists only
inside a partition. The producer picks the partition from the record key, so
every record with the same key lands in the same partition, in the order it
was produced. `pix.payments.v1` is keyed by `merchant_id`: all events of one
restaurant are in one partition, in order, and a consumer instance sees them
one at a time. Events of *different* merchants have no order relative to each
other, and that is fine.

Time: 10 minutes. Nothing here is asserted by a script; you look.

## 1. Fill the topic with a known run

From the repository root, with the pipeline running
(`docker compose --profile apps up -d --build --wait`):

```sh
docker compose --profile experiments run --rm experiments reset
docker compose --profile experiments run --rm experiments shell
```

`reset` empties the topics and stops the default simulator (see
[README.md](README.md#what-an-experiment-does-to-your-stack)). The second
command opens a bash shell in the tooling container; **every command below
runs in that shell** (it has `kcat`, `jq`, the Kafka CLI and the Docker CLI).
Start a short chaos-mode run (200 payments, 5 merchants, out-of-order
injection turned up) and wait for it to finish:

```sh
docker compose --profile apps run -d --no-deps --name exp-simulator \
  -e SIMULATOR_MAX_PAYMENTS=200 -e SIMULATOR_MERCHANTS=5 -e SIMULATOR_SEED=11 \
  -e SIMULATOR_CHAOS_OUT_OF_ORDER_RATE=0.1 simulator
until curl -fsS http://exp-simulator:8082/stats | jq -e .finished > /dev/null; do sleep 2; done
curl -s http://exp-simulator:8082/stats | jq '{events_sent_total, chaos}'
```

## 2. The topic and its partitions

```sh
kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic pix.payments.v1
```

Six partitions, each with one leader (the single broker) and replication
factor 1. How many records each one holds:

```sh
kcat -C -b kafka:29092 -t pix.payments.v1 -o beginning -e -q -f '%p\n' 2>/dev/null | sort | uniq -c
```

With 5 merchants and 6 partitions, at least one partition is empty and some
may hold two merchants (here merchant-001 and merchant-003 share
partition 0, partitions 1 and 5 stay empty): the distribution follows the
keys, not the load.

## 3. One merchant, one partition

Print which partitions each key was written to:

```sh
kcat -C -b kafka:29092 -t pix.payments.v1 -o beginning -e -q -f '%k %p\n' 2>/dev/null | sort -u
```

Every `merchant-00N` appears with exactly **one** partition. The producer (the
Java client in the simulator, and in Debezium on the outbox path) computes
`murmur2(key) mod 6`, so the mapping is stable as long as the partition count
does not change. Adding partitions to a keyed topic moves keys to other
partitions and breaks per-key order for the records in flight: decide the
count up front.

In Kafka UI (http://localhost:8080): **Topics → pix.payments.v1 → Messages**.
Each row shows `Partition`, `Offset` and `Key`; filter by a key (search box) to
see that one merchant only ever shows one partition.

## 4. Order inside the partition

Take one merchant's partition and print its records in offset order:

```sh
P=$(kcat -C -b kafka:29092 -t pix.payments.v1 -o beginning -e -q -f '%k %p\n' 2>/dev/null | awk '$1 == "merchant-001" { print $2; exit }')
kcat -C -b kafka:29092 -t pix.payments.v1 -p "$P" -o beginning -e -q -f '%o %k %s\n' 2>/dev/null \
  | awk '$2 == "merchant-001"' | cut -d' ' -f1,3- \
  | while read -r offset value; do echo "$value" | jq -r --arg o "$offset" '"\($o) \(.payment_id[0:8]) seq=\(.sequence) \(.event_type)"' 2>/dev/null; done | head -30
```

Read it per payment (the 8-character id): `payment.created` (sequence 1)
normally comes before `payment.confirmed` or `payment.expired` (sequence 2),
and a refund (sequence 3) last. Kafka keeps them exactly in the order the
producer sent them. You will also spot the simulator's chaos: the same
confirmation at two offsets (an injected duplicate), an invalid record such
as `payment.teleported`, and a few payments whose confirmation comes first.

## 5. When the producer itself sends out of order

The simulator held back some `payment.created` events and published them
right after the payment's sequence-2 event (`chaos.out_of_order` in the
stats). Find them: payments whose first record on the topic has `sequence` 2.

```sh
kcat -C -b kafka:29092 -t pix.payments.v1 -o beginning -e -q -f '%s\n' 2>/dev/null \
  | jq -rR 'fromjson? | select(.event_id) | "\(.payment_id) \(.sequence) \(.event_type)"' \
  | awk '!seen[$1]++ && $2 != 1 { print }'
```

Each line is a payment whose confirmation (or expiration) arrived *before* its
creation, in the same partition. Kafka preserved the order it was given; the
order was already wrong. That is why consumers never trust arrival order:
reconciliation compares `sequence` with what it has stored, defers the early
event through the retry topic, and applies it once the creation is there. See
it happen:

```sh
docker logs payments-kafka-reconciliation-1 2>&1 | grep 'retry topic' | jq -c '{partition, offset, key, err}' | tail
```

## 6. Where order ends

- Across partitions there is no order: merchant-001's events and
  merchant-002's events are consumed in parallel.
- A retried record leaves its partition's order (it comes back through
  `pix.payments.retry.v1` later); handlers must be idempotent and use
  `sequence` (see [walkthrough-retry-dlq.md](walkthrough-retry-dlq.md)).
- Scaling consumers does not change the mapping of keys to partitions, only
  which instance reads which partition (see
  [walkthrough-consumer-groups.md](walkthrough-consumer-groups.md)).

## Clean up

```sh
docker rm -f exp-simulator
exit
```
