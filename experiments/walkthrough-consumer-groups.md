# Walkthrough: consumer groups, lag and rebalancing

**Lesson.** A consumer group shares the partitions of the topics it reads
among its members: each partition is read by exactly one member at a time. The
group remembers how far it got with a **committed offset** per partition; the
distance to the end of the partition is the **lag**. When a member joins or
leaves, the group **rebalances**: partitions are taken away and handed out
again. More instances than partitions means idle instances.

Time: 15 minutes. You look; nothing is asserted.

All consumers here subscribe to `pix.payments.v1` (6 partitions) and
`pix.payments.retry.v1` (6 partitions), so a group has 12 partitions to share
(notification also reads `pix.fraud-alerts.v1`: 18).

## 1. Start from a known state

From the repository root, with the pipeline running:

```sh
docker compose --profile experiments run --rm experiments reset
docker compose --profile experiments run --rm experiments shell
```

**Every command below runs in that shell**, unless marked *host*. Start a
long chaos-mode run at a modest rate, so there is always traffic:

```sh
docker compose --profile apps run -d --no-deps --name exp-simulator \
  -e SIMULATOR_MAX_PAYMENTS=20000 -e SIMULATOR_RATE_PER_SECOND=40 simulator
```

## 2. One member owns everything

```sh
kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation --state
kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation --members
kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation
```

- `--state`: `Stable`, 1 member, assignment strategy `roundrobin`.
- `--members`: one member, `#PARTITIONS` 12.
- the default view: one row per partition with `CURRENT-OFFSET` (committed),
  `LOG-END-OFFSET`, `LAG` and the `CONSUMER-ID` that owns it. Lag stays close
  to 0: one instance keeps up with 40 steps per second.

Kafka UI (http://localhost:8080): **Consumers → reconciliation** shows the same
table, with the total lag at the top. Note that the other groups
(`antifraud`, `notification`) have their own offsets on the same partitions:
groups are independent.

## 3. Lag grows when nobody consumes

*Host:*

```sh
docker compose stop reconciliation
```

In the shell, run the default view a few times, 10 seconds apart:

```sh
kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation
```

`Consumer group 'reconciliation' has no active members.` `CURRENT-OFFSET`
stays put, `LOG-END-OFFSET` keeps growing, so `LAG` grows. Nothing is lost:
the records wait in the log (until retention, 7 days by default). In Kafka UI
the group is `EMPTY` and its lag climbs. *Host:*

```sh
docker compose --profile apps up -d --no-deps reconciliation
```

Run the describe again: the lag drains to about 0 within seconds, because the
consumer resumes from the committed offsets.

## 4. Scale to three: the rebalance

*Host:*

```sh
docker compose --profile apps up -d --no-deps --scale reconciliation=3 reconciliation
```

In the shell:

```sh
kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation --members
```

Three members, 4 partitions each (round robin over the 12). The rebalance is
in the logs: every instance logs `partitions revoked` and `partitions
assigned` with its new list.

```sh
for c in $(docker ps -q --filter label=com.docker.compose.service=reconciliation); do
  echo "== $(docker inspect -f '{{.Name}}' "$c")"
  docker logs --since 2m "$c" 2>&1 | grep -E '"msg":"partitions (assigned|revoked)"' \
    | jq -c '{time, msg, count, partitions: [.partitions[] | "\(.topic | sub("pix.payments."; ""))/\(.partition)"]}'
done
```

The group uses the eager protocol: on **every** rebalance each member is
revoked **all** its partitions first, including the instance that was already
running, and then gets its new share. During that short stop-the-world the
lag can rise for a moment. Kafka UI → **Consumers → reconciliation** shows
the consumer id per partition: three different ids now.

## 5. More instances than partitions

*Host:*

```sh
docker compose --profile apps up -d --no-deps --scale reconciliation=7 reconciliation
```

```sh
kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation --members
```

Twelve partitions for seven members: some get two, some one. With 13 members
or more, some would get **zero** and sit idle. The partition count is the
upper bound on a group's parallelism.

## 6. A member that dies without leaving

A clean stop (`docker compose stop`, `SIGTERM`) makes the member leave the
group at once. A crash does not: the coordinator waits for the member's
session to time out (45 s by default) before it reassigns its partitions.

```sh
victim=$(docker ps -q --filter label=com.docker.compose.service=reconciliation | head -1)
docker kill --signal KILL "$victim"
for i in 1 2 3 4 5 6; do
  kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group reconciliation | awk 'NR == 1 || $6 > 0'
  sleep 10
done
```

For up to about 45 seconds, the dead member's partitions still show its
consumer id and their lag grows; then the group rebalances and the survivors
take them over and drain them. The experiment
[02-kill-consumer.md](02-kill-consumer.md) checks that nothing is lost or
applied twice when this happens.

## Clean up

*Host:*

```sh
docker compose --profile apps up -d --no-deps --scale reconciliation=1 reconciliation
```

Shell:

```sh
docker rm -f exp-simulator
exit
```
