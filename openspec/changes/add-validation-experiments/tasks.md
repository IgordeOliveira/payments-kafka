# Tasks

## 1. End-to-end bring-up

- [ ] 1.1 Run `docker compose --profile apps up -d --build` on a clean checkout and fix any integration issue in the owning component, noting the root cause in the pull request; verify all services are healthy and the outbox connector is `RUNNING`
- [ ] 1.2 Trace one payment end to end in normal mode (API database, outbox, topic with key, reconciliation state); verify each stage manually and record the commands for the README

## 2. Experiment harness

- [ ] 2.1 Add the `experiments` Compose service (profile `experiments`) with the tooling from design.md and a dispatcher entry point; verify `docker compose --profile experiments run --rm experiments help` lists the experiments
- [ ] 2.2 Implement shared helpers: wait for zero lag with timeout, run a bounded simulator pass, export balances, count distinct events on a topic, count notifications, assert with PASS/FAIL output and exit status; verify each helper with a short self-test command

## 3. Experiments

- [ ] 3.1 Implement and document `duplicates` (`experiments/01-duplicates.md`); verify it prints PASS, and prints FAIL when run against a deliberately broken expectation
- [ ] 3.2 Implement and document `kill-consumer` (`experiments/02-kill-consumer.md`); verify it prints PASS across three consecutive runs
- [ ] 3.3 Implement and document `kafka-outage` (`experiments/03-kafka-outage.md`); verify it prints PASS and that the outbox row count grew while the broker was down
- [ ] 3.4 Implement and document `replay` (`experiments/04-replay.md`); verify it prints PASS and that the before and after balance exports are byte-identical

## 4. Walkthroughs

- [ ] 4.1 Write `experiments/walkthrough-partitioning.md` (keys, partitions, per-merchant order); verify every command runs as written
- [ ] 4.2 Write `experiments/walkthrough-consumer-groups.md` (lag, scaling, rebalance logs and Kafka UI views); verify every command runs as written
- [ ] 4.3 Write `experiments/walkthrough-retry-dlq.md` (invalid message and failing handler through retry to DLQ, reading headers); verify every command runs as written

## 5. Project entry point

- [ ] 5.1 Rewrite the root `README.md`: prerequisites, quick start, Mermaid architecture diagram, component links, learning goals mapped to experiments and walkthroughs, troubleshooting; verify the quick start on a clean checkout after `docker compose --profile apps down -v`
- [ ] 5.2 Run all four experiments in sequence from a clean environment; verify four PASS results

## Workflow follow-up

- Archive all changes after this pull request is merged (`openspec archive <change>`), so `openspec/specs/` holds the final specs.
