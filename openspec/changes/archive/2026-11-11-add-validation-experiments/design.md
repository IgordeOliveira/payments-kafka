# Design

## Context

All components exist and are individually tested with Testcontainers. This
change is the first to run them together on the Compose stack described in
`docs/architecture.md`. The simulator exposes counters (`GET /stats`),
reconciliation exposes `check`, `balances` and `reset-state`, and notification
writes countable JSON lines: these are the measurement points.

## Goals / Non-Goals

**Goals:**
- Each experiment is one command, ends with an explicit PASS or FAIL, and
  explains what to look at in Kafka UI while it runs.
- Experiments are bounded (fixed payment count and seed) so expected values
  are exact, not approximate.

**Non-Goals:**
- A CI pipeline or performance benchmarks.
- The RAG-feeding consumer (later extension, no change yet).

## Decisions

- **Scripts run inside a container.** A small `experiments` Compose service
  (profile `experiments`, image with `bash`, `curl`, `jq`, `psql`, the Kafka
  CLI and the Docker CLI with the socket mounted) runs the scripts, so they
  behave the same on Windows, macOS and Linux. Entry point:
  `docker compose --profile experiments run --rm experiments <name>`.
- **Each experiment starts from a clean, known state** (it resets topics'
  consumer offsets and service state it depends on, or documents that it
  needs `down -v` first) and uses a bounded simulator run
  (`SIMULATOR_MAX_PAYMENTS`, fixed `SIMULATOR_SEED`).
- **"Wait until drained" instead of sleeping**: a helper polls consumer group
  lag until it is zero for all partitions (with a timeout) before asserting.
- **Assertions compare independent sources**:
  - duplicates: simulator `duplicates` counter > 0, and reconciliation balances
    equal those of the same seed run without duplicates; `check` passes;
    notification count equals the number of distinct notifiable events;
  - killed consumer: `docker kill` a reconciliation instance mid-run; after
    drain, distinct processed events equal distinct events produced and
    `check` passes;
  - Kafka outage: normal mode; stop `kafka`, keep the simulator calling the
    API, assert API calls succeed and outbox rows accumulate; start `kafka`;
    after drain, events on the topic equal outbox rows and reconciliation's
    payments match the API database;
  - replay: export `balances`, stop reconciliation, `reset-state --yes`, reset
    group offsets to earliest, start, drain, export again, diff must be empty.
- **Walkthroughs are guided documents with commands**, not asserted scripts,
  where the lesson is observation: partition assignment per merchant key,
  lag growth and rebalance when scaling consumers, a poison message travelling
  through retry to the DLQ and how to read its headers.
- **Integration fixes are made in the owning component** in this change's pull
  request, with the root cause noted, because all other changes are merged by
  now.
- **Root README** becomes the guide: prerequisites, quick start, architecture
  diagram (Mermaid), component links, the learning goals mapped to experiments,
  troubleshooting.

## Risks / Trade-offs

- [Mounting the Docker socket into the experiments container] → local study
  project only; called out in the README.
- [Timing-dependent experiments can flake] → bounded runs, lag-based waiting
  and generous timeouts; each script prints the measured values on failure.
- [Outbox publication is at-least-once, so the topic may hold a duplicate
  after a Connect restart] → assertions compare distinct `event_id`s.
