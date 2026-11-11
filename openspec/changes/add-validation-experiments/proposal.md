# Proposal

## Why

The point of the project is not that the pipeline runs, but that it survives
being broken on purpose. Each learning goal needs a repeatable experiment that
provokes a failure and shows, with numbers, that the system held. Without
them the reliability features are untested claims.

## What Changes

- Add `experiments/` with one runnable, documented experiment per failure:
  1. duplicated events are applied once;
  2. a consumer killed mid-processing loses nothing;
  3. Kafka goes down while the API keeps receiving payments, and everything
     arrives afterwards;
  4. reconciliation offsets are reset and the same balances are rebuilt.
- Add supporting walkthroughs for the remaining learning goals: partitioning
  and per-merchant order, consumer groups with lag and rebalance, retry and
  DLQ inspection.
- Finalize the root `README.md` as the entry point to run and study the project.
- Verify the whole pipeline end to end with the `apps` Compose profile.

## Capabilities

### New Capabilities
- `validation-experiments`: the repeatable failure experiments and walkthroughs that demonstrate each reliability property of the pipeline.

### Modified Capabilities

None.

## Impact

- New directory `experiments/`; root `README.md` rewritten.
- Exercises every other component; may surface integration fixes in them.
- Requires the full Compose stack, so it runs after all other changes merge.
