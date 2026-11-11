# Proposal

## Why

Producers and consumers are built in parallel by different people in different
languages. The event contract is their only coupling, so it must exist, be
versioned and be machine-checkable before any component is written.

## What Changes

- Add JSON Schema (draft 2020-12) files for the payment event and the fraud alert.
- Add valid and invalid example files that every component uses in its tests.
- Document topics, keys, lifecycle and `sequence` rules in `docs/architecture.md`.

## Capabilities

### New Capabilities
- `event-contracts`: the versioned JSON contracts for payment events and fraud alerts, including lifecycle and compatibility rules.

### Modified Capabilities

None.

## Impact

- New directory `contracts/` and new file `docs/architecture.md`.
- Every other change depends on these files; none of them may redefine the contract.
