# Tasks

## 1. Service setup

- [ ] 1.1 Scaffold `services/antifraud` as a workspace depending on `@payments-kafka/consumer-runtime` (TypeScript, Vitest, `start` script); verify `npm run build` and `npm test` pass at the root
- [ ] 1.2 Load `ANTIFRAUD_*` thresholds from the environment with the defaults in design.md; verify with unit tests for defaults and invalid values

## 2. Rules

- [ ] 2.1 Implement per-merchant state (running mean, sliding windows, bounded seen-event-id set) and the `amount_outlier` rule as pure functions; verify with unit tests for warm-up, firing and the outlier not polluting the mean
- [ ] 2.2 Implement `payment_burst` with once-per-breach behavior on event time; verify with unit tests for firing once, re-arming after the window drains and staying quiet at normal pace
- [ ] 2.3 Implement `refund_excess` with once-per-breach behavior; verify with unit tests mirroring 2.2
- [ ] 2.4 Verify idempotency and determinism with unit tests: a stream delivered twice, and the same stream replayed from empty state, yield identical `alert_id` sets

## 3. Kafka integration

- [ ] 3.1 Wire the handler to the consumer runtime (group `antifraud`, topic `pix.payments.v1`) and publish alerts keyed by `merchant_id` with `acks=all`, awaited inside the handler; verify with a Testcontainers test that a crafted burst yields one schema-valid alert with the expected `alert_id`
- [ ] 3.2 Drop merchant state on partition revocation and start empty on assignment; verify with an integration test using two instances
- [ ] 3.3 Verify with an integration test that a failed alert publish sends the event to the retry topic and the alert is published on retry

## 4. Packaging and docs

- [ ] 4.1 Add `services/antifraud/Dockerfile` following the pattern in the consumer-runtime README (root build context); verify `docker build -f services/antifraud/Dockerfile .` succeeds
- [ ] 4.2 Write `services/antifraud/README.md` (rules, thresholds, the in-memory state trade-off and warm-up after rebalance, how to run and test); verify the documented commands run as written
