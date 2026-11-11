# Tasks

## 1. Service setup

- [ ] 1.1 Scaffold `services/notification` as a workspace depending on `@payments-kafka/consumer-runtime` and `pg` (TypeScript, Vitest, `start` script); verify `npm run build` and `npm test` pass at the root
- [ ] 1.2 Add SQL migrations for `sent_notifications` and a startup migration runner reading `DATABASE_URL`; verify with a Testcontainers Postgres test that the table exists after startup and that running migrations twice is safe

## 2. Dedupe and notification

- [ ] 2.1 Implement the claim operation (`INSERT ... ON CONFLICT DO NOTHING`, committed before sending); verify with integration tests that the first claim wins, the second is rejected, and concurrent claims yield exactly one winner
- [ ] 2.2 Implement the notifier that writes the JSON notification line for payment events and fraud alerts; verify with unit tests on the output fields and message per kind
- [ ] 2.3 Implement the handler: dispatch by topic, skip `payment.created`, claim then send; verify with unit tests using fakes that a rejected claim emits nothing

## 3. Kafka integration

- [ ] 3.1 Wire the handler to the consumer runtime (group `notification`, topics `pix.payments.v1` and `pix.fraud-alerts.v1` with their schemas); verify with a Testcontainers test that one confirmed event and one alert each yield one notification
- [ ] 3.2 Verify "never twice" end to end with integration tests: a duplicated event, a redelivery after an abrupt stop, and two instances in the same group
- [ ] 3.3 Verify storage failure handling: with Postgres stopped the record goes to the retry topic and nothing is emitted; after Postgres returns the retry emits exactly one notification
- [ ] 3.4 Demonstrate the accepted trade-off with a test that fails the send after a successful claim and asserts the notification is not emitted on redelivery

## 4. Packaging and docs

- [ ] 4.1 Add `services/notification/Dockerfile` following the pattern in the consumer-runtime README (root build context); verify `docker build -f services/notification/Dockerfile .` succeeds
- [ ] 4.2 Write `services/notification/README.md` (what is notified, the claim-then-send decision and its at-most-once consequence, how a real channel would close the gap, how to run and test); verify the documented commands run as written
