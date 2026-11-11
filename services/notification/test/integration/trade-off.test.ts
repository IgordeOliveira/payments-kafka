import { collect, makePaymentEvent, produce, waitFor } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import type { Notifier } from '../../src/notifier.js';
import {
  bootstrapServers,
  capturingNotifier,
  freshDatabase,
  freshTopics,
  logsFor,
  startService,
  waitForCommitted,
} from './harness.js';
import { queryOnce } from './postgres.js';

describe('claim-then-send trade-off (at most once)', () => {
  it('loses the notification when the send fails after a successful claim: the redelivery does not send it', async () => {
    const topics = await freshTopics();
    const databaseUrl = await freshDatabase();
    const { notifier: consoleNotifier, lines } = capturingNotifier();
    // The channel fails exactly once, after the claim was committed: the same
    // window a crash between claim and send would hit.
    let failures = 1;
    const notifier: Notifier = {
      send: (notification) => {
        if (failures > 0) {
          failures -= 1;
          throw new Error('notification channel unavailable');
        }
        return consoleNotifier.send(notification);
      },
    };
    const { config, log } = await startService({ topics, databaseUrl, notifier });

    const lost = makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2 });
    const delivered = makePaymentEvent({ event_type: 'payment.expired', sequence: 2 });
    await produce(bootstrapServers, topics.payments, [{ key: lost.merchant_id, value: lost, partition: 0 }]);

    // First delivery: claimed, send threw, record handed to the retry topic.
    const [retry] = await collect(bootstrapServers, topics.retry, { count: 1, timeoutMs: 30_000 });
    expect(retry?.json()).toMatchObject({ event_id: lost.event_id });
    // Redelivery (attempt 1): the key is already claimed, nothing is sent.
    await waitFor(() => logsFor(log, 'notification already claimed; not sent again', lost.event_id).length === 1, {
      message: 'redelivery rejected by the claim',
    });
    expect(
      log.find('notification already claimed; not sent again').find((line) => line['notification_key'] === lost.event_id),
    ).toMatchObject({ attempt: 1 });

    // The channel works again for other events.
    await produce(bootstrapServers, topics.payments, [{ key: delivered.merchant_id, value: delivered, partition: 0 }]);
    await waitForCommitted(config.groupId, topics.payments, 2);
    await waitFor(() => lines.length === 1, { message: 'the next event is notified' });

    expect(lines.map((line) => line.notification_key)).toEqual([delivered.event_id]);
    expect(await collect(bootstrapServers, topics.dlq, { timeoutMs: 1_000 })).toEqual([]);
    // The lost notification is recorded as sent: that is the at-most-once gap.
    expect(
      await queryOnce(databaseUrl, 'SELECT notification_key FROM sent_notifications ORDER BY sent_at'),
    ).toEqual([{ notification_key: lost.event_id }, { notification_key: delivered.event_id }]);
  });
});
