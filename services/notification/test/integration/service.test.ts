import { collect, makeFraudAlert, makePaymentEvent, produce } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import {
  bootstrapServers,
  capturingNotifier,
  freshDatabase,
  freshTopics,
  startService,
  waitForCommitted,
} from './harness.js';

describe('notification service', () => {
  it('notifies one confirmed payment and one fraud alert once each, and skips payment.created', async () => {
    const topics = await freshTopics();
    const { notifier, lines } = capturingNotifier();
    const { config, log } = await startService({ topics, databaseUrl: await freshDatabase(), notifier });

    const created = makePaymentEvent({ merchant_id: 'merchant-007' });
    const confirmed = makePaymentEvent({
      event_type: 'payment.confirmed',
      sequence: 2,
      merchant_id: 'merchant-007',
      payment_id: created.payment_id,
    });
    const alert = makeFraudAlert({ rule: 'payment_burst', merchant_id: 'merchant-012' });
    await produce(bootstrapServers, topics.payments, [
      { key: created.merchant_id, value: created },
      { key: confirmed.merchant_id, value: confirmed },
    ]);
    await produce(bootstrapServers, topics.fraudAlerts, [{ key: alert.merchant_id, value: alert }]);

    await waitForCommitted(config.groupId, topics.payments, 2);
    await waitForCommitted(config.groupId, topics.fraudAlerts, 1);

    expect(lines).toHaveLength(2);
    expect(lines).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'notification',
          kind: 'payment.confirmed',
          merchant_id: 'merchant-007',
          notification_key: confirmed.event_id,
          payment_id: confirmed.payment_id,
          message: expect.stringContaining('was confirmed'),
        }),
        expect.objectContaining({
          type: 'notification',
          kind: 'fraud_alert',
          merchant_id: 'merchant-012',
          notification_key: alert.alert_id,
          rule: 'payment_burst',
          message: expect.stringContaining('payment_burst'),
        }),
      ]),
    );
    // Nothing went to retry or the DLQ, and no log line looks like a notification.
    expect(await collect(bootstrapServers, topics.dlq, { timeoutMs: 1_000 })).toEqual([]);
    expect(log.lines.filter((line) => line['type'] === 'notification')).toEqual([]);
  });

  it('writes exactly one countable line per distinct confirmed, expired and refunded event', async () => {
    const topics = await freshTopics();
    const { notifier, lines } = capturingNotifier();
    const { config } = await startService({ topics, databaseUrl: await freshDatabase(), notifier });

    const confirmed = Array.from({ length: 10 }, (_, i) =>
      makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2, merchant_id: `merchant-${i % 4}` }),
    );
    const expired = makePaymentEvent({ event_type: 'payment.expired', sequence: 2 });
    const refunded = makePaymentEvent({ event_type: 'payment.refunded', sequence: 3 });
    const events = [...confirmed, expired, refunded];
    await produce(
      bootstrapServers,
      topics.payments,
      events.map((value) => ({ key: value.merchant_id, value })),
    );
    await waitForCommitted(config.groupId, topics.payments, events.length);

    expect(lines.filter((line) => line.type === 'notification' && line.kind === 'payment.confirmed')).toHaveLength(10);
    expect(lines.filter((line) => line.kind === 'payment.expired').map((line) => line.notification_key)).toEqual([
      expired.event_id,
    ]);
    expect(lines.filter((line) => line.kind === 'payment.refunded').map((line) => line.notification_key)).toEqual([
      refunded.event_id,
    ]);
    expect(new Set(lines.map((line) => line.notification_key)).size).toBe(12);
  });
});
