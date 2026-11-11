import type { HandlerContext, RecordProducer } from '@payments-kafka/consumer-runtime';
import { createCapturingLogger, makeFraudAlert, makePaymentEvent } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import type { ClaimFn, NotificationClaim } from '../../src/dedupe.js';
import { createNotificationHandler, type NotificationMessage } from '../../src/handler.js';
import type { Notification, Notifier } from '../../src/notifier.js';

function paymentMessage(value = makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2 })): NotificationMessage {
  return {
    schema: 'payment-event',
    value,
    topic: 'pix.payments.v1',
    partition: 0,
    offset: '0',
    key: value.merchant_id,
    headers: {},
    attempt: 0,
    timestamp: '0',
    record: { topic: 'pix.payments.v1', partition: 0, offset: '0' },
  };
}

function alertMessage(value = makeFraudAlert()): NotificationMessage {
  return {
    schema: 'fraud-alert',
    value,
    topic: 'pix.fraud-alerts.v1',
    partition: 0,
    offset: '0',
    key: value.merchant_id,
    headers: {},
    attempt: 0,
    timestamp: '0',
    record: { topic: 'pix.fraud-alerts.v1', partition: 0, offset: '0' },
  };
}

/** In-memory claim store behaving like the table's primary key. */
function fakeClaim(options: { fail?: boolean } = {}): { claim: ClaimFn; claims: NotificationClaim[] } {
  const keys = new Set<string>();
  const claims: NotificationClaim[] = [];
  return {
    claims,
    claim: async (claim) => {
      if (options.fail) throw new Error('connect ECONNREFUSED');
      claims.push(claim);
      if (keys.has(claim.notificationKey)) return false;
      keys.add(claim.notificationKey);
      return true;
    },
  };
}

function fakeNotifier(): { notifier: Notifier; sent: Notification[] } {
  const sent: Notification[] = [];
  return { sent, notifier: { send: (notification) => void sent.push(notification) } };
}

function context() {
  const log = createCapturingLogger('debug');
  const ctx: HandlerContext = { logger: log.logger, producer: {} as RecordProducer, groupId: 'notification' };
  return { ctx, log };
}

describe('notification handler', () => {
  it('claims, then sends a payment notification', async () => {
    const store = fakeClaim();
    const { notifier, sent } = fakeNotifier();
    const { ctx, log } = context();
    const message = paymentMessage(
      makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2, merchant_id: 'merchant-007' }),
    );

    await createNotificationHandler({ claim: store.claim, notifier })(message, ctx);

    const event = message.value as { event_id: string };
    expect(store.claims).toEqual([
      { notificationKey: event.event_id, kind: 'payment.confirmed', merchantId: 'merchant-007' },
    ]);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({ kind: 'payment.confirmed', merchant_id: 'merchant-007', notification_key: event.event_id });
    expect(log.find('notification sent')).toHaveLength(1);
  });

  it('dispatches fraud alerts by their contract and keys them by alert_id', async () => {
    const store = fakeClaim();
    const { notifier, sent } = fakeNotifier();
    const alert = makeFraudAlert({ rule: 'payment_burst', merchant_id: 'merchant-012' });

    await createNotificationHandler({ claim: store.claim, notifier })(alertMessage(alert), context().ctx);

    expect(store.claims).toEqual([{ notificationKey: alert.alert_id, kind: 'fraud_alert', merchantId: 'merchant-012' }]);
    expect(sent).toEqual([expect.objectContaining({ kind: 'fraud_alert', rule: 'payment_burst', merchant_id: 'merchant-012' })]);
  });

  it('skips payment.created without claiming or sending', async () => {
    const store = fakeClaim();
    const { notifier, sent } = fakeNotifier();
    const message = paymentMessage(makePaymentEvent({ event_type: 'payment.created', sequence: 1 }));

    await createNotificationHandler({ claim: store.claim, notifier })(message, context().ctx);

    expect(store.claims).toEqual([]);
    expect(sent).toEqual([]);
  });

  it('emits nothing when the claim is rejected', async () => {
    const store = fakeClaim();
    const { notifier, sent } = fakeNotifier();
    const handler = createNotificationHandler({ claim: store.claim, notifier });
    const message = paymentMessage();
    const alert = alertMessage();

    await handler(message, context().ctx);
    await handler(alert, context().ctx);
    const { ctx, log } = context();
    await handler(message, ctx);
    await handler(alert, ctx);

    expect(store.claims).toHaveLength(4);
    expect(sent).toHaveLength(2);
    expect(log.find('notification already claimed; not sent again')).toHaveLength(2);
    expect(log.find('notification sent')).toHaveLength(0);
  });

  it('rejects without sending when the claim store fails (retryable)', async () => {
    const { notifier, sent } = fakeNotifier();
    const handler = createNotificationHandler({ claim: fakeClaim({ fail: true }).claim, notifier });

    await expect(handler(paymentMessage(), context().ctx)).rejects.toThrow('ECONNREFUSED');
    expect(sent).toEqual([]);
  });

  it('never claims after a send: a failed send is not sent again on redelivery', async () => {
    const store = fakeClaim();
    let failures = 1;
    const sent: Notification[] = [];
    const notifier: Notifier = {
      send: (notification) => {
        if (failures-- > 0) throw new Error('channel down');
        sent.push(notification);
      },
    };
    const handler = createNotificationHandler({ claim: store.claim, notifier });
    const message = paymentMessage();

    await expect(handler(message, context().ctx)).rejects.toThrow('channel down');
    await handler(message, context().ctx);

    expect(sent).toEqual([]);
  });

  it('never writes log lines with type "notification"', async () => {
    const { ctx, log } = context();
    const handler = createNotificationHandler({ claim: fakeClaim().claim, notifier: fakeNotifier().notifier });
    await handler(paymentMessage(), ctx);
    await handler(paymentMessage(makePaymentEvent()), ctx);
    expect(log.lines.length).toBeGreaterThan(0);
    expect(log.lines.filter((line) => 'type' in line)).toEqual([]);
  });
});
