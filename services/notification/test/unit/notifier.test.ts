import { makeFraudAlert, makePaymentEvent } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import {
  createConsoleNotifier,
  fraudAlertNotification,
  paymentNotification,
  type Notification,
} from '../../src/notifier.js';

const NOW = new Date('2026-11-11T14:03:27.512Z');

function render(notification: Notification): Record<string, unknown> {
  const lines: string[] = [];
  createConsoleNotifier({ service: 'notification', now: () => NOW, write: (line) => lines.push(line) }).send(
    notification,
  );
  expect(lines).toHaveLength(1);
  expect(lines[0]).not.toContain('\n');
  return JSON.parse(lines[0]!) as Record<string, unknown>;
}

describe('payment notifications', () => {
  const base = {
    event_id: '5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11',
    payment_id: '0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42',
    merchant_id: 'merchant-007',
    amount_cents: 4590,
  };

  it.each([
    ['payment.confirmed', 2, 'Payment 0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42 of BRL 45.90 was confirmed.'],
    ['payment.expired', 2, 'Payment 0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42 of BRL 45.90 expired without being paid.'],
    ['payment.refunded', 3, 'Payment 0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42 of BRL 45.90 was refunded.'],
  ] as const)('writes one JSON line for %s', (eventType, sequence, message) => {
    const notification = paymentNotification(makePaymentEvent({ ...base, event_type: eventType, sequence }));
    expect(notification).not.toBeNull();
    expect(render(notification!)).toEqual({
      type: 'notification',
      time: '2026-11-11T14:03:27.512Z',
      service: 'notification',
      kind: eventType,
      merchant_id: 'merchant-007',
      notification_key: base.event_id,
      payment_id: base.payment_id,
      message,
    });
  });

  it('does not notify payment.created', () => {
    expect(paymentNotification(makePaymentEvent({ event_type: 'payment.created', sequence: 1 }))).toBeNull();
  });

  it('formats amounts in currency units', () => {
    const event = makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2, amount_cents: 7 });
    expect(paymentNotification(event)?.message).toContain('of BRL 0.07 ');
  });
});

describe('fraud alert notifications', () => {
  it('writes one JSON line naming the rule, keyed by alert_id', () => {
    const alert = makeFraudAlert({ rule: 'payment_burst', merchant_id: 'merchant-012' });
    expect(render(fraudAlertNotification(alert))).toEqual({
      type: 'notification',
      time: '2026-11-11T14:03:27.512Z',
      service: 'notification',
      kind: 'fraud_alert',
      merchant_id: 'merchant-012',
      notification_key: alert.alert_id,
      payment_id: alert.payment_id,
      rule: 'payment_burst',
      message: `Fraud alert "payment_burst" raised for payment ${alert.payment_id}.`,
    });
  });

  it('puts type first so the line is easy to grep', () => {
    const lines: string[] = [];
    createConsoleNotifier({ write: (line) => lines.push(line) }).send(fraudAlertNotification(makeFraudAlert()));
    expect(lines[0]).toMatch(/^\{"type":"notification",/);
  });
});
