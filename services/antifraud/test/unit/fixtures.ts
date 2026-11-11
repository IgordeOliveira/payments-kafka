import { randomUUID } from 'node:crypto';
import type { PaymentEvent } from '@payments-kafka/consumer-runtime';

export const T0 = Date.parse('2026-11-11T14:00:00.000Z');

/** A payment event at `T0 + offsetMs` (event time), with random ids unless overridden. */
export function event(offsetMs: number, overrides: Partial<PaymentEvent> = {}): PaymentEvent {
  return {
    event_id: randomUUID(),
    event_type: 'payment.created',
    schema_version: 1,
    occurred_at: new Date(T0 + offsetMs).toISOString(),
    payment_id: randomUUID(),
    merchant_id: 'merchant-001',
    sequence: 1,
    amount_cents: 5000,
    currency: 'BRL',
    ...overrides,
  };
}

export function created(offsetMs: number, amountCents = 5000, merchantId = 'merchant-001'): PaymentEvent {
  return event(offsetMs, { amount_cents: amountCents, merchant_id: merchantId });
}

export function refunded(offsetMs: number, merchantId = 'merchant-001'): PaymentEvent {
  return event(offsetMs, { event_type: 'payment.refunded', sequence: 3, merchant_id: merchantId });
}
