import type { PaymentEventType } from '@payments-kafka/consumer-runtime';
import { describe, expect, it } from 'vitest';
import { decide, type PaymentState, type PaymentStatus } from '../../src/decision.js';

const SEQUENCE: Record<PaymentEventType, 1 | 2 | 3> = {
  'payment.created': 1,
  'payment.confirmed': 2,
  'payment.expired': 2,
  'payment.refunded': 3,
};

function event(type: PaymentEventType, overrides: { merchant_id?: string; amount_cents?: number } = {}) {
  return {
    event_type: type,
    sequence: SEQUENCE[type],
    merchant_id: overrides.merchant_id ?? 'merchant-001',
    amount_cents: overrides.amount_cents ?? 1000,
  };
}

const LAST_SEQUENCE: Record<PaymentStatus, number> = { CREATED: 1, CONFIRMED: 2, EXPIRED: 2, REFUNDED: 3 };

function state(status: PaymentStatus): PaymentState {
  return { merchantId: 'merchant-001', amountCents: 1000, status, lastSequence: LAST_SEQUENCE[status] };
}

describe('decide: lifecycle steps in order', () => {
  it('applies created to an unknown payment, without a ledger entry', () => {
    expect(decide(event('payment.created'), null, false)).toEqual({
      kind: 'apply',
      status: 'CREATED',
      ledgerEntry: null,
    });
  });

  it('applies confirmed after created, as a credit', () => {
    expect(decide(event('payment.confirmed'), state('CREATED'), false)).toEqual({
      kind: 'apply',
      status: 'CONFIRMED',
      ledgerEntry: 'credit',
    });
  });

  it('applies expired after created, without a ledger entry', () => {
    expect(decide(event('payment.expired'), state('CREATED'), false)).toEqual({
      kind: 'apply',
      status: 'EXPIRED',
      ledgerEntry: null,
    });
  });

  it('applies refunded after confirmed, as a debit', () => {
    expect(decide(event('payment.refunded'), state('CONFIRMED'), false)).toEqual({
      kind: 'apply',
      status: 'REFUNDED',
      ledgerEntry: 'debit',
    });
  });
});

describe('decide: duplicates', () => {
  const states: Array<PaymentState | null> = [null, state('CREATED'), state('CONFIRMED'), state('EXPIRED'), state('REFUNDED')];
  const types = Object.keys(SEQUENCE) as PaymentEventType[];

  it.each(types)('an already processed %s is a duplicate in every state', (type) => {
    for (const s of states) expect(decide(event(type), s, true)).toEqual({ kind: 'duplicate' });
  });
});

describe('decide: gaps (out of order, retryable)', () => {
  it.each([
    ['payment.confirmed', null],
    ['payment.expired', null],
    ['payment.refunded', null],
    ['payment.refunded', 'CREATED'],
  ] as Array<[PaymentEventType, PaymentStatus | null]>)('%s with payment %s is a gap', (type, status) => {
    const decision = decide(event(type), status ? state(status) : null, false);
    expect(decision.kind).toBe('gap');
  });

  it('explains the gap', () => {
    const decision = decide(event('payment.refunded'), state('CREATED'), false);
    expect(decision).toMatchObject({ reason: expect.stringContaining('last applied sequence is 1') });
  });
});

describe('decide: conflicts (non-retryable)', () => {
  it.each([
    // sequence at or below the last applied one, with a new event_id
    ['payment.created', 'CREATED'],
    ['payment.created', 'CONFIRMED'],
    ['payment.created', 'EXPIRED'],
    ['payment.created', 'REFUNDED'],
    ['payment.confirmed', 'CONFIRMED'],
    ['payment.confirmed', 'EXPIRED'],
    ['payment.confirmed', 'REFUNDED'],
    ['payment.expired', 'CONFIRMED'],
    ['payment.expired', 'EXPIRED'],
    ['payment.expired', 'REFUNDED'],
    ['payment.refunded', 'REFUNDED'],
    // next sequence, but a transition the lifecycle does not allow
    ['payment.refunded', 'EXPIRED'],
  ] as Array<[PaymentEventType, PaymentStatus]>)('%s on a %s payment is a conflict', (type, status) => {
    expect(decide(event(type), state(status), false).kind).toBe('conflict');
  });

  it('expired after confirmed names both steps', () => {
    expect(decide(event('payment.expired'), state('CONFIRMED'), false)).toMatchObject({
      kind: 'conflict',
      reason: expect.stringMatching(/payment\.expired.*CONFIRMED/),
    });
  });

  it('refunded after expired is a conflict, not a gap', () => {
    expect(decide(event('payment.refunded'), state('EXPIRED'), false)).toMatchObject({
      kind: 'conflict',
      reason: expect.stringContaining('not allowed from EXPIRED'),
    });
  });

  it('an event for another merchant than the stored payment is a conflict', () => {
    expect(decide(event('payment.confirmed', { merchant_id: 'merchant-999' }), state('CREATED'), false)).toMatchObject({
      kind: 'conflict',
      reason: expect.stringContaining('merchant-999'),
    });
  });

  it('a refund of a different amount is a conflict (refunds are always full)', () => {
    expect(decide(event('payment.refunded', { amount_cents: 500 }), state('CONFIRMED'), false)).toMatchObject({
      kind: 'conflict',
      reason: expect.stringContaining('amount 500'),
    });
  });
});
