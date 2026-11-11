import type { PaymentEvent } from '@payments-kafka/consumer-runtime';
import { collect, makePaymentEvent, waitFor } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import {
  balancesOf,
  bootstrap,
  countRows,
  createDatabase,
  lifecycle,
  newTopics,
  paymentRow,
  produceEvents,
  startService,
  waitForProcessed,
} from './helpers.js';

describe('reconciliation consumer on Kafka', () => {
  it('turns a produced lifecycle into payment state and balance', async () => {
    const { pool } = await createDatabase();
    const topics = await newTopics();
    const refunded = lifecycle('merchant-k1', 1500, 'refunded');
    const confirmed = lifecycle('merchant-k1', 2500, 'confirmed');
    const expired = lifecycle('merchant-k2', 999, 'expired');
    await startService({ pool, topics });
    await produceEvents(topics.payments, [...refunded, ...confirmed, ...expired]);

    await waitForProcessed(pool, 7);
    expect(await paymentRow(pool, refunded[0]!.payment_id)).toMatchObject({ status: 'REFUNDED', last_sequence: 3 });
    expect(await paymentRow(pool, confirmed[0]!.payment_id)).toMatchObject({ status: 'CONFIRMED', last_sequence: 2 });
    expect(await paymentRow(pool, expired[0]!.payment_id)).toMatchObject({ status: 'EXPIRED', last_sequence: 2 });
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-k1', balance_cents: 2500, confirmed_count: 2, refunded_count: 1 },
    ]);
  });

  it('applies a confirmation consumed before its creation through the retry topic', async () => {
    const { pool } = await createDatabase();
    const topics = await newTopics();
    const [created, confirmed] = lifecycle('merchant-k3', 4200, 'confirmed') as [PaymentEvent, PaymentEvent];
    const service = await startService({ pool, topics, retryMaxAttempts: 4, retryBackoffMs: 300 });

    await produceEvents(topics.payments, [confirmed]);
    // The confirmation fails as out of order and is handed to the retry topic...
    const [retried] = await collect(bootstrap, topics.retry, { count: 1, timeoutMs: 30_000 });
    expect(retried?.json()).toMatchObject({ event_id: confirmed.event_id });
    expect(retried?.headers['x-retry-group']).toBe(service.groupId);
    expect(retried?.headers['x-error-message']).toContain('out of order');
    // ...and the creation arrives shortly after.
    await produceEvents(topics.payments, [created]);

    await waitForProcessed(pool, 2);
    expect(await paymentRow(pool, created.payment_id)).toMatchObject({ status: 'CONFIRMED', last_sequence: 2 });
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-k3', balance_cents: 4200, confirmed_count: 1, refunded_count: 0 },
    ]);
    const applied = await waitFor(() => service.logs.lines.find((l) => l.msg === 'payment event applied' && l.event_id === confirmed.event_id));
    expect(applied).toMatchObject({ attempt: expect.any(Number) });
    expect((applied as { attempt: number }).attempt).toBeGreaterThan(0);
    expect(await collect(bootstrap, topics.dlq, { timeoutMs: 1_500 })).toEqual([]);
  });

  it('dead-letters an orphan confirmation after the retries, with the balance unchanged', async () => {
    const { pool } = await createDatabase();
    const topics = await newTopics();
    const [, orphan] = lifecycle('merchant-k4', 7000, 'confirmed') as [PaymentEvent, PaymentEvent];
    const other = lifecycle('merchant-k4', 100, 'confirmed');
    await startService({ pool, topics, retryMaxAttempts: 2, retryBackoffMs: 100 });
    await produceEvents(topics.payments, [orphan, ...other]);

    const [dead] = await collect(bootstrap, topics.dlq, { count: 1, timeoutMs: 45_000 });
    expect(dead?.json()).toMatchObject({ event_id: orphan.event_id });
    expect(dead?.headers['x-dlq-reason']).toBe('retries_exhausted');
    expect(dead?.headers['x-dlq-attempts']).toBe('2');

    expect(await paymentRow(pool, orphan.payment_id)).toBeUndefined();
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-k4', balance_cents: 100, confirmed_count: 1, refunded_count: 0 },
    ]);
    expect(await countRows(pool, 'processed_events')).toBe(2);
  });

  it('dead-letters a conflicting event straight away (expired after confirmed)', async () => {
    const { pool } = await createDatabase();
    const topics = await newTopics();
    const [created, confirmed] = lifecycle('merchant-k5', 800, 'confirmed') as [PaymentEvent, PaymentEvent];
    const expired = makePaymentEvent({
      event_type: 'payment.expired',
      sequence: 2,
      payment_id: created.payment_id,
      merchant_id: 'merchant-k5',
      amount_cents: 800,
    });
    await startService({ pool, topics });
    await produceEvents(topics.payments, [created, confirmed, expired]);

    const [dead] = await collect(bootstrap, topics.dlq, { count: 1, timeoutMs: 30_000 });
    expect(dead?.json()).toMatchObject({ event_id: expired.event_id });
    expect(dead?.headers['x-dlq-reason']).toBe('non_retryable');
    expect(dead?.headers['x-dlq-attempts']).toBe('0');
    expect(await collect(bootstrap, topics.retry, { timeoutMs: 1_000 })).toEqual([]);
    expect(await paymentRow(pool, created.payment_id)).toMatchObject({ status: 'CONFIRMED' });
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-k5', balance_cents: 800, confirmed_count: 1, refunded_count: 0 },
    ]);
  });
});
