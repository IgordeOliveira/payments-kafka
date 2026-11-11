import type pg from 'pg';
import { randomUUID } from 'node:crypto';
import type { PaymentEvent } from '@payments-kafka/consumer-runtime';
import { makePaymentEvent } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import { applyEvent } from '../../src/apply.js';
import {
  balancesOf,
  countRows,
  createDatabase,
  expectedBalances,
  generateStream,
  lifecycle,
  newPool,
  paymentRow,
  snapshot,
} from './helpers.js';

async function applyAll(pool: pg.Pool, events: PaymentEvent[]) {
  const results = [];
  for (const event of events) results.push(await applyEvent(pool, event));
  return results;
}

describe('applying events (single transaction)', () => {
  it('tracks the full lifecycle: created, confirmed, refunded', async () => {
    const { pool } = await createDatabase();
    const events = lifecycle('merchant-a', 4590, 'refunded');
    const results = await applyAll(pool, events);
    expect(results.map((r) => r.outcome)).toEqual(['applied', 'applied', 'applied']);

    expect(await paymentRow(pool, events[0]!.payment_id)).toEqual({
      status: 'REFUNDED',
      last_sequence: 3,
      merchant_id: 'merchant-a',
      amount_cents: '4590',
    });
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-a', balance_cents: 0, confirmed_count: 1, refunded_count: 1 },
    ]);
    expect(await countRows(pool, 'processed_events')).toBe(3);
  });

  it('three confirmed payments of 1000 with one refunded leave a balance of 2000', async () => {
    const { pool } = await createDatabase();
    await applyAll(pool, [
      ...lifecycle('merchant-b', 1000, 'confirmed'),
      ...lifecycle('merchant-b', 1000, 'confirmed'),
      ...lifecycle('merchant-b', 1000, 'refunded'),
    ]);
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-b', balance_cents: 2000, confirmed_count: 3, refunded_count: 1 },
    ]);
  });

  it('writes one ledger entry per confirmation (credit) and refund (debit)', async () => {
    const { pool } = await createDatabase();
    const first = lifecycle('merchant-c', 700, 'refunded');
    const second = lifecycle('merchant-c', 300, 'confirmed');
    await applyAll(pool, [...first, ...second]);
    const { rows } = await pool.query(
      `SELECT event_id, entry_type, amount_cents::int AS amount FROM ledger_entries WHERE merchant_id = 'merchant-c' ORDER BY entry_type, amount`,
    );
    expect(rows).toEqual([
      { event_id: second[1]!.event_id, entry_type: 'credit', amount: 300 },
      { event_id: first[1]!.event_id, entry_type: 'credit', amount: 700 },
      { event_id: first[2]!.event_id, entry_type: 'debit', amount: 700 },
    ]);
  });

  it('does not change the balance on created or expired', async () => {
    const { pool } = await createDatabase();
    const created = lifecycle('merchant-d', 5000, 'created');
    const expired = lifecycle('merchant-d', 6000, 'expired');
    await applyAll(pool, [...created, ...expired]);
    expect(await balancesOf(pool)).toEqual([]);
    expect(await countRows(pool, 'ledger_entries')).toBe(0);
    expect((await paymentRow(pool, created[0]!.payment_id))?.status).toBe('CREATED');
    expect((await paymentRow(pool, expired[0]!.payment_id))?.status).toBe('EXPIRED');

    // An expired payment next to a confirmed one: only the confirmation counts.
    await applyAll(pool, lifecycle('merchant-d', 250, 'confirmed'));
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-d', balance_cents: 250, confirmed_count: 1, refunded_count: 0 },
    ]);
  });

  it('rolls back gaps and conflicts: nothing stored, event not marked processed', async () => {
    const { pool } = await createDatabase();
    const [created, confirmed] = lifecycle('merchant-e', 900, 'confirmed') as [PaymentEvent, PaymentEvent];

    expect(await applyEvent(pool, confirmed)).toMatchObject({ outcome: 'gap' });
    expect(await countRows(pool, 'processed_events')).toBe(0);
    expect(await countRows(pool, 'payments')).toBe(0);

    await applyAll(pool, [created, confirmed]);
    const expired = makePaymentEvent({
      event_type: 'payment.expired',
      sequence: 2,
      payment_id: created.payment_id,
      merchant_id: 'merchant-e',
      amount_cents: 900,
    });
    expect(await applyEvent(pool, expired)).toMatchObject({ outcome: 'conflict' });
    expect((await paymentRow(pool, created.payment_id))?.status).toBe('CONFIRMED');
    expect(await countRows(pool, 'processed_events')).toBe(2);
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-e', balance_cents: 900, confirmed_count: 1, refunded_count: 0 },
    ]);
  });
});

describe('idempotency', () => {
  it('a stream applied twice equals the stream applied once', async () => {
    const stream = generateStream({ seed: 7, merchants: 4, paymentsPerMerchant: 15, outOfOrderRate: 0, duplicateRate: 0 });
    const once = await createDatabase();
    const twice = await createDatabase();

    const firstPass = await applyAll(once.pool, stream.records);
    expect(firstPass.every((r) => r.outcome === 'applied')).toBe(true);

    await applyAll(twice.pool, stream.records);
    const secondPass = await applyAll(twice.pool, stream.records);
    expect(secondPass.every((r) => r.outcome === 'duplicate')).toBe(true);

    expect(await snapshot(twice.pool)).toEqual(await snapshot(once.pool));
    expect(await balancesOf(once.pool)).toEqual(expectedBalances(stream.distinct));
  });

  it('every event delivered twice in a row equals once', async () => {
    const stream = generateStream({ seed: 8, merchants: 3, paymentsPerMerchant: 10, outOfOrderRate: 0, duplicateRate: 0 });
    const once = await createDatabase();
    const doubled = await createDatabase();
    await applyAll(once.pool, stream.records);
    await applyAll(
      doubled.pool,
      stream.records.flatMap((e) => [e, e]),
    );
    expect(await snapshot(doubled.pool)).toEqual(await snapshot(once.pool));
  });

  it('a failure before COMMIT leaves no partial effect, and the redelivered event is applied in full', async () => {
    const { pool } = await createDatabase();
    const [created, confirmed] = lifecycle('merchant-f', 1234, 'confirmed') as [PaymentEvent, PaymentEvent];
    await applyEvent(pool, created);
    const before = await snapshot(pool);

    await expect(
      applyEvent(pool, confirmed, {
        beforeCommit: () => {
          throw new Error('injected crash before commit');
        },
      }),
    ).rejects.toThrow('injected crash before commit');

    // Every effect of the confirmation was rolled back with the dedupe row.
    expect(await snapshot(pool)).toEqual(before);
    expect(await countRows(pool, 'ledger_entries')).toBe(0);
    expect((await paymentRow(pool, created.payment_id))?.status).toBe('CREATED');

    expect(await applyEvent(pool, confirmed)).toMatchObject({ outcome: 'applied' });
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-f', balance_cents: 1234, confirmed_count: 1, refunded_count: 0 },
    ]);
    expect(await applyEvent(pool, confirmed)).toEqual({ outcome: 'duplicate' });
  });
});

describe('concurrency (row locks, atomic balance updates)', () => {
  it('concurrent confirmations and refunds of one merchant are all counted', async () => {
    const db = await createDatabase();
    const pool = newPool(db.url, 30);
    const payments = Array.from({ length: 60 }, (_, i) => lifecycle('merchant-hot', 100 + i, i % 3 === 0 ? 'refunded' : 'confirmed'));
    await Promise.all(payments.map((events) => applyEvent(pool, events[0]!)));
    // All confirmations at once, then all refunds at once.
    await Promise.all(payments.map((events) => applyEvent(pool, events[1]!)));
    await Promise.all(payments.filter((e) => e[2]).map((events) => applyEvent(pool, events[2]!)));

    expect(await balancesOf(pool)).toEqual(expectedBalances(payments.flat()));
  });

  it('the same event applied concurrently is applied once', async () => {
    const db = await createDatabase();
    const pool = newPool(db.url, 20);
    const [created, confirmed] = lifecycle('merchant-dup', 5000, 'confirmed') as [PaymentEvent, PaymentEvent];
    await applyEvent(pool, created);
    const results = await Promise.all(Array.from({ length: 20 }, () => applyEvent(pool, confirmed)));
    expect(results.filter((r) => r.outcome === 'applied')).toHaveLength(1);
    expect(results.filter((r) => r.outcome === 'duplicate')).toHaveLength(19);
    expect(await balancesOf(pool)).toEqual([
      { merchant_id: 'merchant-dup', balance_cents: 5000, confirmed_count: 1, refunded_count: 0 },
    ]);
  });

  it('two different events racing for the same step: one applied, the other a conflict', async () => {
    const db = await createDatabase();
    const pool = newPool(db.url, 20);
    for (let i = 0; i < 10; i++) {
      const paymentId = randomUUID();
      const base = { payment_id: paymentId, merchant_id: 'merchant-race', amount_cents: 100 };
      const [a, b] = await Promise.all([
        applyEvent(pool, makePaymentEvent({ ...base, event_type: 'payment.created', sequence: 1 })),
        applyEvent(pool, makePaymentEvent({ ...base, event_type: 'payment.created', sequence: 1 })),
      ]);
      expect([a.outcome, b.outcome].sort()).toEqual(['applied', 'conflict']);
      const [c, d] = await Promise.all([
        applyEvent(pool, makePaymentEvent({ ...base, event_type: 'payment.confirmed', sequence: 2 })),
        applyEvent(pool, makePaymentEvent({ ...base, event_type: 'payment.expired', sequence: 2 })),
      ]);
      expect([c.outcome, d.outcome].sort()).toEqual(['applied', 'conflict']);
    }
    const [balance] = await balancesOf(pool);
    const { rows } = await pool.query<{ n: string }>(`SELECT COUNT(*) AS n FROM payments WHERE status = 'CONFIRMED'`);
    expect(balance?.confirmed_count ?? 0).toBe(Number(rows[0]!.n));
    expect(balance?.balance_cents ?? 0).toBe(100 * Number(rows[0]!.n));
  });
});
