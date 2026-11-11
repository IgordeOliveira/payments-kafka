/**
 * Applies one payment event to Postgres in a single transaction:
 *
 *   1. insert the event_id into processed_events (ON CONFLICT DO NOTHING);
 *      no row inserted → duplicate, nothing else happens;
 *   2. lock the payment row (SELECT ... FOR UPDATE);
 *   3. decide (see decision.ts);
 *   4. on apply: upsert the payment, insert the ledger entry and update the
 *      merchant balance atomically (balance_cents = balance_cents + delta);
 *   5. COMMIT. On gap or conflict the transaction is rolled back, so the event
 *      is not marked processed and nothing of it is stored.
 *
 * Locks are always taken in the same order (processed event, payment,
 * merchant balance), so concurrent transactions for the same merchant or the
 * same payment serialize instead of deadlocking.
 */
import type { PaymentEvent } from '@payments-kafka/consumer-runtime';
import type pg from 'pg';
import { decide, type Decision, type PaymentState, type PaymentStatus } from './decision.js';

export type ApplyResult =
  | { outcome: 'applied'; status: PaymentStatus; ledgerEntry: 'credit' | 'debit' | null }
  | { outcome: 'duplicate' }
  | { outcome: 'gap'; reason: string }
  | { outcome: 'conflict'; reason: string };

export interface ApplyOptions {
  /**
   * Test seam: called inside the transaction after every effect was written
   * and before COMMIT. Throwing from it rolls the transaction back.
   */
  beforeCommit?: () => void | Promise<void>;
}

interface PaymentRow {
  merchant_id: string;
  amount_cents: string;
  status: PaymentStatus;
  last_sequence: number;
}

export async function applyEvent(pool: pg.Pool, event: PaymentEvent, options: ApplyOptions = {}): Promise<ApplyResult> {
  const client = await pool.connect();
  let broken = false;
  try {
    await client.query('BEGIN');
    try {
      const result = await applyInTransaction(client, event);
      if (result.outcome === 'applied') {
        await options.beforeCommit?.();
        await client.query('COMMIT');
      } else {
        await client.query('ROLLBACK');
      }
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {
        broken = true;
      });
      throw error;
    }
  } finally {
    // A connection whose ROLLBACK failed is discarded instead of reused.
    client.release(broken);
  }
}

async function applyInTransaction(client: pg.PoolClient, event: PaymentEvent): Promise<ApplyResult> {
  const inserted = await client.query(
    'INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING',
    [event.event_id],
  );
  const alreadyProcessed = inserted.rowCount === 0;
  if (alreadyProcessed) return toResult(decide(event, null, true));

  let state = await lockPayment(client, event.payment_id);
  let decision = decide(event, state, false);

  if (decision.kind === 'apply' && state === null) {
    // payment.created: insert the row. If a concurrent transaction inserted
    // the same payment first, this waits for it, inserts nothing, and the
    // event is decided again against the committed row.
    const created = await client.query(
      `INSERT INTO payments (payment_id, merchant_id, amount_cents, status, last_sequence)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (payment_id) DO NOTHING`,
      [event.payment_id, event.merchant_id, event.amount_cents, decision.status, event.sequence],
    );
    if (created.rowCount === 1) return toResult(decision);
    state = await lockPayment(client, event.payment_id);
    decision = decide(event, state, false);
    if (decision.kind === 'apply') {
      // Unreachable: a payment that exists is never at sequence 0.
      throw new Error(`payment ${event.payment_id} appeared concurrently in an unexpected state`);
    }
    return toResult(decision);
  }

  if (decision.kind !== 'apply') return toResult(decision);

  await client.query(
    'UPDATE payments SET status = $2, last_sequence = $3, updated_at = now() WHERE payment_id = $1',
    [event.payment_id, decision.status, event.sequence],
  );

  if (decision.ledgerEntry) {
    const credit = decision.ledgerEntry === 'credit';
    await client.query(
      `INSERT INTO ledger_entries (event_id, merchant_id, payment_id, entry_type, amount_cents, occurred_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [event.event_id, event.merchant_id, event.payment_id, decision.ledgerEntry, event.amount_cents, event.occurred_at],
    );
    await client.query(
      `INSERT INTO merchant_balances AS b (merchant_id, balance_cents, confirmed_count, refunded_count)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (merchant_id) DO UPDATE SET
         balance_cents   = b.balance_cents   + EXCLUDED.balance_cents,
         confirmed_count = b.confirmed_count + EXCLUDED.confirmed_count,
         refunded_count  = b.refunded_count  + EXCLUDED.refunded_count,
         updated_at      = now()`,
      [event.merchant_id, credit ? event.amount_cents : -event.amount_cents, credit ? 1 : 0, credit ? 0 : 1],
    );
  }

  return toResult(decision);
}

async function lockPayment(client: pg.PoolClient, paymentId: string): Promise<PaymentState | null> {
  const { rows } = await client.query<PaymentRow>(
    'SELECT merchant_id, amount_cents, status, last_sequence FROM payments WHERE payment_id = $1 FOR UPDATE',
    [paymentId],
  );
  const row = rows[0];
  if (!row) return null;
  return {
    merchantId: row.merchant_id,
    amountCents: Number(row.amount_cents),
    status: row.status,
    lastSequence: row.last_sequence,
  };
}

function toResult(decision: Decision): ApplyResult {
  switch (decision.kind) {
    case 'apply':
      return { outcome: 'applied', status: decision.status, ledgerEntry: decision.ledgerEntry };
    case 'duplicate':
      return { outcome: 'duplicate' };
    case 'gap':
      return { outcome: 'gap', reason: decision.reason };
    case 'conflict':
      return { outcome: 'conflict', reason: decision.reason };
  }
}
