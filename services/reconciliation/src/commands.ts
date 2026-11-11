/** The queries behind the CLI commands `check`, `balances` and `reset-state`. */
import type pg from 'pg';

type Queryable = Pick<pg.Pool | pg.Client, 'query'>;

export interface MerchantBalance {
  merchant_id: string;
  balance_cents: number;
  confirmed_count: number;
  refunded_count: number;
}

export interface Mismatch {
  merchant_id: string;
  field: 'balance_cents' | 'confirmed_count' | 'refunded_count';
  /** Value in merchant_balances (`null` when the merchant has no balance row). */
  stored: number | null;
  /** Value derived from ledger_entries. */
  ledger: number;
}

export interface CheckReport {
  ok: boolean;
  merchants: number;
  ledger_entries: number;
  processed_events: number;
  mismatches: Mismatch[];
}

/** pg returns BIGINT and NUMERIC as strings; amounts here stay far below 2^53. */
function toNumber(value: string | number): number {
  const n = Number(value);
  if (!Number.isSafeInteger(n)) throw new Error(`value ${value} is not a safe integer`);
  return n;
}

/** Code-point order: stable across locales and database collations. */
function byMerchantId<T extends { merchant_id: string }>(a: T, b: T): number {
  return a.merchant_id < b.merchant_id ? -1 : a.merchant_id > b.merchant_id ? 1 : 0;
}

/** Every merchant balance, sorted by merchant_id. */
export async function listBalances(db: Queryable): Promise<MerchantBalance[]> {
  const { rows } = await db.query<{
    merchant_id: string;
    balance_cents: string;
    confirmed_count: string;
    refunded_count: string;
  }>('SELECT merchant_id, balance_cents, confirmed_count, refunded_count FROM merchant_balances');
  return rows
    .map((row) => ({
      merchant_id: row.merchant_id,
      balance_cents: toNumber(row.balance_cents),
      confirmed_count: toNumber(row.confirmed_count),
      refunded_count: toNumber(row.refunded_count),
    }))
    .sort(byMerchantId);
}

/** The `balances` output: pretty JSON with a trailing newline, byte-for-byte deterministic. */
export function formatBalances(balances: MerchantBalance[]): string {
  return `${JSON.stringify(balances, null, 2)}\n`;
}

/**
 * Compares, for every merchant present in either table, merchant_balances with
 * the totals derived from the ledger: balance = credits - debits,
 * confirmed_count = number of credits, refunded_count = number of debits.
 */
export async function checkConsistency(db: Queryable): Promise<CheckReport> {
  const { rows } = await db.query<{
    merchant_id: string;
    stored_balance: string | null;
    stored_confirmed: string | null;
    stored_refunded: string | null;
    ledger_balance: string;
    ledger_credits: string;
    ledger_debits: string;
  }>(`
    WITH ledger AS (
      SELECT merchant_id,
             SUM(CASE entry_type WHEN 'credit' THEN amount_cents ELSE -amount_cents END) AS balance,
             COUNT(*) FILTER (WHERE entry_type = 'credit') AS credits,
             COUNT(*) FILTER (WHERE entry_type = 'debit')  AS debits
        FROM ledger_entries
       GROUP BY merchant_id
    )
    SELECT COALESCE(b.merchant_id, l.merchant_id) AS merchant_id,
           b.balance_cents   AS stored_balance,
           b.confirmed_count AS stored_confirmed,
           b.refunded_count  AS stored_refunded,
           COALESCE(l.balance, 0) AS ledger_balance,
           COALESCE(l.credits, 0) AS ledger_credits,
           COALESCE(l.debits, 0)  AS ledger_debits
      FROM merchant_balances b
      FULL OUTER JOIN ledger l ON l.merchant_id = b.merchant_id`);

  const mismatches: Mismatch[] = [];
  for (const row of rows) {
    const pairs: Array<[Mismatch['field'], string | null, string]> = [
      ['balance_cents', row.stored_balance, row.ledger_balance],
      ['confirmed_count', row.stored_confirmed, row.ledger_credits],
      ['refunded_count', row.stored_refunded, row.ledger_debits],
    ];
    for (const [field, stored, ledger] of pairs) {
      const storedValue = stored === null ? null : toNumber(stored);
      const ledgerValue = toNumber(ledger);
      if (storedValue !== ledgerValue) {
        mismatches.push({ merchant_id: row.merchant_id, field, stored: storedValue, ledger: ledgerValue });
      }
    }
  }
  mismatches.sort((a, b) => byMerchantId(a, b) || (a.field < b.field ? -1 : a.field > b.field ? 1 : 0));

  const counts = await db.query<{ ledger_entries: string; processed_events: string }>(`
    SELECT (SELECT COUNT(*) FROM ledger_entries)   AS ledger_entries,
           (SELECT COUNT(*) FROM processed_events) AS processed_events`);
  const totals = counts.rows[0];

  return {
    ok: mismatches.length === 0,
    merchants: rows.length,
    ledger_entries: toNumber(totals?.ledger_entries ?? 0),
    processed_events: toNumber(totals?.processed_events ?? 0),
    mismatches,
  };
}

/** Deletes all reconciliation state (not the schema). Consumers must be stopped. */
export async function resetState(db: Queryable): Promise<void> {
  await db.query('TRUNCATE processed_events, payments, ledger_entries, merchant_balances');
}
