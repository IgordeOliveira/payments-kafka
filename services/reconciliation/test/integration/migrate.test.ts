import type pg from 'pg';
import { describe, expect, it } from 'vitest';
import { migrate } from '../../src/migrate.js';
import { createEmptyDatabase, newPool } from './helpers.js';

const TABLES = ['ledger_entries', 'merchant_balances', 'payments', 'processed_events', 'schema_migrations'];

async function tables(pool: pg.Pool): Promise<string[]> {
  const { rows } = await pool.query<{ table_name: string }>(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`,
  );
  return rows.map((r) => r.table_name);
}

describe('migrations', () => {
  it('create the four tables on an empty database', async () => {
    const db = await createEmptyDatabase();
    const result = await migrate(db.pool);
    expect(result.applied).toEqual(['001_initial_schema.sql']);
    expect(await tables(db.pool)).toEqual(TABLES);

    const { rows } = await db.pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'merchant_balances' ORDER BY ordinal_position`,
    );
    expect(rows.map((r) => r.column_name)).toEqual([
      'merchant_id',
      'balance_cents',
      'confirmed_count',
      'refunded_count',
      'updated_at',
    ]);
  });

  it('are re-runnable: a second run applies nothing and keeps the data', async () => {
    const db = await createEmptyDatabase();
    await migrate(db.pool);
    await db.pool.query(
      `INSERT INTO merchant_balances (merchant_id, balance_cents, confirmed_count, refunded_count) VALUES ('m', 1, 1, 0)`,
    );
    expect((await migrate(db.pool)).applied).toEqual([]);
    expect(await tables(db.pool)).toEqual(TABLES);
    const { rows } = await db.pool.query('SELECT merchant_id FROM merchant_balances');
    expect(rows).toHaveLength(1);
  });

  it('are applied once when several instances start at the same time', async () => {
    const db = await createEmptyDatabase();
    const instances = Array.from({ length: 5 }, () => newPool(db.url, 2));
    const results = await Promise.all(instances.map((pool) => migrate(pool)));
    expect(results.flatMap((r) => r.applied)).toEqual(['001_initial_schema.sql']);
    expect(await tables(db.pool)).toEqual(TABLES);
  });
});
