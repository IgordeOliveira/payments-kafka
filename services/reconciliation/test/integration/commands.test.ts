import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { applyEvent } from '../../src/apply.js';
import { EXIT, runCli } from '../../src/run-cli.js';
import { countRows, createDatabase, createEmptyDatabase, generateStream, lifecycle, type TestDatabase } from './helpers.js';

async function cli(db: { url: string }, ...argv: string[]) {
  let stdout = '';
  let stderr = '';
  const code = await runCli(argv, {
    stdout: (t) => (stdout += t),
    stderr: (t) => (stderr += t),
    env: { DATABASE_URL: db.url },
  });
  return { code, stdout, stderr };
}

async function seed(db: TestDatabase, merchants = ['merchant-a', 'merchant-b']) {
  for (const merchant of merchants) {
    for (const event of [
      ...lifecycle(merchant, 1000, 'confirmed'),
      ...lifecycle(merchant, 2000, 'confirmed'),
      ...lifecycle(merchant, 500, 'refunded'),
      ...lifecycle(merchant, 700, 'expired'),
    ]) {
      await applyEvent(db.pool, event);
    }
  }
}

describe('check', () => {
  it('reports no mismatch and exits 0 on a consistent state', async () => {
    const db = await createDatabase();
    await seed(db);
    const { code, stdout, stderr } = await cli(db, 'check');
    expect(code).toBe(EXIT.ok);
    expect(stderr).toBe('');
    expect(JSON.parse(stdout)).toEqual({
      ok: true,
      merchants: 2,
      ledger_entries: 8,
      processed_events: 18,
      mismatches: [],
    });
  });

  it('passes on an empty (even unmigrated) database', async () => {
    const db = await createEmptyDatabase();
    const { code, stdout } = await cli(db, 'check');
    expect(code).toBe(EXIT.ok);
    expect(JSON.parse(stdout)).toMatchObject({ ok: true, merchants: 0 });
  });

  it('reports a tampered balance and exits non-zero', async () => {
    const db = await createDatabase();
    await seed(db);
    await db.pool.query(`UPDATE merchant_balances SET balance_cents = balance_cents + 1 WHERE merchant_id = 'merchant-b'`);
    const { code, stdout, stderr } = await cli(db, 'check');
    expect(code).toBe(EXIT.mismatch);
    expect(JSON.parse(stdout)).toMatchObject({
      ok: false,
      mismatches: [{ merchant_id: 'merchant-b', field: 'balance_cents', stored: 3001, ledger: 3000 }],
    });
    expect(stderr).toContain('MISMATCH merchant=merchant-b field=balance_cents');
  });

  it('reports tampered counters, a missing balance row and a balance without ledger', async () => {
    const db = await createDatabase();
    await seed(db, ['merchant-a', 'merchant-b', 'merchant-c']);
    await db.pool.query(`UPDATE merchant_balances SET refunded_count = 0 WHERE merchant_id = 'merchant-a'`);
    await db.pool.query(`DELETE FROM merchant_balances WHERE merchant_id = 'merchant-b'`);
    await db.pool.query(`DELETE FROM ledger_entries WHERE merchant_id = 'merchant-c'`);
    const { code, stdout } = await cli(db, 'check');
    expect(code).toBe(EXIT.mismatch);
    expect(JSON.parse(stdout).mismatches).toEqual([
      { merchant_id: 'merchant-a', field: 'refunded_count', stored: 0, ledger: 1 },
      { merchant_id: 'merchant-b', field: 'balance_cents', stored: null, ledger: 3000 },
      { merchant_id: 'merchant-b', field: 'confirmed_count', stored: null, ledger: 3 },
      { merchant_id: 'merchant-b', field: 'refunded_count', stored: null, ledger: 1 },
      { merchant_id: 'merchant-c', field: 'balance_cents', stored: 3000, ledger: 0 },
      { merchant_id: 'merchant-c', field: 'confirmed_count', stored: 3, ledger: 0 },
      { merchant_id: 'merchant-c', field: 'refunded_count', stored: 1, ledger: 0 },
    ]);
  });
});

describe('balances', () => {
  it('prints stable JSON sorted by merchant_id (code-point order), independent of insertion order', async () => {
    const ids = ['merchant-b', 'Merchant-z', 'merchant-10', 'merchant-9', 'merchant-a', 'merchant-é'];
    const first = await createDatabase();
    const second = await createDatabase();
    await seed(first, ids);
    await seed(second, [...ids].reverse());

    const out1 = await cli(first, 'balances');
    const out2 = await cli(second, 'balances');
    expect(out1.code).toBe(EXIT.ok);
    expect(out1.stderr).toBe('');
    expect(out1.stdout).toBe(out2.stdout);
    expect(out1.stdout).toBe((await cli(first, 'balances')).stdout);

    const parsed = JSON.parse(out1.stdout) as Array<Record<string, unknown>>;
    expect(parsed.map((b) => b.merchant_id)).toEqual([
      'Merchant-z',
      'merchant-10',
      'merchant-9',
      'merchant-a',
      'merchant-b',
      'merchant-é',
    ]);
    expect(parsed[0]).toEqual({ merchant_id: 'Merchant-z', balance_cents: 3000, confirmed_count: 3, refunded_count: 1 });
    expect(out1.stdout.startsWith('[\n  {\n    "merchant_id": "Merchant-z",\n    "balance_cents": 3000,')).toBe(true);
    expect(out1.stdout.endsWith('}\n]\n')).toBe(true);
  });

  it('prints an empty array when there is no balance', async () => {
    const db = await createDatabase();
    expect(await cli(db, 'balances')).toEqual({ code: EXIT.ok, stdout: '[]\n', stderr: '' });
  });
});

describe('reset-state', () => {
  it('refuses without --yes and deletes nothing', async () => {
    const db = await createDatabase();
    await seed(db);
    const { code, stdout, stderr } = await cli(db, 'reset-state');
    expect(code).toBe(EXIT.usage);
    expect(stdout).toBe('');
    expect(stderr).toContain('--yes');
    for (const table of ['processed_events', 'payments', 'ledger_entries', 'merchant_balances']) {
      expect(await countRows(db.pool, table)).toBeGreaterThan(0);
    }
  });

  it('with --yes empties the four tables and keeps the schema', async () => {
    const db = await createDatabase();
    await seed(db);
    const { code, stdout } = await cli(db, 'reset-state', '--yes');
    expect(code).toBe(EXIT.ok);
    expect(stdout).toBe('');
    for (const table of ['processed_events', 'payments', 'ledger_entries', 'merchant_balances']) {
      expect(await countRows(db.pool, table)).toBe(0);
    }
    // Events can be applied again from scratch.
    const events = generateStream({ seed: 3, merchants: 1, paymentsPerMerchant: 3, duplicateRate: 0, outOfOrderRate: 0 }).records;
    for (const event of events) expect((await applyEvent(db.pool, event)).outcome).toBe('applied');
    expect((await cli(db, 'check')).code).toBe(EXIT.ok);
  });
});

describe('usage and errors', () => {
  it('rejects unknown commands and arguments with exit code 2', async () => {
    const db = { url: 'postgres://unused' };
    expect((await cli(db, 'nope')).code).toBe(EXIT.usage);
    expect((await cli(db, 'balances', '--yes')).code).toBe(EXIT.usage);
    expect((await cli(db)).code).toBe(EXIT.usage);
    expect((await cli(db, '--help')).code).toBe(EXIT.ok);
  });

  it('exits 3 when the database is unreachable', async () => {
    const { code, stdout, stderr } = await cli({ url: 'postgres://pix:pix@127.0.0.1:1/reconciliation' }, 'balances');
    expect(code).toBe(EXIT.error);
    expect(stdout).toBe('');
    expect(stderr).toContain('balances failed');
  });
});

describe('the CLI entry point as a process', () => {
  const entry = fileURLToPath(new URL('../../src/cli.ts', import.meta.url));
  const run = (url: string, ...args: string[]) =>
    spawnSync(process.execPath, ['--import', 'tsx', entry, ...args], {
      env: { ...process.env, DATABASE_URL: url },
      encoding: 'utf8',
    });

  it('writes only the JSON to stdout and sets the exit codes', async () => {
    const db = await createDatabase();
    await seed(db);

    const balances = run(db.url, 'balances');
    expect(balances.status).toBe(0);
    expect(balances.stdout).toBe((await cli(db, 'balances')).stdout);

    await db.pool.query(`UPDATE merchant_balances SET balance_cents = 0 WHERE merchant_id = 'merchant-a'`);
    expect(run(db.url, 'check').status).toBe(EXIT.mismatch);
    expect(run(db.url, 'reset-state').status).toBe(EXIT.usage);
    expect(run(db.url, 'reset-state', '--yes').status).toBe(EXIT.ok);
    const check = run(db.url, 'check');
    expect(check.status).toBe(EXIT.ok);
    expect(JSON.parse(check.stdout)).toMatchObject({ ok: true, merchants: 0 });
  });
});
