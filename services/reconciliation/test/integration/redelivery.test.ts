/**
 * Crash safety with a real process kill: the consumer runs in a child process
 * and is SIGKILLed at a precise point of handling one event.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import type { PaymentEvent } from '@payments-kafka/consumer-runtime';
import { collect, committedOffsets, uniqueId, waitFor } from '@payments-kafka/consumer-runtime/testing';
import { afterEach, describe, expect, it } from 'vitest';
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

const fixture = fileURLToPath(new URL('../fixtures/crash-consumer.ts', import.meta.url));
const children: ChildProcess[] = [];

afterEach(() => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  }
});

function spawnCrashConsumer(env: Record<string, string>) {
  const child = spawn(process.execPath, ['--import', 'tsx', fixture], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
  children.push(child);
  const messages: Array<Record<string, unknown>> = [];
  let stderr = '';
  child.on('message', (m) => messages.push(m as Record<string, unknown>));
  child.stderr?.setEncoding('utf8').on('data', (chunk: string) => {
    stderr += chunk;
  });
  const exited = new Promise<void>((resolve) => child.on('exit', () => resolve()));
  return {
    kill: async () => {
      child.kill('SIGKILL');
      await exited;
    },
    next: (type: string) =>
      waitFor(() => messages.find((m) => m.type === type), {
        timeoutMs: 60_000,
        message: `child message "${type}"; stderr: ${stderr.slice(-2000)}`,
      }),
  };
}

describe('crash safety', () => {
  it('killed after the database commit and before the offset commit: the redelivered event counts once', async () => {
    const db = await createDatabase();
    const topics = await newTopics();
    const groupId = uniqueId('reconciliation');
    const [created, confirmed] = lifecycle('merchant-crash-1', 3100, 'confirmed') as [PaymentEvent, PaymentEvent];
    await produceEvents(topics.payments, [created, confirmed]);
    const records = await collect(bootstrap, topics.payments, { count: 2 });
    const target = records.find((r) => (r.json() as PaymentEvent).event_id === confirmed.event_id)!;

    const child = spawnCrashConsumer({
      BOOTSTRAP: bootstrap,
      GROUP: groupId,
      TOPIC: topics.payments,
      RETRY_TOPIC: topics.retry,
      DLQ_TOPIC: topics.dlq,
      DATABASE_URL: db.url,
      TARGET_EVENT: confirmed.event_id,
      MODE: 'after-commit',
    });
    await child.next('committed');

    // The effect is in the database, but the offset of the event is not committed.
    expect(await balancesOf(db.pool)).toEqual([
      { merchant_id: 'merchant-crash-1', balance_cents: 3100, confirmed_count: 1, refunded_count: 0 },
    ]);
    const offsets = await committedOffsets(bootstrap, groupId, topics.payments);
    expect(Number(offsets[target.partition] ?? -1)).toBeLessThanOrEqual(Number(target.offset));

    await child.kill();

    // Restart: the same group receives the event again and recognizes it.
    const service = await startService({ pool: db.pool, topics, groupId });
    await waitFor(
      () => service.logs.find('duplicate event ignored').find((l) => l.event_id === confirmed.event_id),
      { timeoutMs: 60_000, message: 'redelivered event to be ignored as a duplicate' },
    );
    await waitFor(async () => {
      const after = await committedOffsets(bootstrap, groupId, topics.payments);
      return Number(after[target.partition] ?? -1) > Number(target.offset);
    });

    expect(await balancesOf(db.pool)).toEqual([
      { merchant_id: 'merchant-crash-1', balance_cents: 3100, confirmed_count: 1, refunded_count: 0 },
    ]);
    expect(await countRows(db.pool, 'ledger_entries')).toBe(1);
    expect(await countRows(db.pool, 'processed_events')).toBe(2);
  });

  it('killed inside the database transaction: nothing is stored and the redelivered event is applied in full', async () => {
    const db = await createDatabase();
    const topics = await newTopics();
    const groupId = uniqueId('reconciliation');
    const [created, confirmed] = lifecycle('merchant-crash-2', 2700, 'confirmed') as [PaymentEvent, PaymentEvent];
    await produceEvents(topics.payments, [created, confirmed]);

    const child = spawnCrashConsumer({
      BOOTSTRAP: bootstrap,
      GROUP: groupId,
      TOPIC: topics.payments,
      RETRY_TOPIC: topics.retry,
      DLQ_TOPIC: topics.dlq,
      DATABASE_URL: db.url,
      TARGET_EVENT: confirmed.event_id,
      MODE: 'before-commit',
    });
    await child.next('in-transaction');
    await child.kill();

    // Postgres aborts the open transaction when the connection drops.
    await waitFor(async () => {
      const { rows } = await db.pool.query<{ n: string }>(
        `SELECT COUNT(*) AS n FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'`,
      );
      return Number(rows[0]?.n) === 0;
    });
    expect(await paymentRow(db.pool, created.payment_id)).toMatchObject({ status: 'CREATED', last_sequence: 1 });
    expect(await countRows(db.pool, 'ledger_entries')).toBe(0);
    expect(await countRows(db.pool, 'merchant_balances')).toBe(0);
    expect(await countRows(db.pool, 'processed_events')).toBe(1);

    const service = await startService({ pool: db.pool, topics, groupId });
    await waitForProcessed(db.pool, 2);
    await waitFor(() => service.logs.find('payment event applied').find((l) => l.event_id === confirmed.event_id));
    expect(await paymentRow(db.pool, created.payment_id)).toMatchObject({ status: 'CONFIRMED', last_sequence: 2 });
    expect(await balancesOf(db.pool)).toEqual([
      { merchant_id: 'merchant-crash-2', balance_cents: 2700, confirmed_count: 1, refunded_count: 0 },
    ]);
    expect(await countRows(db.pool, 'ledger_entries')).toBe(1);
  });
});
