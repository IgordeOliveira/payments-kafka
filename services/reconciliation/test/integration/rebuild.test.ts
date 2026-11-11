/**
 * The replay procedure: export balances, reset state, reprocess the topic from
 * the earliest offset with a fresh group, export again and compare.
 */
import { uniqueId, waitFor } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import { EXIT, runCli } from '../../src/run-cli.js';
import {
  countRows,
  createDatabase,
  expectedBalances,
  generateStream,
  newTopics,
  produceEvents,
  startService,
  waitForProcessed,
} from './helpers.js';

async function cli(url: string, ...argv: string[]) {
  let stdout = '';
  const code = await runCli(argv, { stdout: (t) => (stdout += t), stderr: () => undefined, env: { DATABASE_URL: url } });
  return { code, stdout };
}

describe('deterministic rebuild', () => {
  it('reprocessing the topic from the earliest offset after reset-state reproduces identical balances', async () => {
    const stream = generateStream({ seed: 1234, merchants: 8, paymentsPerMerchant: 20, outOfOrderRate: 0.15, duplicateRate: 0.15 });
    const topics = await newTopics();
    const db = await createDatabase();
    await produceEvents(topics.payments, stream.records);

    const first = await startService({ pool: db.pool, topics, groupId: uniqueId('reconciliation') });
    await waitForProcessed(db.pool, stream.distinct.length);
    await first.runtime.stop();
    const before = await cli(db.url, 'balances');
    expect(before.code).toBe(EXIT.ok);
    expect(JSON.parse(before.stdout)).toEqual(expectedBalances(stream.distinct));

    // An offset reset alone is a no-op: every event is recognized as processed.
    const noop = await startService({ pool: db.pool, topics, groupId: uniqueId('reconciliation') });
    // The duplicates in the stream plus the replay of every distinct event.
    await waitForDuplicates(noop.logs, stream.records.length);
    await noop.runtime.stop();
    expect((await cli(db.url, 'balances')).stdout).toBe(before.stdout);

    // Reset, then replay from the earliest offset with a fresh group.
    expect((await cli(db.url, 'reset-state', '--yes')).code).toBe(EXIT.ok);
    expect(await countRows(db.pool, 'processed_events')).toBe(0);
    expect((await cli(db.url, 'balances')).stdout).toBe('[]\n');

    await startService({ pool: db.pool, topics, groupId: uniqueId('reconciliation') });
    await waitForProcessed(db.pool, stream.distinct.length);
    const after = await cli(db.url, 'balances');
    expect(after.stdout).toBe(before.stdout);
    expect((await cli(db.url, 'check')).code).toBe(EXIT.ok);
  });
});

async function waitForDuplicates(logs: { find(msg: string): unknown[] }, count: number) {
  await waitFor(() => logs.find('duplicate event ignored').length >= count, {
    timeoutMs: 60_000,
    message: `${count} duplicates to be ignored`,
  });
}
