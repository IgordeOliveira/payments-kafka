/**
 * Several instances of one group, joining and leaving while a stream with
 * duplicates and out-of-order events is processed, must produce the same
 * state as a single instance. Out-of-order events go through the retry topic,
 * which any instance may own, so the same merchant (and the same payment) is
 * touched concurrently by different instances.
 *
 * It is also the regression test for a consumer-runtime fix: a retry
 * partition paused (retry not due yet) when a rebalance revoked it stayed
 * paused forever after being reassigned to the same member.
 */
import { collect, sleep, waitFor } from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import { checkConsistency } from '../../src/commands.js';
import {
  balancesOf,
  bootstrap,
  createDatabase,
  expectedBalances,
  generateStream,
  newPool,
  newTopics,
  produceEvents,
  snapshot,
  startService,
  waitForProcessed,
  type StartedService,
} from './helpers.js';

describe('multiple instances', () => {
  it('produce the same balances as a single instance while instances join and leave', { timeout: 240_000 }, async () => {
    const stream = generateStream({
      seed: 42,
      merchants: 12,
      paymentsPerMerchant: 25,
      outOfOrderRate: 0.2,
      duplicateRate: 0.1,
    });
    const topics = await newTopics();
    const WAVES = 5;
    const waves = Array.from({ length: WAVES }, (_, i) =>
      stream.records.slice(
        Math.floor((i * stream.records.length) / WAVES),
        Math.floor(((i + 1) * stream.records.length) / WAVES),
      ),
    );

    // --- scaled run: A, then B joins, A leaves, C joins, B leaves -------------
    const scaled = await createDatabase();
    const groupId = `reconciliation-scaled-${Date.now()}`;
    const instance = () => startService({ pool: newPool(scaled.url), topics, groupId });
    const assigned = (s: StartedService) =>
      waitFor(() => s.logs.find('partitions assigned').length > 0, { timeoutMs: 30_000, message: 'partitions assigned' });
    const handled = (s: StartedService) =>
      waitFor(() => s.logs.find('payment event applied').length > 0, { timeoutMs: 30_000, message: 'an applied event' });

    const a = await instance();
    await assigned(a);
    await produceEvents(topics.payments, waves[0]!);
    await handled(a);

    const b = await instance(); // join: the group rebalances while wave 1 may still be in flight
    await assigned(b);
    await produceEvents(topics.payments, waves[1]!);
    await handled(b);

    const stoppingA = a.runtime.stop(); // leave while wave 3 is being produced
    await produceEvents(topics.payments, waves[2]!);
    await stoppingA;

    const c = await instance(); // join
    await assigned(c);
    await produceEvents(topics.payments, waves[3]!);
    await handled(c);

    await b.runtime.stop(); // leave; C finishes alone
    await produceEvents(topics.payments, waves[4]!);

    await waitForProcessed(scaled.pool, stream.distinct.length, 120_000);

    // Every instance actually handled events, and retries happened.
    for (const s of [a, b, c]) expect(s.logs.find('payment event applied').length).toBeGreaterThan(0);
    const retried = [a, b, c].flatMap((s) => s.logs.find('payment event applied')).filter((l) => Number(l.attempt) > 0);
    expect(retried.length).toBeGreaterThan(0);

    // --- single-instance run over the same topic, fresh group and database ----
    const single = await createDatabase();
    await startService({ pool: single.pool, topics });
    await waitForProcessed(single.pool, stream.distinct.length, 120_000);

    // --- same result, equal to the pure function of the event set -------------
    await sleep(1_000); // nothing more may arrive
    const scaledSnapshot = await snapshot(scaled.pool);
    expect(scaledSnapshot.processed).toHaveLength(stream.distinct.length);
    expect(scaledSnapshot).toEqual(await snapshot(single.pool));
    expect(await balancesOf(scaled.pool)).toEqual(expectedBalances(stream.distinct));
    expect((await checkConsistency(scaled.pool)).mismatches).toEqual([]);
    expect(await collect(bootstrap, topics.dlq, { timeoutMs: 2_000 })).toEqual([]);
  });
});
