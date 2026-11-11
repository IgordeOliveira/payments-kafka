import { afterEach, describe, expect, it } from 'vitest';
import type pg from 'pg';
import { createPool, runMigrations } from '../../src/db.js';
import { createClaim, type NotificationClaim } from '../../src/dedupe.js';
import { freshDatabase } from './harness.js';
import { queryOnce } from './postgres.js';

const pools: pg.Pool[] = [];

afterEach(async () => {
  await Promise.all(pools.splice(0).map((pool) => pool.end()));
});

async function migratedPool(databaseUrl: string): Promise<pg.Pool> {
  const pool = createPool(databaseUrl);
  pools.push(pool);
  await runMigrations(pool);
  return pool;
}

const claimOf = (notificationKey: string): NotificationClaim => ({
  notificationKey,
  kind: 'payment.confirmed',
  merchantId: 'merchant-007',
});

describe('claim', () => {
  it('lets the first claim win and rejects the second', async () => {
    const databaseUrl = await freshDatabase();
    const claim = createClaim(await migratedPool(databaseUrl));

    expect(await claim(claimOf('event-1'))).toBe(true);
    expect(await claim(claimOf('event-1'))).toBe(false);
    expect(await claim(claimOf('event-2'))).toBe(true);

    const rows = await queryOnce(databaseUrl, 'SELECT notification_key, kind, merchant_id FROM sent_notifications ORDER BY 1');
    expect(rows).toEqual([
      { notification_key: 'event-1', kind: 'payment.confirmed', merchant_id: 'merchant-007' },
      { notification_key: 'event-2', kind: 'payment.confirmed', merchant_id: 'merchant-007' },
    ]);
  });

  it('is committed when it resolves, before anything is sent', async () => {
    const databaseUrl = await freshDatabase();
    const claim = createClaim(await migratedPool(databaseUrl));

    expect(await claim(claimOf('event-committed'))).toBe(true);
    // Visible to another connection (another instance) right away.
    expect(
      await queryOnce(databaseUrl, 'SELECT 1 FROM sent_notifications WHERE notification_key = $1', ['event-committed']),
    ).toHaveLength(1);
    // And a claim from another pool is rejected.
    const otherInstance = createClaim(await migratedPool(databaseUrl));
    expect(await otherInstance(claimOf('event-committed'))).toBe(false);
  });

  it('gives exactly one winner among concurrent claims from several instances', async () => {
    const databaseUrl = await freshDatabase();
    const instances = await Promise.all([1, 2, 3, 4].map(async () => createClaim(await migratedPool(databaseUrl))));

    for (const key of ['race-1', 'race-2', 'race-3']) {
      // 4 instances x 5 attempts, all in flight at once.
      const results = await Promise.all(instances.flatMap((claim) => [1, 2, 3, 4, 5].map(() => claim(claimOf(key)))));
      expect(results.filter(Boolean)).toHaveLength(1);
      expect(results).toHaveLength(20);
    }
    expect(await queryOnce(databaseUrl, 'SELECT count(*)::int AS n FROM sent_notifications')).toEqual([{ n: 3 }]);
  });

  it('rejects (retryable) when the database is unreachable', async () => {
    const pool = createPool('postgres://pix:pix@127.0.0.1:1/notification');
    pools.push(pool);
    await expect(createClaim(pool)(claimOf('event-x'))).rejects.toThrow();
  });
});
