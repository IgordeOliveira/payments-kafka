import { describe, expect, it } from 'vitest';
import { createPool, runMigrations } from '../../src/db.js';
import { capturingNotifier, freshDatabase, freshTopics, startService } from './harness.js';
import { queryOnce } from './postgres.js';

async function columns(databaseUrl: string): Promise<Array<{ column_name: string; data_type: string }>> {
  return queryOnce(
    databaseUrl,
    `SELECT column_name, data_type FROM information_schema.columns
     WHERE table_name = 'sent_notifications' ORDER BY ordinal_position`,
  );
}

describe('migrations', () => {
  it('creates sent_notifications when the service starts', async () => {
    const databaseUrl = await freshDatabase();
    await startService({ topics: await freshTopics(), databaseUrl, notifier: capturingNotifier().notifier });

    expect(await columns(databaseUrl)).toEqual([
      { column_name: 'notification_key', data_type: 'text' },
      { column_name: 'kind', data_type: 'text' },
      { column_name: 'merchant_id', data_type: 'text' },
      { column_name: 'sent_at', data_type: 'timestamp with time zone' },
    ]);
    const [pk] = await queryOnce<{ column_name: string }>(
      databaseUrl,
      `SELECT kcu.column_name FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu USING (constraint_name)
       WHERE tc.table_name = 'sent_notifications' AND tc.constraint_type = 'PRIMARY KEY'`,
    );
    expect(pk?.column_name).toBe('notification_key');
  });

  it('is safe to run twice, sequentially and concurrently, and keeps existing rows', async () => {
    const databaseUrl = await freshDatabase();
    const pools = [createPool(databaseUrl), createPool(databaseUrl), createPool(databaseUrl)];
    try {
      expect(await runMigrations(pools[0]!)).toEqual(['001_create_sent_notifications.sql']);
      await queryOnce(
        databaseUrl,
        `INSERT INTO sent_notifications (notification_key, kind, merchant_id) VALUES ('k1', 'fraud_alert', 'm1')`,
      );

      expect(await runMigrations(pools[0]!)).toEqual([]);
      // Two instances starting at the same time.
      expect(await Promise.all([runMigrations(pools[1]!), runMigrations(pools[2]!)])).toEqual([[], []]);

      expect(await columns(databaseUrl)).toHaveLength(4);
      expect(await queryOnce(databaseUrl, 'SELECT notification_key FROM sent_notifications')).toEqual([
        { notification_key: 'k1' },
      ]);
      expect(await queryOnce(databaseUrl, 'SELECT version FROM schema_migrations')).toEqual([
        { version: '001_create_sent_notifications.sql' },
      ]);
    } finally {
      await Promise.all(pools.map((pool) => pool.end()));
    }
  });

  it('applies concurrently started migrations exactly once on a fresh database', async () => {
    const databaseUrl = await freshDatabase();
    const pools = [createPool(databaseUrl), createPool(databaseUrl)];
    try {
      const results = await Promise.all(pools.map((pool) => runMigrations(pool)));
      expect(results.flat()).toEqual(['001_create_sent_notifications.sql']);
    } finally {
      await Promise.all(pools.map((pool) => pool.end()));
    }
  });
});
