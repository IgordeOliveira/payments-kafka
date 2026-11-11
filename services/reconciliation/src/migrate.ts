/**
 * Plain SQL migrations, applied at startup (service and CLI).
 *
 * Files in `services/reconciliation/migrations/` named `NNN_description.sql`
 * are applied in name order, each once, recorded in `schema_migrations`. The
 * whole run is one transaction holding an advisory lock, so several instances
 * starting at the same time (`--scale reconciliation=3`) apply them once.
 */
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

/** Works from both `src/` (tests) and `dist/` (built service). */
export const MIGRATIONS_DIR = fileURLToPath(new URL('../migrations/', import.meta.url));

const MIGRATION_FILE = /^\d{3}_[a-z0-9_]+\.sql$/;

export interface MigrationResult {
  /** Migrations applied by this run (empty when the schema was up to date). */
  applied: string[];
}

export async function migrate(
  db: pg.Pool | pg.Client,
  options: { migrationsDir?: string } = {},
): Promise<MigrationResult> {
  const dir = options.migrationsDir ?? MIGRATIONS_DIR;
  const files = (await readdir(dir)).filter((name) => MIGRATION_FILE.test(name)).sort();

  const pooled = db instanceof pg.Pool ? await db.connect() : null;
  const client = pooled ?? (db as pg.Client);
  try {
    await client.query('BEGIN');
    try {
      await client.query(`SELECT pg_advisory_xact_lock(hashtext('reconciliation.schema_migrations'))`);
      await client.query(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    TEXT        PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`);
      const done = new Set(
        (await client.query<{ version: string }>('SELECT version FROM schema_migrations')).rows.map((r) => r.version),
      );
      const applied: string[] = [];
      for (const file of files) {
        if (done.has(file)) continue;
        await client.query(await readFile(join(dir, file), 'utf8'));
        await client.query('INSERT INTO schema_migrations (version) VALUES ($1)', [file]);
        applied.push(file);
      }
      await client.query('COMMIT');
      return { applied };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    }
  } finally {
    pooled?.release();
  }
}
