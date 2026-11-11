import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Logger } from '@payments-kafka/consumer-runtime';
import pg from 'pg';

/** `services/notification/migrations`, both from `src/` (tests) and `dist/` (built). */
export const MIGRATIONS_DIR = fileURLToPath(new URL('../migrations/', import.meta.url));

/** Arbitrary constant identifying the migration lock among advisory locks. */
const MIGRATION_LOCK_ID = 727_001;

/**
 * A small pool for the dedupe store. The timeouts make an unreachable
 * database fail the handler (a retryable error) instead of blocking the
 * partition indefinitely.
 */
export function createPool(databaseUrl: string, logger?: Logger): pg.Pool {
  const pool = new pg.Pool({
    connectionString: databaseUrl,
    max: 5,
    connectionTimeoutMillis: 5_000,
    query_timeout: 10_000,
  });
  // An idle client losing its connection (e.g. Postgres restarted) emits
  // 'error' on the pool; without a listener it would crash the process.
  pool.on('error', (error) => {
    logger?.warn({ err: error.message }, 'idle database connection failed');
  });
  return pool;
}

/**
 * Applies the plain SQL files of `dir` in file-name order, each at most once
 * (tracked in `schema_migrations`). Runs in one transaction under an advisory
 * lock, so instances starting at the same time do not race. Returns the
 * versions applied by this call (empty when already up to date).
 */
export async function runMigrations(
  pool: pg.Pool,
  options: { dir?: string; logger?: Logger } = {},
): Promise<string[]> {
  const dir = options.dir ?? MIGRATIONS_DIR;
  const files = (await readdir(dir)).filter((file) => file.endsWith('.sql')).sort();
  const client = await pool.connect();
  const applied: string[] = [];
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version    TEXT        PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`);
    const { rows } = await client.query<{ version: string }>('SELECT version FROM schema_migrations');
    const done = new Set(rows.map((row) => row.version));
    for (const file of files) {
      if (done.has(file)) continue;
      await client.query(await readFile(path.join(dir, file), 'utf8'));
      await client.query('INSERT INTO schema_migrations (version) VALUES ($1)', [file]);
      applied.push(file);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
  options.logger?.info({ applied, total: files.length }, 'database migrations up to date');
  return applied;
}
