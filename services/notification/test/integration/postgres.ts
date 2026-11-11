import { randomUUID } from 'node:crypto';
import pg from 'pg';

/** Same major version as Compose; override with POSTGRES_TEST_IMAGE. */
export const POSTGRES_IMAGE = process.env['POSTGRES_TEST_IMAGE'] ?? 'postgres:18.6';

/** Creates a fresh, empty database on the server of `adminUrl` and returns its URL. */
export async function createDatabase(adminUrl: string): Promise<string> {
  const name = `notification_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  try {
    await admin.query(`CREATE DATABASE ${name}`);
  } finally {
    await admin.end();
  }
  const url = new URL(adminUrl);
  url.pathname = `/${name}`;
  return url.toString();
}

/** Runs one query on a short-lived connection (independent of the service's pool). */
export async function queryOnce<R extends pg.QueryResultRow>(
  databaseUrl: string,
  sql: string,
  params: unknown[] = [],
): Promise<R[]> {
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    return (await client.query<R>(sql, params)).rows;
  } finally {
    await client.end();
  }
}
