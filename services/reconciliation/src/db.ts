import { envString } from '@payments-kafka/consumer-runtime';

export const DEFAULT_DATABASE_URL = 'postgres://pix:pix@localhost:5432/reconciliation';

/** `DATABASE_URL` (docs/architecture.md, "Environment variables"). */
export function databaseUrl(env: NodeJS.ProcessEnv = process.env): string {
  return envString(env, 'DATABASE_URL', DEFAULT_DATABASE_URL);
}
