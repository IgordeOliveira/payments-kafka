/** Entry point of the reconciliation consumer: `node services/reconciliation/dist/main.js`. */
import { createLogger, loadConfig } from '@payments-kafka/consumer-runtime';
import pg from 'pg';
import { databaseUrl } from './db.js';
import { migrate } from './migrate.js';
import { createReconciliationRuntime, SERVICE_NAME } from './service.js';

async function main(): Promise<void> {
  const config = loadConfig({ serviceName: SERVICE_NAME });
  const logger = createLogger({ level: config.logLevel, bindings: { service: SERVICE_NAME } });
  const pool = new pg.Pool({ connectionString: databaseUrl() });
  // An idle client that loses its connection must not crash the process; the
  // next query gets a fresh connection (and a failing handler is retried).
  pool.on('error', (error) => logger.warn({ err: error.message }, 'idle database connection failed'));

  try {
    const { applied } = await migrate(pool);
    logger.info({ applied }, applied.length > 0 ? 'migrations applied' : 'schema up to date');

    const runtime = createReconciliationRuntime({ config, pool, logger });
    await runtime.start();
    await runtime.stopped;
  } finally {
    await pool.end();
  }
}

main().catch((error: unknown) => {
  createLogger({ bindings: { service: SERVICE_NAME } }).fatal(
    { err: error instanceof Error ? error.message : String(error) },
    'startup failed',
  );
  process.exit(1);
});
