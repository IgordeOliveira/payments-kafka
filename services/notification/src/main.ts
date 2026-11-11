import { createLogger } from '@payments-kafka/consumer-runtime';
import { loadNotificationConfig, SERVICE_NAME } from './config.js';
import { startNotificationService } from './service.js';

async function main(): Promise<void> {
  const { runtime: config, databaseUrl } = loadNotificationConfig();
  const logger = createLogger({ level: config.logLevel, bindings: { service: SERVICE_NAME } });
  const service = await startNotificationService({ config, databaseUrl, logger });
  // Resolves after SIGTERM/SIGINT: in-flight records were finished and
  // committed, the instance left the group and the database pool is closed.
  await service.stopped;
}

main().catch((error: unknown) => {
  // ConfigError, unreachable database or Kafka at startup.
  createLogger({ bindings: { service: SERVICE_NAME } }).fatal(
    { err: error instanceof Error ? error.message : String(error) },
    'startup failed',
  );
  process.exit(1);
});
