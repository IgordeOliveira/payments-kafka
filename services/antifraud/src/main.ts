import {
  createConsumerRuntime,
  createLogger,
  loadConfig,
  TOPICS,
} from '@payments-kafka/consumer-runtime';
import { loadAntifraudConfig } from './config.js';
import { createAntifraudService } from './service.js';

const SERVICE_NAME = 'antifraud';

async function main(): Promise<void> {
  const config = loadConfig({ serviceName: SERVICE_NAME });
  const antifraudConfig = loadAntifraudConfig(process.env);
  const logger = createLogger({ level: config.logLevel, bindings: { service: SERVICE_NAME } });

  const service = createAntifraudService({ config: antifraudConfig, logger });
  const runtime = createConsumerRuntime({
    config,
    logger,
    topics: [{ topic: TOPICS.payments, schema: 'payment-event' }],
    handler: service.handler,
    onPartitionsAssigned: service.onPartitionsAssigned,
    onPartitionsRevoked: service.onPartitionsRevoked,
  });

  logger.info({ thresholds: antifraudConfig }, 'antifraud thresholds loaded');
  await runtime.start();
  await runtime.stopped;
}

main().catch((error: unknown) => {
  createLogger().fatal({ err: error instanceof Error ? error.message : String(error) }, 'startup failed');
  process.exit(1);
});
