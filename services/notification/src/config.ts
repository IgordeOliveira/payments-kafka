import { envString, loadConfig, type Env, type RuntimeConfig } from '@payments-kafka/consumer-runtime';

export const SERVICE_NAME = 'notification';
export const DEFAULT_DATABASE_URL = 'postgres://pix:pix@localhost:5432/notification';

export interface NotificationConfig {
  /** Kafka, retry and logging settings shared by every consumer. */
  runtime: RuntimeConfig;
  /** `DATABASE_URL`: the `notification` database holding the dedupe table. */
  databaseUrl: string;
}

/** Reads the configuration from the environment; throws `ConfigError` on invalid values. */
export function loadNotificationConfig(env: Env = process.env): NotificationConfig {
  return {
    runtime: loadConfig({ serviceName: SERVICE_NAME, env }),
    databaseUrl: envString(env, 'DATABASE_URL', DEFAULT_DATABASE_URL),
  };
}
