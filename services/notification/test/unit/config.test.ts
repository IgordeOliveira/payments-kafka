import { ConfigError } from '@payments-kafka/consumer-runtime';
import { describe, expect, it } from 'vitest';
import { DEFAULT_DATABASE_URL, loadNotificationConfig } from '../../src/config.js';

describe('loadNotificationConfig', () => {
  it('defaults to group notification and the local notification database', () => {
    const config = loadNotificationConfig({});
    expect(config.runtime.groupId).toBe('notification');
    expect(config.databaseUrl).toBe(DEFAULT_DATABASE_URL);
  });

  it('reads DATABASE_URL and the shared consumer variables', () => {
    const config = loadNotificationConfig({
      DATABASE_URL: 'postgres://pix:pix@postgres:5432/notification',
      KAFKA_BOOTSTRAP_SERVERS: 'kafka:29092',
      RETRY_MAX_ATTEMPTS: '2',
    });
    expect(config.databaseUrl).toBe('postgres://pix:pix@postgres:5432/notification');
    expect(config.runtime).toMatchObject({ kafkaBootstrapServers: 'kafka:29092', retryMaxAttempts: 2 });
  });

  it('rejects an empty DATABASE_URL', () => {
    expect(() => loadNotificationConfig({ DATABASE_URL: '' })).toThrow(ConfigError);
  });
});
