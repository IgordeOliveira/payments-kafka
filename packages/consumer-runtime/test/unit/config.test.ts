import { describe, expect, it } from 'vitest';
import { envInteger, envNumber, loadConfig } from '../../src/config.js';
import { ConfigError } from '../../src/errors.js';

const load = (env: Record<string, string>) => loadConfig({ serviceName: 'reconciliation', env });

describe('loadConfig', () => {
  it('applies the defaults of docs/architecture.md', () => {
    expect(load({})).toEqual({
      kafkaBootstrapServers: 'localhost:9092',
      groupId: 'reconciliation',
      retryMaxAttempts: 4,
      retryBackoffMs: 1000,
      logLevel: 'info',
    });
  });

  it('reads every variable', () => {
    expect(
      load({
        KAFKA_BOOTSTRAP_SERVERS: 'kafka:29092, other:9093',
        KAFKA_GROUP_ID: 'reconciliation-b',
        RETRY_MAX_ATTEMPTS: '0',
        RETRY_BACKOFF_MS: '250',
        LOG_LEVEL: 'DEBUG',
      }),
    ).toEqual({
      kafkaBootstrapServers: 'kafka:29092,other:9093',
      groupId: 'reconciliation-b',
      retryMaxAttempts: 0,
      retryBackoffMs: 250,
      logLevel: 'debug',
    });
  });

  it('reads process.env when no env is given', () => {
    expect(loadConfig({ serviceName: 'antifraud' }).groupId).toBe(
      process.env['KAFKA_GROUP_ID'] ?? 'antifraud',
    );
  });

  const invalid: Array<[string, string, RegExp]> = [
    [
      'RETRY_MAX_ATTEMPTS',
      '-1',
      /^Invalid RETRY_MAX_ATTEMPTS: expected a non-negative integer, got "-1"$/,
    ],
    ['RETRY_MAX_ATTEMPTS', 'four', /^Invalid RETRY_MAX_ATTEMPTS/],
    ['RETRY_MAX_ATTEMPTS', '1.5', /^Invalid RETRY_MAX_ATTEMPTS/],
    ['RETRY_MAX_ATTEMPTS', '', /^Invalid RETRY_MAX_ATTEMPTS/],
    ['RETRY_BACKOFF_MS', '-5', /^Invalid RETRY_BACKOFF_MS: expected a non-negative integer/],
    ['RETRY_BACKOFF_MS', '1s', /^Invalid RETRY_BACKOFF_MS/],
    [
      'LOG_LEVEL',
      'verbose',
      /^Invalid LOG_LEVEL: expected one of trace, debug, info, warn, error, fatal, silent, got "verbose"$/,
    ],
    ['KAFKA_GROUP_ID', '  ', /^Invalid KAFKA_GROUP_ID: must not be empty$/],
    [
      'KAFKA_BOOTSTRAP_SERVERS',
      'localhost',
      /^Invalid KAFKA_BOOTSTRAP_SERVERS: expected a comma-separated list of host:port/,
    ],
    ['KAFKA_BOOTSTRAP_SERVERS', 'localhost:99999', /^Invalid KAFKA_BOOTSTRAP_SERVERS/],
    ['KAFKA_BOOTSTRAP_SERVERS', 'a:1,,b:2', /^Invalid KAFKA_BOOTSTRAP_SERVERS/],
  ];

  it.each(invalid)('fails with an error naming %s for %j', (variable, value, message) => {
    let thrown: unknown;
    try {
      load({ [variable]: value });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(ConfigError);
    expect((thrown as ConfigError).variable).toBe(variable);
    expect((thrown as ConfigError).message).toMatch(message);
  });
});

describe('env helpers for service-specific variables', () => {
  it('envInteger honours bounds', () => {
    expect(envInteger({ X: '5' }, 'X', 1, { min: 1, max: 10 })).toBe(5);
    expect(() => envInteger({ X: '11' }, 'X', 1, { min: 1, max: 10 })).toThrow(
      'Invalid X: expected an integer between 1 and 10, got "11"',
    );
  });

  it('envNumber accepts fractions', () => {
    expect(envNumber({ F: '2.5' }, 'F', 5, { min: 0 })).toBe(2.5);
    expect(envNumber({}, 'F', 5)).toBe(5);
    expect(() => envNumber({ F: 'x' }, 'F', 5, { min: 0 })).toThrow(
      'Invalid F: expected a non-negative number, got "x"',
    );
  });
});
