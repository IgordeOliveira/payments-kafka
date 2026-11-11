import { ConfigError } from './errors.js';
import { LOG_LEVELS, type LogLevel } from './logger.js';

/** Runtime configuration, read from the environment by {@link loadConfig}. */
export interface RuntimeConfig {
  /** `KAFKA_BOOTSTRAP_SERVERS`, default `localhost:9092`. */
  kafkaBootstrapServers: string;
  /** `KAFKA_GROUP_ID`, default the service name. */
  groupId: string;
  /** `RETRY_MAX_ATTEMPTS`: retries before the DLQ, default `4`. `0` sends the first failure to the DLQ. */
  retryMaxAttempts: number;
  /** `RETRY_BACKOFF_MS`: base delay of the exponential backoff, default `1000`. */
  retryBackoffMs: number;
  /** `LOG_LEVEL`, default `info`. */
  logLevel: LogLevel;
}

export type Env = Readonly<Record<string, string | undefined>>;

export interface LoadConfigOptions {
  /** Service name; the default consumer group id (`antifraud`, `notification`, `reconciliation`). */
  serviceName: string;
  /** Environment to read. Defaults to `process.env`. */
  env?: Env;
}

/**
 * Reads the consumer environment variables defined in `docs/architecture.md`
 * and applies their defaults. Throws a {@link ConfigError} naming the variable
 * on the first invalid value, so a misconfigured service fails at startup.
 */
export function loadConfig(options: LoadConfigOptions): RuntimeConfig {
  const env = options.env ?? process.env;
  return {
    kafkaBootstrapServers: envBootstrapServers(env, 'KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    groupId: envString(env, 'KAFKA_GROUP_ID', options.serviceName),
    retryMaxAttempts: envInteger(env, 'RETRY_MAX_ATTEMPTS', 4, { min: 0 }),
    retryBackoffMs: envInteger(env, 'RETRY_BACKOFF_MS', 1000, { min: 0 }),
    logLevel: envEnum(env, 'LOG_LEVEL', 'info', LOG_LEVELS),
  };
}

// ---------------------------------------------------------------------------
// Helpers, exported so services can read their own variables (ANTIFRAUD_*,
// DATABASE_URL...) with the same behavior: unset means default, anything
// else must be valid or startup fails with a ConfigError naming the variable.
// ---------------------------------------------------------------------------

function raw(env: Env, name: string): string | undefined {
  const value = env[name];
  return value === undefined ? undefined : value.trim();
}

/** A non-empty string. An empty value is an error, not "use the default". */
export function envString(env: Env, name: string, defaultValue: string): string {
  const value = raw(env, name);
  if (value === undefined) return defaultValue;
  if (value === '') throw new ConfigError(name, `Invalid ${name}: must not be empty`);
  return value;
}

/** An integer written in decimal digits, optionally bounded. */
export function envInteger(
  env: Env,
  name: string,
  defaultValue: number,
  bounds: { min?: number; max?: number } = {},
): number {
  const value = raw(env, name);
  if (value === undefined) return defaultValue;
  const describe = describeBounds(bounds);
  if (!/^-?\d+$/.test(value)) {
    throw new ConfigError(name, `Invalid ${name}: expected ${describe}, got "${value}"`);
  }
  const parsed = Number(value);
  if (
    !Number.isSafeInteger(parsed) ||
    (bounds.min !== undefined && parsed < bounds.min) ||
    (bounds.max !== undefined && parsed > bounds.max)
  ) {
    throw new ConfigError(name, `Invalid ${name}: expected ${describe}, got "${value}"`);
  }
  return parsed;
}

/** A finite decimal number (integer or fraction), optionally bounded. */
export function envNumber(
  env: Env,
  name: string,
  defaultValue: number,
  bounds: { min?: number; max?: number } = {},
): number {
  const value = raw(env, name);
  if (value === undefined) return defaultValue;
  const parsed = value === '' ? Number.NaN : Number(value);
  if (
    !Number.isFinite(parsed) ||
    (bounds.min !== undefined && parsed < bounds.min) ||
    (bounds.max !== undefined && parsed > bounds.max)
  ) {
    throw new ConfigError(
      name,
      `Invalid ${name}: expected ${describeBounds(bounds, 'a number')}, got "${value}"`,
    );
  }
  return parsed;
}

/** One of a fixed set of values (case-insensitive). */
export function envEnum<const T extends string>(
  env: Env,
  name: string,
  defaultValue: T,
  allowed: readonly T[],
): T {
  const value = raw(env, name);
  if (value === undefined) return defaultValue;
  const match = allowed.find((candidate) => candidate === value.toLowerCase());
  if (match === undefined) {
    throw new ConfigError(
      name,
      `Invalid ${name}: expected one of ${allowed.join(', ')}, got "${value}"`,
    );
  }
  return match;
}

/** A comma-separated list of `host:port`. */
export function envBootstrapServers(env: Env, name: string, defaultValue: string): string {
  const value = raw(env, name);
  if (value === undefined) return defaultValue;
  const entries = value.split(',').map((entry) => entry.trim());
  const valid =
    value !== '' &&
    entries.every((entry) => {
      const match = /^([^\s:,/]+):(\d{1,5})$/.exec(entry);
      if (!match) return false;
      const port = Number(match[2]);
      return port >= 1 && port <= 65535;
    });
  if (!valid) {
    throw new ConfigError(
      name,
      `Invalid ${name}: expected a comma-separated list of host:port, got "${value}"`,
    );
  }
  return entries.join(',');
}

function describeBounds(bounds: { min?: number; max?: number }, kind = 'an integer'): string {
  const { min, max } = bounds;
  const noun = kind.replace(/^an? /, '');
  if (min !== undefined && max !== undefined) return `${kind} between ${min} and ${max}`;
  if (min === 0) return `a non-negative ${noun}`;
  if (min !== undefined) return `${kind} >= ${min}`;
  if (max !== undefined) return `${kind} <= ${max}`;
  return kind;
}
