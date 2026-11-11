/**
 * Thin access layer over @confluentinc/kafka-javascript (KafkaJS-compatible
 * API on top of librdkafka). Everything Kafka-specific that more than one
 * module needs lives here.
 */
import confluentKafka from '@confluentinc/kafka-javascript';
import type { KafkaJS as KafkaJSTypes } from '@confluentinc/kafka-javascript';
import type { Logger } from './logger.js';

export const { KafkaJS } = confluentKafka;

export type KafkaConsumer = KafkaJSTypes.Consumer;
export type KafkaProducer = KafkaJSTypes.Producer;
export type KafkaAdmin = KafkaJSTypes.Admin;

/**
 * librdkafka configuration properties (dotted names, see
 * https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md),
 * e.g. `{ 'session.timeout.ms': 10000 }`.
 */
export type LibrdkafkaConfig = Record<string, string | number | boolean>;

/**
 * Routes the client's own log calls into our JSON logger under
 * `component: "kafka-client"`. The client's `info` messages are chatty
 * (every rebalance, connect, disconnect) and the runtime already logs those
 * events itself, so they are demoted to `debug`.
 */
export function kafkaClientLogger(logger: Logger): KafkaJSTypes.Logger {
  const child = logger.child({ component: 'kafka-client' });
  const adapter: KafkaJSTypes.Logger = {
    info: (message, extra) => child.debug({ ...extra }, message),
    warn: (message, extra) => child.warn({ ...extra }, message),
    error: (message, extra) => child.error({ ...extra }, message),
    debug: (message, extra) => child.trace({ ...extra }, message),
    namespace: () => adapter,
    setLogLevel: () => undefined,
  };
  return adapter;
}

/** The client-side log level matching our logger's level. */
export function kafkaClientLogLevel(logger: Logger): KafkaJSTypes.logLevel {
  const { logLevel } = KafkaJS;
  switch (logger.level) {
    case 'trace':
      return logLevel.DEBUG;
    case 'debug':
      return logLevel.INFO;
    case 'silent':
      return logLevel.NOTHING;
    case 'fatal':
    case 'error':
      return logLevel.ERROR;
    default:
      return logLevel.WARN;
  }
}

export function newKafka(): KafkaJSTypes.Kafka {
  return new KafkaJS.Kafka({});
}

/** Creates an admin client (not connected). */
export function createAdmin(bootstrapServers: string, logger?: Logger): KafkaAdmin {
  const kafkaJS = logger
    ? { logger: kafkaClientLogger(logger), logLevel: kafkaClientLogLevel(logger) }
    : { logLevel: KafkaJS.logLevel.ERROR };
  return newKafka().admin({ 'bootstrap.servers': bootstrapServers, kafkaJS });
}

export const ERROR_CODES = KafkaJS.ErrorCodes;
