export {
  createConsumerRuntime,
  type ConsumedMessage,
  type ConsumerRuntime,
  type ConsumerRuntimeOptions,
  type HandlerContext,
  type MessageHandler,
  type TopicPartition,
  type TopicSubscription,
} from './runtime.js';
export {
  envBootstrapServers,
  envEnum,
  envInteger,
  envNumber,
  envString,
  loadConfig,
  type Env,
  type LoadConfigOptions,
  type RuntimeConfig,
} from './config.js';
export {
  defaultContractsDir,
  loadContracts,
  SCHEMA_FILES,
  type Contracts,
  type FraudAlert,
  type FraudRule,
  type PaymentEvent,
  type PaymentEventType,
  type SchemaName,
  type SchemaTypes,
  type ValidationResult,
} from './contracts.js';
export { ConfigError, NonRetryableError, isNonRetryableError } from './errors.js';
export {
  createLogger,
  LOG_LEVELS,
  recordLogFields,
  type CreateLoggerOptions,
  type Logger,
  type LogLevel,
  type RecordLogFields,
} from './logger.js';
export {
  createProducer,
  type CreateProducerOptions,
  type ManagedProducer,
  type OutgoingRecord,
  type RecordProducer,
} from './producer.js';
export {
  backoffDelayMs,
  HEADERS,
  MAX_ERROR_MESSAGE_LENGTH,
  TOPICS,
  type DlqReason,
  type OriginalCoordinates,
} from './protocol.js';
export type { LibrdkafkaConfig } from './kafka.js';
