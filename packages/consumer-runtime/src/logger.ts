import { pino, type DestinationStream, type Logger } from 'pino';

export type { Logger } from 'pino';

/** Accepted values of `LOG_LEVEL`. `silent` disables logging. */
export const LOG_LEVELS = ['trace', 'debug', 'info', 'warn', 'error', 'fatal', 'silent'] as const;
export type LogLevel = (typeof LOG_LEVELS)[number];

export interface CreateLoggerOptions {
  /** Minimum level to emit. Defaults to `info`. */
  level?: LogLevel;
  /** Fields added to every line, e.g. `{ service: 'reconciliation' }`. */
  bindings?: Record<string, unknown>;
  /** Where lines are written. Defaults to stdout, synchronously. */
  destination?: DestinationStream;
}

/**
 * Creates the JSON logger every consumer uses: one JSON object per line on
 * stdout, with `level` as a string label, `time` as an ISO-8601 string and
 * the message under `msg`.
 *
 * ```json
 * {"level":"info","time":"2026-11-11T14:03:27.512Z","service":"reconciliation","group":"reconciliation","msg":"partitions assigned","partitions":[...]}
 * ```
 *
 * Writes are synchronous so that lines are never lost when a container is
 * stopped; experiments count log lines.
 */
export function createLogger(options: CreateLoggerOptions = {}): Logger {
  const destination = options.destination ?? pino.destination({ dest: 1, sync: true });
  return pino(
    {
      level: options.level ?? 'info',
      base: { ...options.bindings },
      messageKey: 'msg',
      timestamp: pino.stdTimeFunctions.isoTime,
      formatters: {
        level: (label) => ({ level: label }),
      },
    },
    destination,
  );
}

/**
 * The standard fields attached to every log line about a record:
 * `topic`, `partition`, `offset`, `key`, `attempt` and, when the value has
 * them, `event_id` (payment events) or `alert_id` (fraud alerts).
 */
export interface RecordLogFields {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  attempt: number;
  event_id?: string;
  alert_id?: string;
}

export function recordLogFields(record: {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  attempt: number;
  value?: unknown;
}): RecordLogFields {
  const fields: RecordLogFields = {
    topic: record.topic,
    partition: record.partition,
    offset: record.offset,
    key: record.key,
    attempt: record.attempt,
  };
  const value = record.value;
  if (value !== null && typeof value === 'object') {
    const v = value as Record<string, unknown>;
    if (typeof v['event_id'] === 'string') fields.event_id = v['event_id'];
    if (typeof v['alert_id'] === 'string') fields.alert_id = v['alert_id'];
  }
  return fields;
}
