/**
 * Topic names, header names and the pure parts of the retry/DLQ protocol
 * defined in docs/architecture.md ("Retry and DLQ protocol").
 */

export const TOPICS = {
  payments: 'pix.payments.v1',
  fraudAlerts: 'pix.fraud-alerts.v1',
  retry: 'pix.payments.retry.v1',
  dlq: 'pix.payments.dlq.v1',
} as const;

export const HEADERS = {
  retryGroup: 'x-retry-group',
  retryAttempt: 'x-retry-attempt',
  retryNotBefore: 'x-retry-not-before',
  originalTopic: 'x-original-topic',
  originalPartition: 'x-original-partition',
  originalOffset: 'x-original-offset',
  errorMessage: 'x-error-message',
  dlqGroup: 'x-dlq-group',
  dlqReason: 'x-dlq-reason',
  dlqAttempts: 'x-dlq-attempts',
} as const;

/**
 * Why a record was dead-lettered (`x-dlq-reason`):
 * - `invalid_message`: not JSON, or violates its schema (never reaches the handler);
 * - `non_retryable`: the handler threw a `NonRetryableError`;
 * - `retries_exhausted`: the handler kept failing after `RETRY_MAX_ATTEMPTS` retries.
 */
export type DlqReason = 'invalid_message' | 'non_retryable' | 'retries_exhausted';

/** `x-error-message` is truncated to this many characters. */
export const MAX_ERROR_MESSAGE_LENGTH = 1000;

/** Where a record first came from (the `x-original-*` headers). */
export interface OriginalCoordinates {
  topic: string;
  partition: number;
  offset: string;
}

/**
 * Delay before retry number `attempt` (1-based):
 * `RETRY_BACKOFF_MS * 2^(attempt-1)`.
 */
export function backoffDelayMs(baseMs: number, attempt: number): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError(`attempt must be an integer >= 1, got ${attempt}`);
  }
  return baseMs * 2 ** (attempt - 1);
}

export function truncateErrorMessage(message: string): string {
  return message.length <= MAX_ERROR_MESSAGE_LENGTH
    ? message
    : `${message.slice(0, MAX_ERROR_MESSAGE_LENGTH - 3)}...`;
}

/** Headers of a record published to the retry topic. */
export function retryHeaders(args: {
  group: string;
  attempt: number;
  notBefore: number;
  original: OriginalCoordinates;
  error: string;
}): Record<string, string> {
  return {
    [HEADERS.retryGroup]: args.group,
    [HEADERS.retryAttempt]: String(args.attempt),
    [HEADERS.retryNotBefore]: String(args.notBefore),
    ...originalHeaders(args.original),
    [HEADERS.errorMessage]: truncateErrorMessage(args.error),
  };
}

/** Headers of a record published to the DLQ. */
export function dlqHeaders(args: {
  group: string;
  reason: DlqReason;
  attempts: number;
  original: OriginalCoordinates;
  error: string;
}): Record<string, string> {
  return {
    ...originalHeaders(args.original),
    [HEADERS.errorMessage]: truncateErrorMessage(args.error),
    [HEADERS.dlqGroup]: args.group,
    [HEADERS.dlqReason]: args.reason,
    [HEADERS.dlqAttempts]: String(args.attempts),
  };
}

function originalHeaders(original: OriginalCoordinates): Record<string, string> {
  return {
    [HEADERS.originalTopic]: original.topic,
    [HEADERS.originalPartition]: String(original.partition),
    [HEADERS.originalOffset]: original.offset,
  };
}

export interface RetryMetadata {
  group: string;
  attempt: number;
  notBefore: number;
  original: OriginalCoordinates;
}

/**
 * Reads the retry headers of a record consumed from the retry topic.
 * Returns `{ group }` alone when the record is owned by a group (so foreign
 * records can be skipped cheaply), or an error when the headers are malformed.
 */
export function parseRetryHeaders(
  headers: Readonly<Record<string, string>>,
): { ok: true; meta: RetryMetadata } | { ok: false; group: string | undefined; error: string } {
  const group = headers[HEADERS.retryGroup];
  const attempt = parseNonNegativeInt(headers[HEADERS.retryAttempt]);
  const notBefore = parseNonNegativeInt(headers[HEADERS.retryNotBefore]);
  const topic = headers[HEADERS.originalTopic];
  const partition = parseNonNegativeInt(headers[HEADERS.originalPartition]);
  const offset = headers[HEADERS.originalOffset];

  const problems: string[] = [];
  if (!group) problems.push(HEADERS.retryGroup);
  if (attempt === undefined || attempt < 1) problems.push(HEADERS.retryAttempt);
  if (notBefore === undefined) problems.push(HEADERS.retryNotBefore);
  if (!topic) problems.push(HEADERS.originalTopic);
  if (partition === undefined) problems.push(HEADERS.originalPartition);
  if (offset === undefined || parseNonNegativeInt(offset) === undefined) {
    problems.push(HEADERS.originalOffset);
  }
  if (problems.length > 0 || !group || !topic || offset === undefined) {
    return { ok: false, group, error: `missing or invalid retry headers: ${problems.join(', ')}` };
  }
  return {
    ok: true,
    meta: {
      group,
      attempt: attempt!,
      notBefore: notBefore!,
      original: { topic, partition: partition!, offset },
    },
  };
}

function parseNonNegativeInt(value: string | undefined): number | undefined {
  if (value === undefined || !/^\d+$/.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

/** Kafka header values arrive as Buffers (possibly repeated); flatten to strings, last one wins. */
export function decodeHeaders(
  headers: Readonly<Record<string, unknown>> | undefined,
): Record<string, string> {
  const result: Record<string, string> = {};
  if (!headers) return result;
  for (const [name, value] of Object.entries(headers)) {
    const last = Array.isArray(value) ? value[value.length - 1] : value;
    if (last === undefined || last === null) continue;
    result[name] = Buffer.isBuffer(last) ? last.toString('utf8') : String(last);
  }
  return result;
}
