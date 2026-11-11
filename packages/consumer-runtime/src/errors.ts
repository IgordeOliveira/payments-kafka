/**
 * Throw from a handler when retrying cannot help (for example a business rule
 * rejects the event). The record goes straight to the DLQ with
 * `x-dlq-reason: non_retryable`, without passing through the retry topic.
 *
 * Any other error thrown by a handler is treated as retryable.
 */
export class NonRetryableError extends Error {
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'NonRetryableError';
  }
}

/** Raised at startup when an environment variable has an invalid value. */
export class ConfigError extends Error {
  /** Name of the offending environment variable. */
  readonly variable: string;

  constructor(variable: string, message: string) {
    super(message);
    this.name = 'ConfigError';
    this.variable = variable;
  }
}

/** True when `error` is a {@link NonRetryableError}, also across module copies. */
export function isNonRetryableError(error: unknown): boolean {
  return (
    error instanceof NonRetryableError ||
    (error instanceof Error && error.name === 'NonRetryableError')
  );
}

/** Best-effort message extraction for anything a handler may throw. */
export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message || error.name;
  if (typeof error === 'string') return error;
  try {
    return JSON.stringify(error) ?? String(error);
  } catch {
    return String(error);
  }
}
