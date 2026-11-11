import { isNonRetryableError, NonRetryableError } from '@payments-kafka/consumer-runtime';
import { describe, expect, it } from 'vitest';
import { assertHandled, OutOfOrderError } from '../../src/handler.js';

describe('assertHandled: apply results to runtime outcomes', () => {
  it('returns for an applied event (offset committed)', () => {
    expect(() => assertHandled({ outcome: 'applied', status: 'CONFIRMED', ledgerEntry: 'credit' })).not.toThrow();
  });

  it('returns for a duplicate (offset committed, nothing applied)', () => {
    expect(() => assertHandled({ outcome: 'duplicate' })).not.toThrow();
  });

  it('throws a retryable OutOfOrderError for a gap', () => {
    let thrown: unknown;
    try {
      assertHandled({ outcome: 'gap', reason: 'predecessor missing' });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(OutOfOrderError);
    expect(thrown).not.toBeInstanceOf(NonRetryableError);
    // The runtime treats every error that is not a NonRetryableError as retryable.
    expect(isNonRetryableError(thrown)).toBe(false);
    expect((thrown as Error).message).toContain('predecessor missing');
  });

  it('throws a NonRetryableError for a conflict (straight to the DLQ)', () => {
    let thrown: unknown;
    try {
      assertHandled({ outcome: 'conflict', reason: 'expired after confirmed' });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(NonRetryableError);
    expect(isNonRetryableError(thrown)).toBe(true);
    expect((thrown as Error).message).toContain('expired after confirmed');
  });
});
