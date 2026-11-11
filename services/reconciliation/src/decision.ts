/**
 * The sequence and transition rules of reconciliation, as a pure function.
 *
 * Lifecycle (docs/architecture.md): created (1) → confirmed (2) | expired (2),
 * confirmed (2) → refunded (3). The decision compares the event's `sequence`
 * with the payment's last applied sequence (0 when the payment is unknown).
 */
import type { PaymentEvent, PaymentEventType } from '@payments-kafka/consumer-runtime';

export type PaymentStatus = 'CREATED' | 'CONFIRMED' | 'EXPIRED' | 'REFUNDED';

/** Stored state of a payment, or `null` when no event of it was applied yet. */
export interface PaymentState {
  merchantId: string;
  amountCents: number;
  status: PaymentStatus;
  lastSequence: number;
}

export type LedgerEntryType = 'credit' | 'debit';

export type Decision =
  /** Apply the event: set the payment status and, for money events, write a ledger entry. */
  | { kind: 'apply'; status: PaymentStatus; ledgerEntry: LedgerEntryType | null }
  /** The event_id was already applied: nothing to do. */
  | { kind: 'duplicate' }
  /** A predecessor has not been applied yet: retry later. */
  | { kind: 'gap'; reason: string }
  /** The event can never be applied: dead-letter it. */
  | { kind: 'conflict'; reason: string };

interface Step {
  status: PaymentStatus;
  /** Status the payment must be in; `null` means the payment must not exist yet. */
  from: PaymentStatus | null;
  ledgerEntry: LedgerEntryType | null;
}

const STEPS: Readonly<Record<PaymentEventType, Step>> = {
  'payment.created': { status: 'CREATED', from: null, ledgerEntry: null },
  'payment.confirmed': { status: 'CONFIRMED', from: 'CREATED', ledgerEntry: 'credit' },
  'payment.expired': { status: 'EXPIRED', from: 'CREATED', ledgerEntry: null },
  'payment.refunded': { status: 'REFUNDED', from: 'CONFIRMED', ledgerEntry: 'debit' },
};

type DecisionInput = Pick<PaymentEvent, 'event_type' | 'sequence' | 'merchant_id' | 'amount_cents'>;

/**
 * Decides what to do with `event` given the payment's current `state` and
 * whether its `event_id` was already applied.
 *
 * - already applied → duplicate;
 * - `sequence > last + 1` → gap (the predecessor has not arrived yet);
 * - `sequence <= last` → conflict (a different event for a step already taken);
 * - `sequence == last + 1` → apply if the transition is allowed and the event
 *   agrees with the stored merchant and amount, otherwise conflict.
 */
export function decide(event: DecisionInput, state: PaymentState | null, alreadyProcessed: boolean): Decision {
  if (alreadyProcessed) return { kind: 'duplicate' };

  const step = STEPS[event.event_type];
  const last = state?.lastSequence ?? 0;

  if (event.sequence > last + 1) {
    return {
      kind: 'gap',
      reason: `${event.event_type} has sequence ${event.sequence} but the payment's last applied sequence is ${last}; waiting for the predecessor`,
    };
  }
  if (event.sequence <= last) {
    return {
      kind: 'conflict',
      reason: `${event.event_type} has sequence ${event.sequence} but the payment is already ${state?.status} at sequence ${last}`,
    };
  }
  const current = state?.status ?? null;
  if (current !== step.from) {
    return {
      kind: 'conflict',
      reason: `${event.event_type} is not allowed from ${current ?? 'no payment'} (expected ${step.from ?? 'no payment'})`,
    };
  }
  if (state && state.merchantId !== event.merchant_id) {
    return {
      kind: 'conflict',
      reason: `${event.event_type} names merchant ${event.merchant_id} but the payment belongs to ${state.merchantId}`,
    };
  }
  if (state && state.amountCents !== event.amount_cents) {
    return {
      kind: 'conflict',
      reason: `${event.event_type} has amount ${event.amount_cents} but the payment amount is ${state.amountCents}`,
    };
  }
  return { kind: 'apply', status: step.status, ledgerEntry: step.ledgerEntry };
}
