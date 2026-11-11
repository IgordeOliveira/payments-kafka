import { writeSync } from 'node:fs';
import type { FraudAlert, PaymentEvent } from '@payments-kafka/consumer-runtime';

/** Payment event types that notify the merchant. `payment.created` does not. */
export const NOTIFIED_EVENT_TYPES = ['payment.confirmed', 'payment.expired', 'payment.refunded'] as const;
export type NotifiedEventType = (typeof NOTIFIED_EVENT_TYPES)[number];
export type NotificationKind = NotifiedEventType | 'fraud_alert';

/**
 * One notification. Written to stdout as one JSON line whose `type` is
 * `notification`: experiments count those lines, and no other line the
 * service writes carries that `type`.
 */
export interface Notification {
  type: 'notification';
  kind: NotificationKind;
  merchant_id: string;
  /** Dedupe key: the payment `event_id` or the fraud `alert_id`. */
  notification_key: string;
  payment_id: string;
  /** Fraud rule, only for `fraud_alert`. */
  rule?: string;
  /** Human-readable text for the merchant. */
  message: string;
}

export function isNotifiedEventType(type: string): type is NotifiedEventType {
  return (NOTIFIED_EVENT_TYPES as readonly string[]).includes(type);
}

function formatAmount(event: PaymentEvent): string {
  return `${event.currency} ${(event.amount_cents / 100).toFixed(2)}`;
}

const PAYMENT_MESSAGES: Record<NotifiedEventType, (paymentId: string, amount: string) => string> = {
  'payment.confirmed': (id, amount) => `Payment ${id} of ${amount} was confirmed.`,
  'payment.expired': (id, amount) => `Payment ${id} of ${amount} expired without being paid.`,
  'payment.refunded': (id, amount) => `Payment ${id} of ${amount} was refunded.`,
};

/** The notification for a payment event, or `null` for event types that do not notify. */
export function paymentNotification(event: PaymentEvent): Notification | null {
  const eventType = event.event_type;
  if (!isNotifiedEventType(eventType)) return null;
  return {
    type: 'notification',
    kind: eventType,
    merchant_id: event.merchant_id,
    notification_key: event.event_id,
    payment_id: event.payment_id,
    message: PAYMENT_MESSAGES[eventType](event.payment_id, formatAmount(event)),
  };
}

/** The notification for a fraud alert (every alert notifies). */
export function fraudAlertNotification(alert: FraudAlert): Notification {
  return {
    type: 'notification',
    kind: 'fraud_alert',
    merchant_id: alert.merchant_id,
    notification_key: alert.alert_id,
    payment_id: alert.payment_id,
    rule: alert.rule,
    message: `Fraud alert "${alert.rule}" raised for payment ${alert.payment_id}.`,
  };
}

/** The notification channel. Throwing (or rejecting) means the notification was not delivered. */
export interface Notifier {
  send(notification: Notification): void | Promise<void>;
}

/** Writes a whole line to stdout synchronously, so no line is lost when the container stops. */
export function writeStdoutLine(line: string): void {
  let buffer = Buffer.from(`${line}\n`, 'utf8');
  while (buffer.length > 0) {
    try {
      buffer = buffer.subarray(writeSync(1, buffer));
    } catch (error) {
      // Non-blocking stdout (a pipe) momentarily full: try again.
      if ((error as NodeJS.ErrnoException).code !== 'EAGAIN') throw error;
    }
  }
}

/**
 * The console channel: one JSON line per notification, `type` first, plus
 * `time` (and `service`) like the service's log lines, e.g.
 *
 * ```json
 * {"type":"notification","time":"...","service":"notification","kind":"payment.confirmed","merchant_id":"merchant-007","notification_key":"5f0c...","payment_id":"0b9d...","message":"Payment 0b9d... of BRL 45.90 was confirmed."}
 * ```
 */
export function createConsoleNotifier(
  options: { write?: (line: string) => void; service?: string; now?: () => Date } = {},
): Notifier {
  const write = options.write ?? writeStdoutLine;
  const now = options.now ?? (() => new Date());
  return {
    send(notification) {
      const { type, ...fields } = notification;
      write(
        JSON.stringify({
          type,
          time: now().toISOString(),
          ...(options.service ? { service: options.service } : {}),
          ...fields,
        }),
      );
    },
  };
}
