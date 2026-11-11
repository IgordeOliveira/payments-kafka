import type { ConsumedMessage, HandlerContext } from '@payments-kafka/consumer-runtime';
import type { ClaimFn } from './dedupe.js';
import { fraudAlertNotification, paymentNotification, type Notification, type Notifier } from './notifier.js';

export type NotificationMessage = ConsumedMessage<'payment-event' | 'fraud-alert'>;

/** The notification a record should produce, or `null` if it notifies nothing. */
export function notificationFor(message: NotificationMessage): Notification | null {
  // `schema` is fixed by the source topic (pix.payments.v1 → payment-event,
  // pix.fraud-alerts.v1 → fraud-alert) and is kept on retries, so this is the
  // dispatch by topic, independent of the topic names used in tests.
  return message.schema === 'fraud-alert'
    ? fraudAlertNotification(message.value)
    : paymentNotification(message.value);
}

/**
 * Claim, then send. The claim is committed before the send, so a key is
 * notified at most once: a duplicate, redelivered or retried record finds the
 * key taken and emits nothing. A crash (or a failed send) after the claim
 * loses that notification: the accepted trade-off (see README).
 *
 * A store failure rejects before anything is sent, so the runtime's retry is
 * safe. A send failure also rejects (the record goes to the retry topic), and
 * the retry then finds the key claimed and emits nothing.
 */
export function createNotificationHandler(deps: { claim: ClaimFn; notifier: Notifier }) {
  return async (message: NotificationMessage, ctx: HandlerContext): Promise<void> => {
    const notification = notificationFor(message);
    if (!notification) {
      ctx.logger.debug('payment event does not notify; skipped');
      return;
    }
    const fields = { notification_key: notification.notification_key, kind: notification.kind };
    const claimed = await deps.claim({
      notificationKey: notification.notification_key,
      kind: notification.kind,
      merchantId: notification.merchant_id,
    });
    if (!claimed) {
      ctx.logger.info(fields, 'notification already claimed; not sent again');
      return;
    }
    await deps.notifier.send(notification);
    ctx.logger.info(fields, 'notification sent');
  };
}
