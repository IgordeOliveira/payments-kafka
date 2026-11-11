import type pg from 'pg';
import type { NotificationKind } from './notifier.js';

export interface NotificationClaim {
  /** Payment `event_id` or fraud `alert_id`. */
  notificationKey: string;
  kind: NotificationKind;
  merchantId: string;
}

/**
 * Records that a notification is about to be sent. Resolves `true` if this
 * call inserted the key (the caller now owns the send), `false` if the key
 * was already there (already sent, or claimed by another instance). Rejects
 * if the store is unavailable.
 */
export type ClaimFn = (claim: NotificationClaim) => Promise<boolean>;

/**
 * The claim against `sent_notifications`. The statement runs outside any
 * explicit transaction (autocommit), so it is committed when the promise
 * resolves, before the notification is sent. The primary key decides
 * between racing claims: exactly one inserts the row.
 */
export function createClaim(pool: pg.Pool): ClaimFn {
  return async ({ notificationKey, kind, merchantId }) => {
    const result = await pool.query(
      `INSERT INTO sent_notifications (notification_key, kind, merchant_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (notification_key) DO NOTHING`,
      [notificationKey, kind, merchantId],
    );
    return result.rowCount === 1;
  };
}
