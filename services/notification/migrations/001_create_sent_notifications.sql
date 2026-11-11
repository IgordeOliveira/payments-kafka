-- Dedupe keys of notifications already claimed: sent, or lost if the process
-- died between the claim and the send (claim-then-send, see README).
-- notification_key is the payment event_id or the fraud alert_id; the primary
-- key is the concurrency control between instances racing on the same key.
CREATE TABLE IF NOT EXISTS sent_notifications (
    notification_key TEXT        PRIMARY KEY,
    kind             TEXT        NOT NULL,
    merchant_id      TEXT        NOT NULL,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
