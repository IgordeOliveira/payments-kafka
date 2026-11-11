-- Reconciliation state. See services/reconciliation/README.md for the data model.

-- One row per applied event, written in the same transaction as its effects.
CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Current state of every payment.
CREATE TABLE payments (
    payment_id    UUID        PRIMARY KEY,
    merchant_id   VARCHAR(64) NOT NULL,
    amount_cents  BIGINT      NOT NULL CHECK (amount_cents > 0),
    status        VARCHAR(16) NOT NULL CHECK (status IN ('CREATED', 'CONFIRMED', 'EXPIRED', 'REFUNDED')),
    last_sequence SMALLINT    NOT NULL CHECK (last_sequence BETWEEN 1 AND 3),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per balance movement: a credit per confirmation, a debit per refund.
CREATE TABLE ledger_entries (
    event_id     UUID        PRIMARY KEY,
    merchant_id  VARCHAR(64) NOT NULL,
    payment_id   UUID        NOT NULL,
    entry_type   VARCHAR(8)  NOT NULL CHECK (entry_type IN ('credit', 'debit')),
    amount_cents BIGINT      NOT NULL CHECK (amount_cents > 0),
    occurred_at  TIMESTAMPTZ NOT NULL,
    -- A payment is credited at most once and debited at most once.
    UNIQUE (payment_id, entry_type)
);

CREATE INDEX ledger_entries_merchant_id_idx ON ledger_entries (merchant_id);

-- Balance and counters of every merchant that had a balance movement.
CREATE TABLE merchant_balances (
    merchant_id     VARCHAR(64) PRIMARY KEY,
    balance_cents   BIGINT      NOT NULL,
    confirmed_count BIGINT      NOT NULL,
    refunded_count  BIGINT      NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
