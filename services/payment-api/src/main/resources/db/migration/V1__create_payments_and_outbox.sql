-- Current state of each payment. Owned by payment-api.
CREATE TABLE payments (
    id           UUID         PRIMARY KEY,
    merchant_id  VARCHAR(64)  NOT NULL,
    amount_cents BIGINT       NOT NULL CHECK (amount_cents > 0),
    currency     VARCHAR(3)   NOT NULL,
    status       VARCHAR(16)  NOT NULL CHECK (status IN ('CREATED', 'CONFIRMED', 'EXPIRED', 'REFUNDED')),
    sequence     INTEGER      NOT NULL CHECK (sequence BETWEEN 1 AND 3),
    created_at   TIMESTAMPTZ  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL
);

-- Transactional outbox: the contract between payment-api and Debezium.
-- Must stay identical to the DDL in docs/architecture.md. Rows are never updated.
CREATE TABLE outbox (
    id             UUID         PRIMARY KEY,           -- equals the event_id
    aggregate_type VARCHAR(64)  NOT NULL,              -- always 'payment'
    aggregate_id   VARCHAR(64)  NOT NULL,              -- payment_id
    merchant_id    VARCHAR(64)  NOT NULL,              -- becomes the Kafka key
    event_type     VARCHAR(64)  NOT NULL,              -- e.g. 'payment.created'
    payload        JSONB        NOT NULL,              -- the full payment event
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
