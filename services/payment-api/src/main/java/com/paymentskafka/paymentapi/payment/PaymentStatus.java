package com.paymentskafka.paymentapi.payment;

/**
 * Status of a payment and the event sequence a payment has once it reaches it
 * (docs/architecture.md, "Lifecycle and sequence").
 */
public enum PaymentStatus {
    CREATED(1),
    CONFIRMED(2),
    EXPIRED(2),
    REFUNDED(3);

    private final int sequence;

    PaymentStatus(int sequence) {
        this.sequence = sequence;
    }

    public int sequence() {
        return sequence;
    }
}
