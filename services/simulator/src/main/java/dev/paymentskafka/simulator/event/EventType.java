package dev.paymentskafka.simulator.event;

/** Lifecycle steps of a payment, with their wire name and fixed {@code sequence}. */
public enum EventType {
    CREATED("payment.created", 1),
    CONFIRMED("payment.confirmed", 2),
    EXPIRED("payment.expired", 2),
    REFUNDED("payment.refunded", 3);

    private final String wireName;
    private final int sequence;

    EventType(String wireName, int sequence) {
        this.wireName = wireName;
        this.sequence = sequence;
    }

    public String wireName() {
        return wireName;
    }

    public int sequence() {
        return sequence;
    }
}
