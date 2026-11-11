package com.paymentskafka.paymentapi.payment;

/**
 * The lifecycle rules in one place: which status each transition is allowed
 * from, which status it leads to and which event it produces. Creation is
 * modelled as a transition from no status at all.
 */
public enum PaymentTransition {
    CREATE(null, PaymentStatus.CREATED, "payment.created"),
    CONFIRM(PaymentStatus.CREATED, PaymentStatus.CONFIRMED, "payment.confirmed"),
    EXPIRE(PaymentStatus.CREATED, PaymentStatus.EXPIRED, "payment.expired"),
    REFUND(PaymentStatus.CONFIRMED, PaymentStatus.REFUNDED, "payment.refunded");

    private final PaymentStatus from;
    private final PaymentStatus to;
    private final String eventType;

    PaymentTransition(PaymentStatus from, PaymentStatus to, String eventType) {
        this.from = from;
        this.to = to;
        this.eventType = eventType;
    }

    /** Status required before the transition; {@code null} for {@link #CREATE}. */
    public PaymentStatus from() {
        return from;
    }

    public PaymentStatus to() {
        return to;
    }

    public String eventType() {
        return eventType;
    }

    /** Sequence carried by the event this transition produces. */
    public int resultingSequence() {
        return to.sequence();
    }

    public boolean isAllowedFrom(PaymentStatus current) {
        return from == current;
    }

    /**
     * Returns the status reached by applying this transition to {@code current}.
     *
     * @throws InvalidTransitionException when the transition is not allowed
     */
    public PaymentStatus applyTo(PaymentStatus current) {
        if (!isAllowedFrom(current)) {
            throw new InvalidTransitionException(this, current);
        }
        return to;
    }
}
