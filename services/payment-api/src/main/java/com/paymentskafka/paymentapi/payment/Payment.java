package com.paymentskafka.paymentapi.payment;

import java.time.Instant;
import java.util.UUID;

/** Current state of a payment, as stored in the {@code payments} table. */
public record Payment(
        UUID id,
        String merchantId,
        long amountCents,
        String currency,
        PaymentStatus status,
        int sequence,
        Instant createdAt,
        Instant updatedAt) {

    public static final String CURRENCY = "BRL";

    public static Payment create(UUID id, String merchantId, long amountCents, Instant now) {
        PaymentTransition creation = PaymentTransition.CREATE;
        return new Payment(id, merchantId, amountCents, CURRENCY, creation.applyTo(null),
                creation.resultingSequence(), now, now);
    }

    /** Applies a lifecycle transition, or throws {@link InvalidTransitionException}. */
    public Payment apply(PaymentTransition transition, Instant now) {
        PaymentStatus next = transition.applyTo(status);
        return new Payment(id, merchantId, amountCents, currency, next, transition.resultingSequence(), createdAt, now);
    }
}
