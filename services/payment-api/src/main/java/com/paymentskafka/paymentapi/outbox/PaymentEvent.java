package com.paymentskafka.paymentapi.outbox;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.paymentskafka.paymentapi.payment.Payment;
import com.paymentskafka.paymentapi.payment.PaymentTransition;
import java.time.Instant;
import java.util.UUID;

/** A payment event as defined by contracts/payment-event.v1.schema.json. */
public record PaymentEvent(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("event_type") String eventType,
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("payment_id") UUID paymentId,
        @JsonProperty("merchant_id") String merchantId,
        @JsonProperty("sequence") int sequence,
        @JsonProperty("amount_cents") long amountCents,
        @JsonProperty("currency") String currency) {

    public static final int SCHEMA_VERSION = 1;

    /** Builds the event describing {@code payment} right after {@code transition} was applied to it. */
    public static PaymentEvent of(UUID eventId, PaymentTransition transition, Payment payment) {
        return new PaymentEvent(
                eventId,
                transition.eventType(),
                SCHEMA_VERSION,
                payment.updatedAt(),
                payment.id(),
                payment.merchantId(),
                payment.sequence(),
                payment.amountCents(),
                payment.currency());
    }
}
