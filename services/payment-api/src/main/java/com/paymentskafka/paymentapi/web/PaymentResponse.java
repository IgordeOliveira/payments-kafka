package com.paymentskafka.paymentapi.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.paymentskafka.paymentapi.payment.Payment;
import java.time.Instant;
import java.util.UUID;

/** Payment representation from docs/architecture.md ("Payment API"). */
public record PaymentResponse(
        @JsonProperty("payment_id") UUID paymentId,
        @JsonProperty("merchant_id") String merchantId,
        @JsonProperty("amount_cents") long amountCents,
        @JsonProperty("currency") String currency,
        @JsonProperty("status") String status,
        @JsonProperty("sequence") int sequence,
        @JsonProperty("created_at") Instant createdAt,
        @JsonProperty("updated_at") Instant updatedAt) {

    static PaymentResponse from(Payment payment) {
        return new PaymentResponse(
                payment.id(),
                payment.merchantId(),
                payment.amountCents(),
                payment.currency(),
                payment.status().name(),
                payment.sequence(),
                payment.createdAt(),
                payment.updatedAt());
    }
}
