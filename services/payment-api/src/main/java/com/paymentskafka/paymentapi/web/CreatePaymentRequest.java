package com.paymentskafka.paymentapi.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /payments}. Non-integer amounts (for example {@code 45.5}
 * or {@code "4590"}) are rejected by Jackson before validation (see
 * {@code spring.jackson.*} in application.yaml).
 */
public record CreatePaymentRequest(
        @JsonProperty("merchant_id") @NotBlank @Size(max = 64) String merchantId,
        @JsonProperty("amount_cents") @NotNull @Positive Long amountCents) {
}
