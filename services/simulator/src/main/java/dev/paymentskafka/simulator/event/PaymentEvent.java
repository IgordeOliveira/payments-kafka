package dev.paymentskafka.simulator.event;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * One step of a payment lifecycle, shaped after
 * {@code contracts/payment-event.v1.schema.json}.
 */
public record PaymentEvent(
        UUID eventId,
        EventType type,
        Instant occurredAt,
        UUID paymentId,
        String merchantId,
        long amountCents) {

    public static final int SCHEMA_VERSION = 1;
    public static final String CURRENCY = "BRL";

    private static final DateTimeFormatter OCCURRED_AT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    public int sequence() {
        return type.sequence();
    }

    /** The event as the JSON object of the contract (field order as in the schema). */
    public Map<String, Object> toContractMap() {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("event_id", eventId.toString());
        json.put("event_type", type.wireName());
        json.put("schema_version", SCHEMA_VERSION);
        json.put("occurred_at", OCCURRED_AT.format(occurredAt));
        json.put("payment_id", paymentId.toString());
        json.put("merchant_id", merchantId);
        json.put("sequence", sequence());
        json.put("amount_cents", amountCents);
        json.put("currency", CURRENCY);
        return json;
    }
}
