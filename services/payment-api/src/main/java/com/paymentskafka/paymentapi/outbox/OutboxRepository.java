package com.paymentskafka.paymentapi.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Writes outbox rows. Debezium turns each inserted row into one record on
 * {@code pix.payments.v1} (key {@code merchant_id}, value {@code payload}).
 * Rows are never updated or deleted.
 */
@Repository
public class OutboxRepository {

    public static final String AGGREGATE_TYPE = "payment";

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public OutboxRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public void insert(PaymentEvent event) {
        jdbc.sql("""
                        INSERT INTO outbox (id, aggregate_type, aggregate_id, merchant_id, event_type, payload, created_at)
                        VALUES (:id, :aggregateType, :aggregateId, :merchantId, :eventType, CAST(:payload AS jsonb), :createdAt)
                        """)
                .param("id", event.eventId())
                .param("aggregateType", AGGREGATE_TYPE)
                .param("aggregateId", event.paymentId().toString())
                .param("merchantId", event.merchantId())
                .param("eventType", event.eventType())
                .param("payload", toJson(event))
                .param("createdAt", Timestamp.from(event.occurredAt()))
                .update();
    }

    private String toJson(PaymentEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize payment event " + event.eventId(), e);
        }
    }
}
