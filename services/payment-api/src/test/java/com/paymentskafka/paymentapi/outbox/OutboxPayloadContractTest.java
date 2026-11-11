package com.paymentskafka.paymentapi.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentskafka.paymentapi.TestcontainersConfiguration;
import com.paymentskafka.paymentapi.payment.Payment;
import com.paymentskafka.paymentapi.payment.PaymentService;
import com.paymentskafka.paymentapi.payment.PaymentTransition;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

/** The payload Debezium will publish must validate against contracts/payment-event.v1.schema.json. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OutboxPayloadContractTest {

    record OutboxRow(UUID id, String aggregateType, String aggregateId, String merchantId, String eventType,
            JsonNode payload) {
    }

    @Autowired
    PaymentService service;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void createdConfirmedRefundedPayloadsValidate() {
        Payment payment = service.create("merchant-007", 4590);
        service.transition(payment.id(), PaymentTransition.CONFIRM);
        service.transition(payment.id(), PaymentTransition.REFUND);

        List<OutboxRow> rows = outboxRows(payment.id());

        assertThat(rows).extracting(OutboxRow::eventType)
                .containsExactly("payment.created", "payment.confirmed", "payment.refunded");
        assertThat(rows).extracting(r -> r.payload().get("sequence").asInt()).containsExactly(1, 2, 3);
        assertThat(rows).extracting(OutboxRow::id).doesNotHaveDuplicates();
        rows.forEach(row -> assertRowMatchesContract(row, payment));
    }

    @Test
    void expiredPayloadValidates() {
        Payment payment = service.create("merchant-042", 990000);
        service.transition(payment.id(), PaymentTransition.EXPIRE);

        List<OutboxRow> rows = outboxRows(payment.id());

        assertThat(rows).extracting(OutboxRow::eventType).containsExactly("payment.created", "payment.expired");
        assertThat(rows).extracting(r -> r.payload().get("sequence").asInt()).containsExactly(1, 2);
        rows.forEach(row -> assertRowMatchesContract(row, payment));
    }

    @Test
    void validatorIsStrictAboutTheSharedInvalidExamples() throws Exception {
        JsonNode invalid = objectMapper.readTree(
                Files.readString(PaymentEventContract.CONTRACTS_DIR.resolve("examples/payment-events.invalid.json")));
        JsonNode valid = objectMapper.readTree(
                Files.readString(PaymentEventContract.CONTRACTS_DIR.resolve("examples/payment-events.valid.json")));

        assertThat(invalid).isNotEmpty();
        invalid.forEach(example -> assertThat(PaymentEventContract.validate(example.get("event")))
                .as("expected rejection: %s", example.get("reason").asText())
                .isNotEmpty());
        valid.forEach(example -> assertThat(PaymentEventContract.validate(example)).isEmpty());
    }

    private void assertRowMatchesContract(OutboxRow row, Payment payment) {
        JsonNode payload = row.payload();
        assertThat(PaymentEventContract.validate(payload)).as("schema errors for %s", row.eventType()).isEmpty();
        assertThat(payload.get("event_id").asText()).isEqualTo(row.id().toString());
        assertThat(payload.get("event_type").asText()).isEqualTo(row.eventType());
        assertThat(payload.get("payment_id").asText()).isEqualTo(payment.id().toString());
        assertThat(payload.get("merchant_id").asText()).isEqualTo(payment.merchantId());
        assertThat(payload.get("amount_cents").asLong()).isEqualTo(payment.amountCents());
        assertThat(row.aggregateType()).isEqualTo("payment");
        assertThat(row.aggregateId()).isEqualTo(payment.id().toString());
        assertThat(row.merchantId()).isEqualTo(payment.merchantId());
    }

    private List<OutboxRow> outboxRows(UUID paymentId) {
        return jdbc.sql("""
                        SELECT id, aggregate_type, aggregate_id, merchant_id, event_type, payload::text AS payload
                        FROM outbox WHERE aggregate_id = :id ORDER BY (payload->>'sequence')::int
                        """)
                .param("id", paymentId.toString())
                .query((rs, n) -> new OutboxRow(
                        rs.getObject("id", UUID.class),
                        rs.getString("aggregate_type"),
                        rs.getString("aggregate_id"),
                        rs.getString("merchant_id"),
                        rs.getString("event_type"),
                        readJson(rs.getString("payload"))))
                .list();
    }

    private JsonNode readJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
