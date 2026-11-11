package com.paymentskafka.paymentapi.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.paymentskafka.paymentapi.TestcontainersConfiguration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class PaymentServiceTest {

    @Autowired
    PaymentService service;

    @Autowired
    JdbcClient jdbc;

    @Test
    void createCommitsPaymentAndOutboxRowTogether() {
        String merchant = uniqueMerchant();

        Payment payment = service.create(merchant, 4590);

        Map<String, Object> row = jdbc.sql("SELECT * FROM payments WHERE id = :id")
                .param("id", payment.id()).query().singleRow();
        assertThat(row).containsEntry("merchant_id", merchant)
                .containsEntry("amount_cents", 4590L)
                .containsEntry("currency", "BRL")
                .containsEntry("status", "CREATED")
                .containsEntry("sequence", 1);

        List<Map<String, Object>> outbox = outboxRows(payment.id());
        assertThat(outbox).hasSize(1);
        assertThat(outbox.getFirst())
                .containsEntry("aggregate_type", "payment")
                .containsEntry("aggregate_id", payment.id().toString())
                .containsEntry("merchant_id", merchant)
                .containsEntry("event_type", "payment.created");
    }

    @Test
    void transitionsUpdatePaymentAndAppendOneOutboxRowEach() {
        Payment created = service.create(uniqueMerchant(), 1000);

        Payment confirmed = service.transition(created.id(), PaymentTransition.CONFIRM);
        Payment refunded = service.transition(created.id(), PaymentTransition.REFUND);

        assertThat(confirmed.status()).isEqualTo(PaymentStatus.CONFIRMED);
        assertThat(confirmed.sequence()).isEqualTo(2);
        assertThat(refunded.status()).isEqualTo(PaymentStatus.REFUNDED);
        assertThat(refunded.sequence()).isEqualTo(3);
        assertThat(service.get(created.id())).isEqualTo(refunded);
        assertThat(outboxRows(created.id()))
                .extracting(r -> r.get("event_type"))
                .containsExactly("payment.created", "payment.confirmed", "payment.refunded");
    }

    @Test
    void invalidTransitionChangesNothing() {
        Payment created = service.create(uniqueMerchant(), 1000);

        assertThatThrownBy(() -> service.transition(created.id(), PaymentTransition.REFUND))
                .isInstanceOf(InvalidTransitionException.class);

        assertThat(service.get(created.id())).isEqualTo(created);
        assertThat(outboxRows(created.id())).hasSize(1);
    }

    @Test
    void unknownPaymentIsReported() {
        UUID unknown = UUID.randomUUID();

        assertThatThrownBy(() -> service.transition(unknown, PaymentTransition.CONFIRM))
                .isInstanceOf(PaymentNotFoundException.class);
        assertThatThrownBy(() -> service.get(unknown)).isInstanceOf(PaymentNotFoundException.class);
    }

    private List<Map<String, Object>> outboxRows(UUID paymentId) {
        return jdbc.sql("SELECT * FROM outbox WHERE aggregate_id = :id ORDER BY (payload->>'sequence')::int")
                .param("id", paymentId.toString()).query().listOfRows();
    }

    static String uniqueMerchant() {
        return "merchant-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
