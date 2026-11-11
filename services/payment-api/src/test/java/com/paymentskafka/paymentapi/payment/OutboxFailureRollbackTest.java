package com.paymentskafka.paymentapi.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import com.paymentskafka.paymentapi.TestcontainersConfiguration;
import com.paymentskafka.paymentapi.outbox.OutboxRepository;
import com.paymentskafka.paymentapi.outbox.PaymentEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/** Forces the outbox insert to fail and checks the payment change is rolled back with it. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class OutboxFailureRollbackTest {

    @MockitoSpyBean
    OutboxRepository outbox;

    @Autowired
    PaymentService service;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcClient jdbc;

    @AfterEach
    void restoreOutbox() {
        reset(outbox);
    }

    @Test
    void failedOutboxInsertRollsBackCreation() {
        String merchant = PaymentServiceTest.uniqueMerchant();
        failOutboxInserts();

        assertThatThrownBy(() -> service.create(merchant, 4590))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(countPayments(merchant)).isZero();
        assertThat(countOutbox(merchant)).isZero();
    }

    @Test
    void failedOutboxInsertRollsBackTransition() {
        Payment created = service.create(PaymentServiceTest.uniqueMerchant(), 4590);
        failOutboxInserts();

        assertThatThrownBy(() -> service.transition(created.id(), PaymentTransition.CONFIRM))
                .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(service.get(created.id())).isEqualTo(created);
        assertThat(countOutbox(created.merchantId())).isEqualTo(1);
    }

    @Test
    void apiReturnsServerErrorWhenOutboxInsertFails() {
        String merchant = PaymentServiceTest.uniqueMerchant();
        failOutboxInserts();

        ResponseEntity<String> response = http.postForEntity("/payments",
                java.util.Map.of("merchant_id", merchant, "amount_cents", 4590), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(countPayments(merchant)).isZero();
        assertThat(countOutbox(merchant)).isZero();
    }

    private void failOutboxInserts() {
        doThrow(new DataIntegrityViolationException("forced outbox failure"))
                .when(outbox).insert(any(PaymentEvent.class));
    }

    private long countPayments(String merchant) {
        return jdbc.sql("SELECT count(*) FROM payments WHERE merchant_id = :m").param("m", merchant)
                .query(Long.class).single();
    }

    private long countOutbox(String merchant) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE merchant_id = :m").param("m", merchant)
                .query(Long.class).single();
    }
}
