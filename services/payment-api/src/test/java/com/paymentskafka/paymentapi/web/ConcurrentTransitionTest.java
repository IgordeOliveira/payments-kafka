package com.paymentskafka.paymentapi.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.paymentskafka.paymentapi.TestcontainersConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Simultaneous confirms over real HTTP: the row lock lets exactly one win. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class ConcurrentTransitionTest {

    private static final int CONCURRENT_REQUESTS = 8;

    @Autowired
    TestRestTemplate http;

    @Autowired
    JdbcClient jdbc;

    @RepeatedTest(5)
    void simultaneousConfirmsSucceedExactlyOnce() throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> created = http.postForObject("/payments",
                Map.of("merchant_id", "merchant-007", "amount_cents", 4590), Map.class);
        String id = (String) created.get("payment_id");

        CountDownLatch start = new CountDownLatch(1);
        List<Future<HttpStatusCode>> responses = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_REQUESTS)) {
            for (int i = 0; i < CONCURRENT_REQUESTS; i++) {
                responses.add(pool.submit(() -> {
                    start.await();
                    return http.postForEntity("/payments/{id}/confirm", null, String.class, id).getStatusCode();
                }));
            }
            start.countDown();

            List<HttpStatusCode> statuses = new ArrayList<>();
            for (Future<HttpStatusCode> response : responses) {
                statuses.add(response.get(30, TimeUnit.SECONDS));
            }

            assertThat(statuses).filteredOn(s -> s.equals(HttpStatus.OK)).hasSize(1);
            assertThat(statuses).filteredOn(s -> s.equals(HttpStatus.CONFLICT)).hasSize(CONCURRENT_REQUESTS - 1);
        }

        long confirmedRows = jdbc.sql("""
                        SELECT count(*) FROM outbox WHERE aggregate_id = :id AND event_type = 'payment.confirmed'
                        """)
                .param("id", id).query(Long.class).single();
        assertThat(confirmedRows).isEqualTo(1);
        assertThat(jdbc.sql("SELECT sequence FROM payments WHERE id = CAST(:id AS uuid)").param("id", id)
                .query(Integer.class).single()).isEqualTo(2);
    }
}
