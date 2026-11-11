package dev.paymentskafka.simulator.sink;

import static dev.paymentskafka.simulator.support.Generators.FIXED_CLOCK;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.config.SimulatorProperties.PaymentApi;
import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.generator.GeneratorSettings;
import dev.paymentskafka.simulator.generator.LifecycleGenerator;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import dev.paymentskafka.simulator.support.Generators;
import dev.paymentskafka.simulator.support.StubPaymentApi;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Normal mode against a stubbed HTTP server that implements the documented payment API contract. */
class ApiSinkTest {

    private static final String TRANSITION = "/payments/[0-9a-f-]{36}/";

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Duration> sleeps = new ArrayList<>();
    private StubPaymentApi api;
    private SimulatorStats stats;

    @BeforeEach
    void setUp() throws Exception {
        api = new StubPaymentApi();
        stats = new SimulatorStats("normal");
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    private ApiSink sink(String baseUrl) {
        PaymentApi config = new PaymentApi(baseUrl, Duration.ofMillis(10), Duration.ofMillis(30), Duration.ofSeconds(2));
        return new ApiSink(config, stats, mapper, sleeps::add);
    }

    private static PaymentEvent event(UUID paymentId, EventType type) {
        return new PaymentEvent(UUID.randomUUID(), type, Instant.now(), paymentId, "merchant-007", 4590);
    }

    @Test
    void everyStepBecomesTheMatchingApiCall() {
        ApiSink sink = sink(api.baseUrl());
        GeneratorSettings settings = new GeneratorSettings(10, 200, 42, 0.15, 0.2, 0.01, 0.01, 0.01);
        Generators.runToCompletion(new LifecycleGenerator(settings, FIXED_CLOCK, sink, stats));

        assertThat(api.count("POST", "/payments")).isEqualTo(200).isEqualTo(stats.sent(EventType.CREATED));
        assertThat(api.count("POST", TRANSITION + "confirm")).isEqualTo(stats.sent(EventType.CONFIRMED));
        assertThat(api.count("POST", TRANSITION + "expire")).isEqualTo(stats.sent(EventType.EXPIRED));
        assertThat(api.count("POST", TRANSITION + "refund")).isEqualTo(stats.sent(EventType.REFUNDED)).isPositive();
        assertThat(stats.sent(EventType.CONFIRMED) + stats.sent(EventType.EXPIRED)).isEqualTo(200);
        assertThat(api.calls()).hasSize((int) stats.sentTotal());
        assertThat(stats.apiErrors() + stats.apiConflicts() + stats.apiRetries() + stats.apiSkipped()).isZero();

        // POST /payments carries exactly the documented body.
        StubPaymentApi.Call create = api.calls().get(0);
        JsonNode body = readTree(create.body());
        assertThat(body.get("merchant_id").asText()).matches("merchant-0\\d\\d");
        assertThat(body.get("amount_cents").asLong()).isPositive();
        assertThat(body.size()).isEqualTo(2);
    }

    @Test
    void retriesServerErrorsWithBackoffAndContinues() {
        ApiSink sink = sink(api.baseUrl());
        UUID payment = UUID.randomUUID();
        api.failNext(4);

        sink.send(event(payment, EventType.CREATED));
        sink.send(event(payment, EventType.CONFIRMED));

        assertThat(stats.apiRetries()).isEqualTo(4);
        assertThat(sleeps).containsExactly(
                Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofMillis(30), Duration.ofMillis(30));
        assertThat(stats.sent(EventType.CREATED)).isEqualTo(1);
        assertThat(stats.sent(EventType.CONFIRMED)).isEqualTo(1);
        assertThat(stats.apiErrors()).isZero();
    }

    @Test
    void retriesConnectionErrorsUntilTheApiComesUp() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        StubPaymentApi[] late = new StubPaymentApi[1];
        PaymentApi config = new PaymentApi("http://127.0.0.1:" + port, Duration.ofMillis(10), Duration.ofMillis(30),
                Duration.ofSeconds(2));
        ApiSink sink = new ApiSink(config, stats, mapper, d -> {
            sleeps.add(d);
            if (sleeps.size() == 3) {
                try {
                    late[0] = new StubPaymentApi(port); // the API becomes available
                } catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        });
        try {
            sink.send(event(UUID.randomUUID(), EventType.CREATED));
            assertThat(stats.apiRetries()).isEqualTo(3);
            assertThat(stats.sent(EventType.CREATED)).isEqualTo(1);
            assertThat(late[0].count("POST", "/payments")).isEqualTo(1);
        } finally {
            if (late[0] != null) {
                late[0].close();
            }
        }
    }

    @Test
    void conflictIsCountedAndDropped() {
        ApiSink sink = sink(api.baseUrl());
        api.override(call -> call.path().endsWith("/confirm") ? 409 : null);
        UUID payment = UUID.randomUUID();

        sink.send(event(payment, EventType.CREATED));
        sink.send(event(payment, EventType.CONFIRMED));
        // The API still says CREATED, so a refund is an invalid transition: 409 again.
        sink.send(event(payment, EventType.REFUNDED));

        assertThat(stats.apiConflicts()).isEqualTo(2);
        assertThat(stats.sent(EventType.CREATED)).isEqualTo(1);
        assertThat(stats.sent(EventType.CONFIRMED)).isZero();
        assertThat(stats.sent(EventType.REFUNDED)).isZero();
        assertThat(stats.apiRetries()).isZero();
        assertThat(stats.apiErrors()).isZero();
        assertThat(api.calls()).hasSize(3); // 409 is not retried
    }

    @Test
    void otherClientErrorsAreCountedAndLaterStepsSkipped() {
        ApiSink sink = sink(api.baseUrl());
        api.override(call -> call.path().equals("/payments") ? 400 : null);
        UUID payment = UUID.randomUUID();

        sink.send(event(payment, EventType.CREATED));
        sink.send(event(payment, EventType.CONFIRMED));

        assertThat(stats.apiErrors()).isEqualTo(1);
        assertThat(stats.apiSkipped()).isEqualTo(1);
        assertThat(stats.sentTotal()).isZero();
        assertThat(api.calls()).hasSize(1);
    }

    private JsonNode readTree(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
