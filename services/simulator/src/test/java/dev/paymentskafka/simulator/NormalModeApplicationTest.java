package dev.paymentskafka.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.runner.SimulationRunner;
import dev.paymentskafka.simulator.sink.ApiSink;
import dev.paymentskafka.simulator.sink.KafkaSink;
import dev.paymentskafka.simulator.sink.PaymentSink;
import dev.paymentskafka.simulator.support.Await;
import dev.paymentskafka.simulator.support.StubPaymentApi;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole application in normal mode against a stubbed payment API. Kafka
 * points at a port where nothing listens: if anything tried to produce, it
 * would show up as a producer bean or a producer network thread.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class NormalModeApplicationTest {

    private static final StubPaymentApi API = start();

    private static StubPaymentApi start() {
        try {
            return new StubPaymentApi();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("SIMULATOR_MODE", () -> "normal");
        registry.add("SIMULATOR_RATE_PER_SECOND", () -> "500");
        registry.add("SIMULATOR_MAX_PAYMENTS", () -> "30");
        registry.add("PAYMENT_API_BASE_URL", API::baseUrl);
        registry.add("KAFKA_BOOTSTRAP_SERVERS", () -> "127.0.0.1:1");
    }

    @AfterAll
    static void stopApi() {
        API.close();
    }

    @Autowired
    ApplicationContext context;
    @Autowired
    SimulationRunner runner;
    @LocalServerPort
    int port;

    @Test
    void noKafkaProducerIsCreatedInNormalMode() throws Exception {
        Await.until(runner::isCompleted, Duration.ofSeconds(30));

        assertThat(context.getBean(PaymentSink.class)).isInstanceOf(ApiSink.class);
        assertThat(context.getBeanNamesForType(KafkaSink.class)).isEmpty();
        assertThat(context.getBeanNamesForType(ProducerFactory.class)).isEmpty();
        assertThat(context.getBeanNamesForType(KafkaTemplate.class)).isEmpty();
        assertThat(context.getBeanNamesForType(Producer.class)).isEmpty();
        assertThat(Thread.getAllStackTraces().keySet())
                .extracting(Thread::getName)
                .noneMatch(name -> name.startsWith("kafka-producer-network-thread"));

        // Every step went to the API instead.
        assertThat(API.count("POST", "/payments")).isEqualTo(30);
    }

    @Test
    void statsAndHealthAreServed() throws Exception {
        Await.until(runner::isCompleted, Duration.ofSeconds(30));
        HttpClient client = HttpClient.newHttpClient();

        HttpResponse<String> health = client.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/actuator/health")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);

        HttpResponse<String> response = client.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/stats")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode stats = new ObjectMapper().readTree(response.body());
        assertThat(stats.get("mode").asText()).isEqualTo("normal");
        assertThat(stats.get("finished").asBoolean()).isTrue();
        assertThat(stats.at("/events_sent/payment.created").asLong()).isEqualTo(30);
        assertThat(stats.at("/events_sent/payment.confirmed").asLong() + stats.at("/events_sent/payment.expired").asLong())
                .isEqualTo(30);
        assertThat(stats.get("events_sent_total").asLong()).isEqualTo(API.calls().size());
        assertThat(stats.at("/api/errors").asLong()).isZero();
    }
}
