package dev.paymentskafka.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.runner.SimulationRunner;
import dev.paymentskafka.simulator.support.Await;
import dev.paymentskafka.simulator.support.KafkaTestSupport;
import dev.paymentskafka.simulator.support.TopicContents;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole application in chaos mode (the default) against Kafka in
 * Testcontainers: {@code GET /stats} must equal what is on the topic.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
class ChaosModeApplicationTest {

    private static final String TOPIC = KafkaTestSupport.createTopic();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KafkaTestSupport::bootstrapServers);
        registry.add("simulator.kafka.topic", () -> TOPIC);
        registry.add("SIMULATOR_RATE_PER_SECOND", () -> "1000");
        registry.add("SIMULATOR_MAX_PAYMENTS", () -> "400");
        registry.add("SIMULATOR_CHAOS_DUPLICATE_RATE", () -> "0.1");
        registry.add("SIMULATOR_CHAOS_OUT_OF_ORDER_RATE", () -> "0.1");
        registry.add("SIMULATOR_CHAOS_INVALID_RATE", () -> "0.1");
        registry.add("SIMULATOR_FRAUD_OUTLIER_RATE", () -> "0.05");
        registry.add("SIMULATOR_FRAUD_BURST_RATE", () -> "0.01");
        registry.add("SIMULATOR_FRAUD_REFUND_SPREE_RATE", () -> "0.01");
    }

    @Autowired
    SimulationRunner runner;
    @LocalServerPort
    int port;

    @Test
    void statsEndpointReflectsWhatWasPublished() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI statsUri = URI.create("http://localhost:" + port + "/stats");

        Await.until(runner::isCompleted, Duration.ofSeconds(60));
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(statsUri).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValueSatisfying(
                v -> assertThat(v).startsWith("application/json"));
        JsonNode stats = new ObjectMapper().readTree(response.body());

        TopicContents topic = new TopicContents(KafkaTestSupport.readAll(TOPIC));

        assertThat(stats.get("mode").asText()).isEqualTo("chaos");
        assertThat(stats.get("finished").asBoolean()).isTrue();
        assertThat(stats.get("running").asBoolean()).isFalse();
        assertThat(stats.at("/events_sent/payment.created").asLong()).isEqualTo(400);
        for (EventType type : EventType.values()) {
            assertThat(stats.at("/events_sent/" + type.wireName().replace("/", "~1")).asLong())
                    .as(type.wireName()).isEqualTo(topic.distinctPerType.get(type));
        }
        assertThat(stats.at("/chaos/duplicates").asLong()).isEqualTo(topic.duplicates.size()).isPositive();
        assertThat(stats.at("/chaos/invalid").asLong()).isEqualTo(topic.invalid.size()).isPositive();
        assertThat(stats.at("/chaos/out_of_order").asLong()).isEqualTo(topic.outOfOrderPayments).isPositive();
        assertThat(stats.at("/chaos/send_errors").asLong()).isZero();
        assertThat(stats.get("events_sent_total").asLong() + stats.at("/chaos/duplicates").asLong()
                + stats.at("/chaos/invalid").asLong()).isEqualTo(topic.records.size());
        assertThat(stats.at("/fraud/outliers").asLong()).isPositive();
        assertThat(topic.wronglyKeyed).isZero();

        HttpResponse<String> health = client.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/actuator/health")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);
    }

    @Test
    void statsExposeEveryCounterGroup() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        JsonNode stats = new ObjectMapper().readTree(client.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/stats")).build(), HttpResponse.BodyHandlers.ofString()).body());
        assertThat(stats.has("events_sent")).isTrue();
        assertThat(stats.has("chaos")).isTrue();
        assertThat(stats.has("api")).isTrue();
        assertThat(stats.has("fraud")).isTrue();
        assertThat(stats.get("steps").asLong()).isGreaterThanOrEqualTo(0);
    }
}
