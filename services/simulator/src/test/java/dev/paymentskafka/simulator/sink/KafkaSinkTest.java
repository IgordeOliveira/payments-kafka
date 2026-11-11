package dev.paymentskafka.simulator.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.config.SimulatorConfiguration;
import dev.paymentskafka.simulator.config.SimulatorProperties.Chaos;
import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.generator.GeneratorSettings;
import dev.paymentskafka.simulator.generator.LifecycleGenerator;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import dev.paymentskafka.simulator.support.Generators;
import dev.paymentskafka.simulator.support.KafkaTestSupport;
import dev.paymentskafka.simulator.support.TopicContents;
import java.time.Clock;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

/** Chaos mode against a real broker (Testcontainers, random port). */
class KafkaSinkTest {

    private static final Chaos NO_FAULTS = new Chaos(0, 0, 0);

    private SimulatorStats stats;

    /** Runs a limited simulation through a KafkaSink and returns what is on the topic. */
    private TopicContents simulate(Chaos chaos, long payments) {
        String topic = KafkaTestSupport.createTopic();
        stats = new SimulatorStats("chaos");
        KafkaTemplate<String, byte[]> template = KafkaTestSupport.template();
        try {
            KafkaSink sink = new KafkaSink(template, topic, chaos, 42, stats, new ObjectMapper());
            // Fraud scenarios on, so their traffic shapes are covered too.
            GeneratorSettings settings = new GeneratorSettings(20, payments, 42, 0.15, 0.1, 0.01, 0.01, 0.01);
            Generators.runToCompletion(new LifecycleGenerator(settings, Clock.systemUTC(), sink, stats));
            sink.flush();
        } finally {
            template.destroy();
        }
        return new TopicContents(KafkaTestSupport.readAll(topic));
    }

    @Test
    void producerUsesAcksAllAndIdempotence() {
        Map<String, Object> config = SimulatorConfiguration.producerConfig("localhost:9092");
        assertThat(config.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
    }

    @Test
    void withoutFaultsEveryRecordIsKeyedAndValid() {
        TopicContents topic = simulate(NO_FAULTS, 300);

        assertThat(topic.records).isNotEmpty();
        assertThat(topic.invalid).isEmpty();
        assertThat(topic.duplicates).isEmpty();
        assertThat(topic.wronglyKeyed).isZero();
        assertThat(topic.outOfOrderPayments).isZero();
        assertThat((long) topic.records.size()).isEqualTo(stats.sentTotal());
        for (EventType type : EventType.values()) {
            assertThat(topic.distinctPerType.get(type)).as(type.wireName()).isEqualTo(stats.sent(type));
        }
        assertThat(stats.sent(EventType.CREATED)).isEqualTo(300);
        assertThat(stats.sendErrors()).isZero();
    }

    @Test
    void duplicatesOnTheTopicMatchTheCounter() {
        TopicContents topic = simulate(new Chaos(0.2, 0, 0), 300);

        assertThat(stats.duplicates()).isPositive();
        assertThat((long) topic.duplicates.size()).isEqualTo(stats.duplicates());
        assertThat(topic.alteredDuplicates).as("duplicates are resent unchanged").isZero();
        assertThat((long) topic.records.size()).isEqualTo(stats.sentTotal() + stats.duplicates());
        assertThat(topic.invalid).isEmpty();
        assertThat(topic.outOfOrderPayments).isZero();
    }

    @Test
    void outOfOrderPaymentsOnTheTopicMatchTheCounter() {
        TopicContents topic = simulate(new Chaos(0, 0.2, 0), 300);

        assertThat(stats.outOfOrder()).isPositive();
        assertThat(topic.outOfOrderPayments).isEqualTo(stats.outOfOrder());
        assertThat((long) topic.records.size()).isEqualTo(stats.sentTotal());
        assertThat(topic.duplicates).isEmpty();
        assertThat(topic.invalid).isEmpty();
        for (EventType type : EventType.values()) {
            assertThat(topic.distinctPerType.get(type)).isEqualTo(stats.sent(type));
        }
    }

    @Test
    void invalidRecordsOnTheTopicMatchTheCounter() {
        TopicContents topic = simulate(new Chaos(0, 0, 0.2), 300);

        assertThat(stats.invalid()).isPositive();
        assertThat((long) topic.invalid.size()).isEqualTo(stats.invalid());
        assertThat(topic.invalidNonJson()).isEqualTo(stats.invalidNonJson()).isPositive();
        assertThat(topic.invalid.size() - topic.invalidNonJson()).isEqualTo(stats.invalidSchema()).isPositive();
        assertThat(topic.validRecords()).isEqualTo(stats.sentTotal());
        assertThat(topic.duplicates).isEmpty();
    }

    @Test
    void allFaultsTogetherStillAddUp() {
        TopicContents topic = simulate(new Chaos(0.1, 0.1, 0.1), 500);

        assertThat((long) topic.records.size())
                .isEqualTo(stats.sentTotal() + stats.duplicates() + stats.invalid());
        assertThat((long) topic.duplicates.size()).isEqualTo(stats.duplicates());
        assertThat((long) topic.invalid.size()).isEqualTo(stats.invalid());
        assertThat(topic.outOfOrderPayments).isEqualTo(stats.outOfOrder());
        assertThat(topic.wronglyKeyed).isZero();
        for (EventType type : EventType.values()) {
            assertThat(topic.distinctPerType.get(type)).isEqualTo(stats.sent(type));
        }
    }
}
