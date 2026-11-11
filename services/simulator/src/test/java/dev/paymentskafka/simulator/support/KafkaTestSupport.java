package dev.paymentskafka.simulator.support;

import dev.paymentskafka.simulator.config.SimulatorConfiguration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** One Kafka container (random port) shared by every test in the JVM. */
public final class KafkaTestSupport {

    public static final int PARTITIONS = 6;

    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"));

    static {
        KAFKA.start();
    }

    private KafkaTestSupport() {
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    /** Creates a fresh topic shaped like {@code pix.payments.v1} (6 partitions) so tests never share records. */
    public static String createTopic() {
        String topic = "pix.payments.v1.test-" + UUID.randomUUID().toString().substring(0, 8);
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
            awaitLeaders(admin, topic);
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
        return topic;
    }

    /**
     * A just-created topic can answer produce requests with NOT_LEADER_OR_FOLLOWER
     * for a moment; wait until every partition reports a leader and an in-sync replica.
     */
    private static void awaitLeaders(Admin admin, String topic) throws InterruptedException, ExecutionException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            boolean ready = description.partitions().size() == PARTITIONS && description.partitions().stream()
                    .allMatch(p -> p.leader() != null && !p.leader().isEmpty() && !p.isr().isEmpty());
            if (ready) {
                Thread.sleep(500);
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("topic " + topic + " has no leaders");
            }
            Thread.sleep(100);
        }
    }

    /** A template with exactly the producer settings the application uses in chaos mode. */
    public static KafkaTemplate<String, byte[]> template() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                SimulatorConfiguration.producerConfig(bootstrapServers())));
    }

    /** Reads every record currently in the topic, partition by partition, up to the end offsets. */
    public static List<ConsumerRecord<String, byte[]>> readAll(String topic) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);

            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("timed out reading " + topic);
                }
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            return records;
        }
    }
}
