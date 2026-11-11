package dev.paymentskafka.simulator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.generator.GeneratorSettings;
import dev.paymentskafka.simulator.generator.LifecycleGenerator;
import dev.paymentskafka.simulator.runner.RateController;
import dev.paymentskafka.simulator.runner.SimulationRunner;
import dev.paymentskafka.simulator.sink.ApiSink;
import dev.paymentskafka.simulator.sink.KafkaSink;
import dev.paymentskafka.simulator.sink.PaymentSink;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import java.time.Clock;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/** Shared wiring. The mode only selects the {@link PaymentSink}. */
@Configuration(proxyBeanMethods = false)
public class SimulatorConfiguration {

    @Bean
    SimulatorStats simulatorStats(SimulatorProperties properties) {
        return new SimulatorStats(properties.mode().name().toLowerCase());
    }

    @Bean
    LifecycleGenerator lifecycleGenerator(SimulatorProperties properties, PaymentSink sink, SimulatorStats stats) {
        return new LifecycleGenerator(GeneratorSettings.from(properties), Clock.systemUTC(), sink, stats);
    }

    @Bean
    SimulationRunner simulationRunner(SimulatorProperties properties, LifecycleGenerator generator, PaymentSink sink,
                                      SimulatorStats stats, ObjectMapper mapper) {
        return new SimulationRunner(generator, sink, stats,
                new RateController(properties.ratePerSecond(), System::nanoTime),
                properties.statsLogInterval(), properties.autostart(), mapper);
    }

    /** Chaos mode: the only place a Kafka producer is created. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "simulator.mode", havingValue = "chaos", matchIfMissing = true)
    static class ChaosConfiguration {

        @Bean
        ProducerFactory<String, byte[]> producerFactory(SimulatorProperties properties) {
            return new DefaultKafkaProducerFactory<>(producerConfig(properties.kafka().bootstrapServers()));
        }

        @Bean
        KafkaTemplate<String, byte[]> kafkaTemplate(ProducerFactory<String, byte[]> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        KafkaSink kafkaSink(KafkaTemplate<String, byte[]> template, SimulatorProperties properties,
                            SimulatorStats stats, ObjectMapper mapper) {
            return new KafkaSink(template, properties.kafka().topic(), properties.chaos(), properties.seed(),
                    stats, mapper);
        }
    }

    /** Normal mode: every step is an HTTP call to the payment API. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "simulator.mode", havingValue = "normal")
    static class NormalConfiguration {

        @Bean
        ApiSink apiSink(SimulatorProperties properties, SimulatorStats stats, ObjectMapper mapper) {
            return new ApiSink(properties.paymentApi(), stats, mapper);
        }
    }

    /**
     * Producer settings for chaos mode. Duplicates are injected on purpose at
     * the application level, never by sloppy producer settings.
     */
    public static Map<String, Object> producerConfig(String bootstrapServers) {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, "simulator",
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5,
                ProducerConfig.LINGER_MS_CONFIG, 5,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    }
}
