package com.paymentskafka.paymentapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The outbox pattern is only honest if the service cannot publish to Kafka
 * itself: no Kafka client may be on the (test) classpath, which is a superset
 * of the runtime classpath.
 */
class NoKafkaDependencyTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "org.apache.kafka.clients.producer.KafkaProducer",
            "org.apache.kafka.clients.consumer.KafkaConsumer",
            "org.springframework.kafka.core.KafkaTemplate"
    })
    void kafkaClientClassesAreAbsent(String className) {
        assertThatThrownBy(() -> Class.forName(className, false, getClass().getClassLoader()))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"org/apache/kafka", "org/springframework/kafka"})
    void noKafkaPackagesOnTheClasspath(String packagePath) throws Exception {
        assertThat(Collections.list(getClass().getClassLoader().getResources(packagePath))).isEmpty();
    }
}
