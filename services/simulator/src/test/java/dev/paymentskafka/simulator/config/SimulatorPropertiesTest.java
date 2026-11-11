package dev.paymentskafka.simulator.config;

import static org.assertj.core.api.Assertions.assertThat;

import dev.paymentskafka.simulator.config.SimulatorProperties.Mode;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** Binds application.yml exactly as the application does, with environment-style overrides. */
class SimulatorPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(SimulatorProperties.class)
    static class Config {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
            .withUserConfiguration(Config.class);

    @Test
    void defaultsMatchTheDesign() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            SimulatorProperties p = context.getBean(SimulatorProperties.class);
            assertThat(p.mode()).isEqualTo(Mode.CHAOS);
            assertThat(p.ratePerSecond()).isEqualTo(50);
            assertThat(p.merchants()).isEqualTo(20);
            assertThat(p.maxPayments()).isZero();
            assertThat(p.seed()).isEqualTo(42L);
            assertThat(p.expireRate()).isEqualTo(0.15);
            assertThat(p.refundRate()).isEqualTo(0.05);
            assertThat(p.chaos().duplicateRate()).isEqualTo(0.05);
            assertThat(p.chaos().outOfOrderRate()).isEqualTo(0.02);
            assertThat(p.chaos().invalidRate()).isEqualTo(0.01);
            assertThat(p.fraud().outlierRate()).isEqualTo(0.002);
            assertThat(p.fraud().burstRate()).isEqualTo(0.001);
            assertThat(p.fraud().refundSpreeRate()).isEqualTo(0.001);
            assertThat(p.kafka().bootstrapServers()).isEqualTo("localhost:9092");
            assertThat(p.kafka().topic()).isEqualTo("pix.payments.v1");
            assertThat(p.paymentApi().baseUrl()).isEqualTo("http://localhost:8081");
            assertThat(p.statsLogInterval()).isEqualTo(Duration.ofSeconds(10));
            assertThat(context.getEnvironment().getProperty("server.port")).isEqualTo("8082");
        });
    }

    @Test
    void environmentVariablesOverrideDefaults() {
        runner.withPropertyValues(
                        "SIMULATOR_MODE=normal",
                        "SIMULATOR_RATE_PER_SECOND=200",
                        "SIMULATOR_MERCHANTS=5",
                        "SIMULATOR_MAX_PAYMENTS=1000",
                        "SIMULATOR_SEED=7",
                        "SIMULATOR_CHAOS_DUPLICATE_RATE=0.5",
                        "SIMULATOR_FRAUD_BURST_RATE=1",
                        "KAFKA_BOOTSTRAP_SERVERS=kafka:29092",
                        "PAYMENT_API_BASE_URL=http://payment-api:8081",
                        "SERVER_PORT=9999")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    SimulatorProperties p = context.getBean(SimulatorProperties.class);
                    assertThat(p.mode()).isEqualTo(Mode.NORMAL);
                    assertThat(p.ratePerSecond()).isEqualTo(200);
                    assertThat(p.merchants()).isEqualTo(5);
                    assertThat(p.maxPayments()).isEqualTo(1000);
                    assertThat(p.seed()).isEqualTo(7);
                    assertThat(p.chaos().duplicateRate()).isEqualTo(0.5);
                    assertThat(p.fraud().burstRate()).isEqualTo(1.0);
                    assertThat(p.kafka().bootstrapServers()).isEqualTo("kafka:29092");
                    assertThat(p.paymentApi().baseUrl()).isEqualTo("http://payment-api:8081");
                    assertThat(context.getEnvironment().getProperty("server.port")).isEqualTo("9999");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SIMULATOR_EXPIRE_RATE=1.5",
            "SIMULATOR_REFUND_RATE=-0.1",
            "SIMULATOR_CHAOS_DUPLICATE_RATE=2",
            "SIMULATOR_CHAOS_OUT_OF_ORDER_RATE=-1",
            "SIMULATOR_CHAOS_INVALID_RATE=1.01",
            "SIMULATOR_FRAUD_OUTLIER_RATE=3",
            "SIMULATOR_FRAUD_BURST_RATE=-0.5",
            "SIMULATOR_FRAUD_REFUND_SPREE_RATE=1.2",
            "SIMULATOR_RATE_PER_SECOND=0",
            "SIMULATOR_RATE_PER_SECOND=20000",
            "SIMULATOR_MERCHANTS=0",
            "SIMULATOR_MAX_PAYMENTS=-1",
            "SIMULATOR_MODE=turbo"
    })
    void rejectsOutOfRangeValues(String override) {
        runner.withPropertyValues(override).run(context -> {
            assertThat(context).hasFailed();
            boolean bindFailure = false;
            for (Throwable t = context.getStartupFailure(); t != null; t = t.getCause()) {
                bindFailure |= t instanceof BindException;
            }
            assertThat(bindFailure).as("startup fails while binding simulator.*").isTrue();
        });
    }
}
