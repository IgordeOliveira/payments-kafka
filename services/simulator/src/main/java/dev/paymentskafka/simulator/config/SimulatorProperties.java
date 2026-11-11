package dev.paymentskafka.simulator.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * All simulator settings. Environment variable names and defaults are mapped in
 * {@code application.yml}; see the README for the full table.
 */
@Validated
@ConfigurationProperties(prefix = "simulator")
public record SimulatorProperties(
        @NotNull Mode mode,
        @Min(1) @Max(10_000) int ratePerSecond,
        @Min(1) @Max(999) int merchants,
        @Min(0) long maxPayments,
        long seed,
        @DecimalMin("0.0") @DecimalMax("1.0") double expireRate,
        @DecimalMin("0.0") @DecimalMax("1.0") double refundRate,
        @NotNull @Valid Chaos chaos,
        @NotNull @Valid Fraud fraud,
        @NotNull @Valid Kafka kafka,
        @NotNull @Valid PaymentApi paymentApi,
        @NotNull Duration statsLogInterval,
        boolean autostart) {

    public enum Mode {
        /** Publishes directly to Kafka and injects faults. */
        CHAOS,
        /** Drives the payment API over HTTP. */
        NORMAL
    }

    public record Chaos(
            @DecimalMin("0.0") @DecimalMax("1.0") double duplicateRate,
            @DecimalMin("0.0") @DecimalMax("1.0") double outOfOrderRate,
            @DecimalMin("0.0") @DecimalMax("1.0") double invalidRate) {
    }

    public record Fraud(
            @DecimalMin("0.0") @DecimalMax("1.0") double outlierRate,
            @DecimalMin("0.0") @DecimalMax("1.0") double burstRate,
            @DecimalMin("0.0") @DecimalMax("1.0") double refundSpreeRate) {
    }

    public record Kafka(@NotBlank String bootstrapServers, @NotBlank String topic) {
    }

    public record PaymentApi(
            @NotBlank String baseUrl,
            @NotNull Duration initialBackoff,
            @NotNull Duration maxBackoff,
            @NotNull Duration requestTimeout) {
    }
}
