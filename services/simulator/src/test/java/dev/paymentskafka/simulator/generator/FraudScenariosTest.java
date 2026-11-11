package dev.paymentskafka.simulator.generator;

import static dev.paymentskafka.simulator.support.Generators.FIXED_CLOCK;
import static dev.paymentskafka.simulator.support.Generators.runToCompletion;
import static org.assertj.core.api.Assertions.assertThat;

import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import dev.paymentskafka.simulator.support.CapturingSink;
import java.util.List;
import org.junit.jupiter.api.Test;

class FraudScenariosTest {

    private SimulatorStats stats;
    private CapturingSink sink;
    private LifecycleGenerator generator;

    private void run(double expireRate, double refundRate, double outlier, double burst, double spree) {
        stats = new SimulatorStats("test");
        sink = new CapturingSink(stats);
        GeneratorSettings settings = new GeneratorSettings(20, 2_000, 42, expireRate, refundRate, outlier, burst, spree);
        generator = new LifecycleGenerator(settings, FIXED_CLOCK, sink, stats);
        runToCompletion(generator);
    }

    private List<PaymentEvent> created() {
        return sink.events().stream().filter(e -> e.type() == EventType.CREATED).toList();
    }

    /** Longest run of consecutive events of one type for one merchant. */
    private int longestRun(EventType type) {
        int longest = 0;
        int current = 0;
        String merchant = null;
        for (PaymentEvent e : sink.events()) {
            if (e.type() == type && e.merchantId().equals(merchant)) {
                current++;
            } else if (e.type() == type) {
                merchant = e.merchantId();
                current = 1;
            } else {
                merchant = null;
                current = 0;
            }
            longest = Math.max(longest, current);
        }
        return longest;
    }

    @Test
    void outlierRateOneMakesEveryPaymentAnOutlier() {
        run(0.15, 0.05, 1, 0, 0);
        assertThat(created()).hasSize(2_000).allSatisfy(e ->
                assertThat(e.amountCents()).isGreaterThanOrEqualTo(10 * generator.typicalAmount(e.merchantId())));
        assertThat(stats.outliers()).isEqualTo(2_000);
    }

    @Test
    void outlierRateZeroProducesNoOutlier() {
        run(0.15, 0.05, 0, 0, 0);
        assertThat(created()).hasSize(2_000).allSatisfy(e ->
                assertThat(e.amountCents()).isLessThan(10 * generator.typicalAmount(e.merchantId())));
        assertThat(stats.outliers()).isZero();
    }

    @Test
    void burstRateOneProducesBursts() {
        run(0.15, 0.05, 0, 1, 0);
        assertThat(stats.bursts()).isPositive();
        assertThat(stats.burstPayments()).isGreaterThanOrEqualTo(stats.bursts() * 2);
        assertThat(longestRun(EventType.CREATED)).isGreaterThanOrEqualTo(LifecycleGenerator.BURST_SIZE_MIN);
        assertThat(created()).hasSize(2_000); // bursts never exceed the payment limit
    }

    @Test
    void burstRateZeroProducesNoBurst() {
        run(0.15, 0.05, 0, 0, 0);
        assertThat(stats.bursts()).isZero();
        assertThat(longestRun(EventType.CREATED)).isLessThan(LifecycleGenerator.BURST_SIZE_MIN);
    }

    @Test
    void refundSpreeRateOneProducesSprees() {
        // No ordinary refunds, so every refund comes from a spree.
        run(0.15, 0, 0, 0, 1);
        assertThat(stats.refundSprees()).isPositive();
        assertThat(sink.count(EventType.REFUNDED)).isEqualTo(stats.refundSpreeRefunds());
        assertThat(longestRun(EventType.REFUNDED)).isGreaterThanOrEqualTo(LifecycleGenerator.SPREE_SIZE_MIN);
    }

    @Test
    void refundSpreeRateZeroProducesNoSpree() {
        run(0.15, 0, 0, 0, 0);
        assertThat(stats.refundSprees()).isZero();
        assertThat(sink.count(EventType.REFUNDED)).isZero();
    }

    @Test
    void spreeRefundsAreConfirmedPaymentsRefundedOnce() {
        run(0.15, 0.2, 0, 0, 0.05);
        assertThat(stats.refundSprees()).isPositive();
        // Each refunded payment was confirmed and is refunded exactly once.
        long distinctRefunded = sink.events().stream()
                .filter(e -> e.type() == EventType.REFUNDED).map(PaymentEvent::paymentId).distinct().count();
        assertThat(distinctRefunded).isEqualTo(sink.count(EventType.REFUNDED));
        List<java.util.UUID> confirmed = sink.events().stream()
                .filter(e -> e.type() == EventType.CONFIRMED).map(PaymentEvent::paymentId).toList();
        sink.events().stream().filter(e -> e.type() == EventType.REFUNDED)
                .forEach(e -> assertThat(confirmed).contains(e.paymentId()));
    }
}
