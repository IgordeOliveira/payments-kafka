package dev.paymentskafka.simulator.generator;

import static dev.paymentskafka.simulator.support.Generators.FIXED_CLOCK;
import static dev.paymentskafka.simulator.support.Generators.runToCompletion;
import static dev.paymentskafka.simulator.support.Generators.settings;
import static org.assertj.core.api.Assertions.assertThat;

import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import dev.paymentskafka.simulator.support.CapturingSink;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class LifecycleGeneratorTest {

    private static List<PaymentEvent> run(GeneratorSettings settings) {
        SimulatorStats stats = new SimulatorStats("test");
        CapturingSink sink = new CapturingSink(stats);
        runToCompletion(new LifecycleGenerator(settings, FIXED_CLOCK, sink, stats));
        return sink.events();
    }

    @Test
    void everyPaymentFollowsTheLifecycle() {
        // A higher refund rate and active fraud scenarios exercise every path.
        GeneratorSettings settings = new GeneratorSettings(20, 2_000, 42, 0.15, 0.3, 0.01, 0.01, 0.01);
        List<PaymentEvent> events = run(settings);

        Map<UUID, List<PaymentEvent>> byPayment = events.stream()
                .collect(Collectors.groupingBy(PaymentEvent::paymentId, LinkedHashMap::new, Collectors.toList()));
        assertThat(byPayment).hasSize(2_000);

        for (List<PaymentEvent> lifecycle : byPayment.values()) {
            List<EventType> types = lifecycle.stream().map(PaymentEvent::type).toList();
            // Exactly one created, first.
            assertThat(types.get(0)).isEqualTo(EventType.CREATED);
            assertThat(types.stream().filter(t -> t == EventType.CREATED)).hasSize(1);
            // At most one of confirmed or expired (exactly one, since a limited run drains).
            assertThat(types.stream().filter(t -> t == EventType.CONFIRMED || t == EventType.EXPIRED)).hasSize(1);
            // A refund only after a confirmation.
            if (types.contains(EventType.REFUNDED)) {
                assertThat(types).containsExactly(EventType.CREATED, EventType.CONFIRMED, EventType.REFUNDED);
            } else {
                assertThat(types).hasSize(2);
            }
            // Sequence follows the contract and every step of a payment shares merchant and amount.
            for (PaymentEvent e : lifecycle) {
                assertThat(e.sequence()).isEqualTo(e.type().sequence());
                assertThat(e.merchantId()).isEqualTo(lifecycle.get(0).merchantId());
                assertThat(e.amountCents()).isEqualTo(lifecycle.get(0).amountCents()).isPositive();
            }
        }

        Set<UUID> eventIds = new HashSet<>();
        events.forEach(e -> assertThat(eventIds.add(e.eventId())).as("event_id is unique").isTrue());
        assertThat(events).extracting(PaymentEvent::merchantId)
                .allMatch(m -> m.matches("merchant-0(0[1-9]|1[0-9]|20)"));
    }

    @Test
    void sharesFollowTheConfiguredRates() {
        List<PaymentEvent> events = run(settings(20, 20_000, 42));
        long created = events.stream().filter(e -> e.type() == EventType.CREATED).count();
        long expired = events.stream().filter(e -> e.type() == EventType.EXPIRED).count();
        long confirmed = events.stream().filter(e -> e.type() == EventType.CONFIRMED).count();
        long refunded = events.stream().filter(e -> e.type() == EventType.REFUNDED).count();

        assertThat(created).isEqualTo(20_000);
        assertThat(expired + confirmed).isEqualTo(created);
        assertThat((double) expired / created).isBetween(0.13, 0.17);
        assertThat((double) refunded / confirmed).isBetween(0.04, 0.06);
    }

    @Test
    void identicalSeedsProduceIdenticalOutput() {
        GeneratorSettings settings = new GeneratorSettings(20, 3_000, 7, 0.15, 0.05, 0.01, 0.01, 0.01);
        List<PaymentEvent> first = run(settings);
        List<PaymentEvent> second = run(settings);
        assertThat(first).isNotEmpty().isEqualTo(second);
    }

    @Test
    void differentSeedsProduceDifferentOutput() {
        assertThat(run(settings(20, 500, 1))).isNotEqualTo(run(settings(20, 500, 2)));
    }

    @Test
    void stopsAfterTheConfiguredNumberOfPayments() {
        SimulatorStats stats = new SimulatorStats("test");
        CapturingSink sink = new CapturingSink(stats);
        LifecycleGenerator generator = new LifecycleGenerator(settings(5, 100, 42), FIXED_CLOCK, sink, stats);
        runToCompletion(generator);

        assertThat(generator.isFinished()).isTrue();
        assertThat(generator.paymentsCreated()).isEqualTo(100);
        assertThat(sink.count(EventType.CREATED)).isEqualTo(100);
        assertThat(generator.tick()).isFalse();
        assertThat(sink.count(EventType.CREATED)).isEqualTo(100);
    }

    @Test
    void unlimitedRunKeepsGoing() {
        SimulatorStats stats = new SimulatorStats("test");
        CapturingSink sink = new CapturingSink(stats);
        LifecycleGenerator generator = new LifecycleGenerator(settings(5, 0, 42), FIXED_CLOCK, sink, stats);
        for (int i = 0; i < 10_000; i++) {
            assertThat(generator.tick()).isTrue();
        }
        // One lifecycle step per slot when no fraud scenario is active.
        assertThat(sink.events()).hasSize(10_000);
        assertThat(stats.steps()).isEqualTo(10_000);
    }
}
