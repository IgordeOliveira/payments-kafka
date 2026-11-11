package dev.paymentskafka.simulator.runner;

import static dev.paymentskafka.simulator.support.Generators.FIXED_CLOCK;
import static dev.paymentskafka.simulator.support.Generators.settings;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.generator.GeneratorSettings;
import dev.paymentskafka.simulator.generator.LifecycleGenerator;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import dev.paymentskafka.simulator.support.CapturingSink;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SimulationRunnerTest {

    private static final long MS = 1_000_000L;

    /** A runner whose time only moves when the test says so. */
    private static final class Harness {
        final AtomicLong nanos = new AtomicLong(123 * MS);
        final SimulatorStats stats = new SimulatorStats("test");
        final CapturingSink sink = new CapturingSink(stats);
        final RateController rate;
        final SimulationRunner runner;

        Harness(int ratePerSecond, GeneratorSettings settings) {
            rate = new RateController(ratePerSecond, nanos::get);
            LifecycleGenerator generator = new LifecycleGenerator(settings, FIXED_CLOCK, sink, stats);
            runner = new SimulationRunner(generator, sink, stats, rate, Duration.ZERO, false, new ObjectMapper());
            rate.start();
        }

        void advance(long millis) {
            nanos.addAndGet(millis * MS);
            runner.tick();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 50, 333, 1000})
    void emitsExactlyTheConfiguredRateWithAFakeClock(int ratePerSecond) {
        Harness h = new Harness(ratePerSecond, settings(20, 0, 42));
        for (int i = 0; i < 1_000; i++) {
            h.advance(SimulationRunner.TICK_MILLIS); // 10 seconds in 10 ms ticks
        }
        assertThat(h.stats.steps()).isEqualTo(10L * ratePerSecond);
        // Without fraud scenarios every slot is exactly one lifecycle step.
        assertThat(h.sink.events()).hasSize(10 * ratePerSecond);
    }

    @Test
    void irregularTickIntervalsDoNotChangeTheRate() {
        Harness h = new Harness(50, settings(20, 0, 42));
        Random jitter = new Random(1);
        long elapsed = 0;
        while (elapsed < 10_000) {
            long step = Math.min(1 + jitter.nextInt(60), 10_000 - elapsed);
            h.advance(step);
            elapsed += step;
        }
        assertThat(h.stats.steps()).isEqualTo(500);
    }

    @Test
    void catchesUpAtMostOneSecondOfBacklog() {
        Harness h = new Harness(50, settings(20, 0, 42));
        h.advance(5_000); // stalled for five seconds
        assertThat(h.stats.steps()).isEqualTo(50);
        h.advance(1_000);
        assertThat(h.stats.steps()).isEqualTo(100);
    }

    @Test
    void stopsAtThePaymentLimit() {
        Harness h = new Harness(1_000, settings(5, 100, 42));
        for (int i = 0; i < 1_000 && !h.runner.isCompleted(); i++) {
            h.advance(SimulationRunner.TICK_MILLIS);
        }
        assertThat(h.runner.isCompleted()).isTrue();
        assertThat(h.stats.isFinished()).isTrue();
        assertThat(h.sink.count(EventType.CREATED)).isEqualTo(100);
        assertThat(h.sink.count(EventType.CONFIRMED) + h.sink.count(EventType.EXPIRED)).isEqualTo(100);

        int size = h.sink.events().size();
        h.advance(10_000);
        assertThat(h.sink.events()).hasSize(size);
    }

    /**
     * The spec scenario with real time: 10 s at 50/s is within 20 percent of 500.
     * Also checks that stats are logged periodically (every 5 s here).
     */
    @Test
    @ExtendWith(OutputCaptureExtension.class)
    void realTimeRunIsWithinTwentyPercent(CapturedOutput output) throws InterruptedException {
        SimulatorStats stats = new SimulatorStats("test");
        CapturingSink sink = new CapturingSink(stats);
        LifecycleGenerator generator = new LifecycleGenerator(settings(20, 0, 42), FIXED_CLOCK, sink, stats);
        SimulationRunner runner = new SimulationRunner(generator, sink, stats,
                new RateController(50, System::nanoTime), Duration.ofSeconds(5), true, new ObjectMapper());
        runner.start();
        TimeUnit.SECONDS.sleep(10);
        runner.stop();
        assertThat(stats.steps()).isBetween(400L, 600L);
        assertThat(output.getOut().split("simulator stats \\{", -1).length - 1)
                .as("periodic stats log lines").isGreaterThanOrEqualTo(1);
        assertThat(output.getOut()).contains("simulator stopped {");
    }
}
