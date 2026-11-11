package dev.paymentskafka.simulator.support;

import dev.paymentskafka.simulator.generator.GeneratorSettings;
import dev.paymentskafka.simulator.generator.LifecycleGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

public final class Generators {

    public static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-11-11T14:03:27.512Z"), ZoneOffset.UTC);

    private Generators() {
    }

    /** Defaults from design.md with all fraud scenarios disabled. */
    public static GeneratorSettings settings(int merchants, long maxPayments, long seed) {
        return new GeneratorSettings(merchants, maxPayments, seed, 0.15, 0.05, 0, 0, 0);
    }

    /** Ticks until the generator reports it is finished. */
    public static long runToCompletion(LifecycleGenerator generator) {
        long ticks = 0;
        while (generator.tick()) {
            ticks++;
            if (ticks > 10_000_000) {
                throw new IllegalStateException("generator did not finish");
            }
        }
        return ticks;
    }
}
