package dev.paymentskafka.simulator.support;

import java.time.Duration;
import java.util.function.BooleanSupplier;

public final class Await {

    private Await() {
    }

    public static void until(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(50);
        }
    }
}
