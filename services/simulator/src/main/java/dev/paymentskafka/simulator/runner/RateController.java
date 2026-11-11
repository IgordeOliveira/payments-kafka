package dev.paymentskafka.simulator.runner;

import java.util.function.LongSupplier;

/**
 * Turns elapsed time into a number of generator slots so that, on average,
 * exactly {@code ratePerSecond} slots run per second regardless of how often
 * the ticker fires. If the simulator falls behind (for example while the
 * payment API is down) at most one second of backlog is caught up; older
 * backlog is dropped instead of being sent as one huge spike.
 */
public final class RateController {

    private final int ratePerSecond;
    private final LongSupplier nanoTime;
    private long startNanos;
    private long issued;
    private boolean started;

    public RateController(int ratePerSecond, LongSupplier nanoTime) {
        if (ratePerSecond < 1) {
            throw new IllegalArgumentException("ratePerSecond must be at least 1");
        }
        this.ratePerSecond = ratePerSecond;
        this.nanoTime = nanoTime;
    }

    public void start() {
        startNanos = nanoTime.getAsLong();
        issued = 0;
        started = true;
    }

    /** Number of slots to run now; consumed by the caller. */
    public long slotsDue() {
        if (!started) {
            throw new IllegalStateException("not started");
        }
        long elapsedMillis = (nanoTime.getAsLong() - startNanos) / 1_000_000L;
        long target = elapsedMillis * ratePerSecond / 1_000L;
        long due = Math.max(0, target - issued);
        issued = Math.max(issued, target);
        // Backlog beyond one second is dropped (issued already moved to target).
        return Math.min(due, ratePerSecond);
    }
}
