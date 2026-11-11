package dev.paymentskafka.simulator.runner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.generator.LifecycleGenerator;
import dev.paymentskafka.simulator.sink.PaymentSink;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Scheduled ticker: every {@value #TICK_MILLIS} ms it asks the
 * {@link RateController} how many slots are due and runs that many generator
 * slots. All generator and sink calls happen on this single thread. Stats are
 * logged every {@code statsLogInterval} and once more when the run ends.
 */
public class SimulationRunner implements SmartLifecycle {

    static final long TICK_MILLIS = 10;

    private static final Logger log = LoggerFactory.getLogger(SimulationRunner.class);

    private final LifecycleGenerator generator;
    private final PaymentSink sink;
    private final SimulatorStats stats;
    private final RateController rate;
    private final Duration statsLogInterval;
    private final boolean autostart;
    private final ObjectMapper mapper;

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> ticker;
    private ScheduledFuture<?> statsLogger;
    private volatile boolean running;
    private volatile boolean completed;

    public SimulationRunner(LifecycleGenerator generator, PaymentSink sink, SimulatorStats stats, RateController rate,
                            Duration statsLogInterval, boolean autostart, ObjectMapper mapper) {
        this.generator = generator;
        this.sink = sink;
        this.stats = stats;
        this.rate = rate;
        this.statsLogInterval = statsLogInterval;
        this.autostart = autostart;
        this.mapper = mapper;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        stats.setRunning(true);
        rate.start();
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "simulator-ticker");
            t.setDaemon(true);
            return t;
        });
        ticker = executor.scheduleAtFixedRate(this::safeTick, 0, TICK_MILLIS, TimeUnit.MILLISECONDS);
        long logMillis = statsLogInterval.toMillis();
        if (logMillis > 0) {
            statsLogger = executor.scheduleAtFixedRate(() -> logStats("simulator stats"), logMillis, logMillis,
                    TimeUnit.MILLISECONDS);
        }
        log.info("simulator started");
    }

    /** Runs the slots that are due now. Package-private for tests that drive time by hand. */
    void tick() {
        if (completed) {
            return;
        }
        long due = rate.slotsDue();
        for (long i = 0; i < due; i++) {
            if (!generator.tick()) {
                complete();
                return;
            }
        }
    }

    private void safeTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            // Never let an exception cancel the scheduled ticker.
            log.error("simulator tick failed", e);
        }
    }

    private void complete() {
        completed = true;
        sink.flush();
        stats.setFinished(true);
        stats.setRunning(false);
        if (ticker != null) {
            ticker.cancel(false);
        }
        if (statsLogger != null) {
            statsLogger.cancel(false);
        }
        logStats("simulator finished: payment limit reached");
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (!completed) {
            sink.flush();
        }
        stats.setRunning(false);
        logStats("simulator stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return autostart;
    }

    public boolean isCompleted() {
        return completed;
    }

    private void logStats(String message) {
        try {
            log.info("{} {}", message, mapper.writeValueAsString(stats.snapshot()));
        } catch (JsonProcessingException e) {
            log.warn("could not serialize stats", e);
        }
    }
}
