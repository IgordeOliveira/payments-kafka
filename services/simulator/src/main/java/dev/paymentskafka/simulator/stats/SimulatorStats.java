package dev.paymentskafka.simulator.stats;

import dev.paymentskafka.simulator.event.EventType;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Run counters. Experiments compare them with what consumers observed, so each
 * "sent" counter is incremented only once the record was acknowledged by Kafka
 * (chaos mode) or the API answered 2xx (normal mode).
 */
public class SimulatorStats {

    private final String mode;
    private volatile boolean running;
    private volatile boolean finished;

    private final AtomicLong steps = new AtomicLong();
    private final Map<EventType, AtomicLong> sent = new EnumMap<>(EventType.class);

    // Chaos mode.
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong outOfOrder = new AtomicLong();
    private final AtomicLong invalidNonJson = new AtomicLong();
    private final AtomicLong invalidSchema = new AtomicLong();
    private final AtomicLong sendErrors = new AtomicLong();

    // Normal mode.
    private final AtomicLong apiRetries = new AtomicLong();
    private final AtomicLong apiConflicts = new AtomicLong();
    private final AtomicLong apiErrors = new AtomicLong();
    private final AtomicLong apiSkipped = new AtomicLong();

    // Fraud scenarios (both modes).
    private final AtomicLong outliers = new AtomicLong();
    private final AtomicLong bursts = new AtomicLong();
    private final AtomicLong burstPayments = new AtomicLong();
    private final AtomicLong refundSprees = new AtomicLong();
    private final AtomicLong refundSpreeRefunds = new AtomicLong();

    public SimulatorStats(String mode) {
        this.mode = mode;
        for (EventType type : EventType.values()) {
            sent.put(type, new AtomicLong());
        }
    }

    public void recordStep() { steps.incrementAndGet(); }
    public void recordSent(EventType type) { sent.get(type).incrementAndGet(); }
    public void recordDuplicate() { duplicates.incrementAndGet(); }
    public void recordOutOfOrder() { outOfOrder.incrementAndGet(); }
    public void recordInvalidNonJson() { invalidNonJson.incrementAndGet(); }
    public void recordInvalidSchema() { invalidSchema.incrementAndGet(); }
    public void recordSendError() { sendErrors.incrementAndGet(); }
    public void recordApiRetry() { apiRetries.incrementAndGet(); }
    public void recordApiConflict() { apiConflicts.incrementAndGet(); }
    public void recordApiError() { apiErrors.incrementAndGet(); }
    public void recordApiSkipped() { apiSkipped.incrementAndGet(); }
    public void recordOutlier() { outliers.incrementAndGet(); }

    public void recordBurst(int payments) {
        bursts.incrementAndGet();
        burstPayments.addAndGet(payments);
    }

    public void recordRefundSpree(int refunds) {
        refundSprees.incrementAndGet();
        refundSpreeRefunds.addAndGet(refunds);
    }

    public void setRunning(boolean value) { running = value; }
    public void setFinished(boolean value) { finished = value; }

    public long steps() { return steps.get(); }
    public long sent(EventType type) { return sent.get(type).get(); }
    public long duplicates() { return duplicates.get(); }
    public long outOfOrder() { return outOfOrder.get(); }
    public long invalid() { return invalidNonJson.get() + invalidSchema.get(); }
    public long invalidNonJson() { return invalidNonJson.get(); }
    public long invalidSchema() { return invalidSchema.get(); }
    public long sendErrors() { return sendErrors.get(); }
    public long apiRetries() { return apiRetries.get(); }
    public long apiConflicts() { return apiConflicts.get(); }
    public long apiErrors() { return apiErrors.get(); }
    public long apiSkipped() { return apiSkipped.get(); }
    public long outliers() { return outliers.get(); }
    public long bursts() { return bursts.get(); }
    public long burstPayments() { return burstPayments.get(); }
    public long refundSprees() { return refundSprees.get(); }
    public long refundSpreeRefunds() { return refundSpreeRefunds.get(); }
    public boolean isFinished() { return finished; }

    public long sentTotal() {
        return sent.values().stream().mapToLong(AtomicLong::get).sum();
    }

    /** Point-in-time view with snake_case keys, served by {@code GET /stats}. */
    public Map<String, Object> snapshot() {
        Map<String, Object> eventsSent = new LinkedHashMap<>();
        for (EventType type : EventType.values()) {
            eventsSent.put(type.wireName(), sent(type));
        }

        Map<String, Object> chaos = new LinkedHashMap<>();
        chaos.put("duplicates", duplicates());
        chaos.put("out_of_order", outOfOrder());
        chaos.put("invalid", invalid());
        chaos.put("invalid_non_json", invalidNonJson());
        chaos.put("invalid_schema", invalidSchema());
        chaos.put("send_errors", sendErrors());

        Map<String, Object> api = new LinkedHashMap<>();
        api.put("retries", apiRetries());
        api.put("conflicts", apiConflicts());
        api.put("errors", apiErrors());
        api.put("skipped", apiSkipped());

        Map<String, Object> fraud = new LinkedHashMap<>();
        fraud.put("outliers", outliers());
        fraud.put("bursts", bursts());
        fraud.put("burst_payments", burstPayments());
        fraud.put("refund_sprees", refundSprees());
        fraud.put("refund_spree_refunds", refundSpreeRefunds());

        Map<String, Object> json = new LinkedHashMap<>();
        json.put("mode", mode);
        json.put("running", running);
        json.put("finished", finished);
        json.put("steps", steps());
        json.put("events_sent", eventsSent);
        json.put("events_sent_total", sentTotal());
        json.put("chaos", chaos);
        json.put("api", api);
        json.put("fraud", fraud);
        return json;
    }
}
