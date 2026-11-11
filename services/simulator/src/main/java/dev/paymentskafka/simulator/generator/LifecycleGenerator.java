package dev.paymentskafka.simulator.generator;

import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.sink.PaymentSink;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.UUID;

/**
 * Decides what happens next in the payment lifecycle and hands each step to a
 * {@link PaymentSink}.
 *
 * <p>Time is logical: every call to {@link #tick()} is one <em>slot</em>, and
 * pending steps are due a number of slots after the previous step. Decisions
 * never depend on wall-clock time, so the same seed and configuration always
 * produce the same sequence of steps (only {@code occurred_at} varies).
 *
 * <p>Each slot performs one lifecycle step: a due pending step if there is one,
 * otherwise a new payment, otherwise (payment limit reached) the next pending
 * step ahead of time so a limited run drains quickly. Fraud scenarios add extra
 * steps on top: a burst creates many payments for one merchant in the same slot
 * and a refund spree refunds several confirmed payments of one merchant.
 */
public final class LifecycleGenerator {

    /** Slots between created and confirmed/expired. */
    static final int DECIDE_DELAY_MIN = 5;
    static final int DECIDE_DELAY_MAX = 50;
    /** Slots between confirmed and refunded. */
    static final int REFUND_DELAY_MIN = 20;
    static final int REFUND_DELAY_MAX = 200;

    /** Typical (mean) payment amount per merchant, in cents. */
    static final long TYPICAL_AMOUNT_MIN = 1_500;
    static final long TYPICAL_AMOUNT_MAX = 20_000;
    /** An ordinary payment is the merchant's typical amount times [0.5, 1.5). */
    static final double NORMAL_FACTOR_MIN = 0.5;
    static final double NORMAL_FACTOR_SPAN = 1.0;
    /** An outlier is the merchant's typical amount times [10, 30). */
    public static final double OUTLIER_FACTOR_MIN = 10.0;
    static final double OUTLIER_FACTOR_SPAN = 20.0;

    static final int BURST_SIZE_MIN = 10;
    static final int BURST_SIZE_MAX = 20;
    static final int SPREE_SIZE_MIN = 3;
    static final int SPREE_SIZE_MAX = 5;
    /** Confirmed payments kept per merchant as refund spree candidates. */
    static final int SPREE_POOL_CAP = 50;

    private final GeneratorSettings settings;
    private final Clock clock;
    private final PaymentSink sink;
    private final SimulatorStats stats;

    /** Drives every decision. */
    private final Random decisions;
    /** Generates ids, kept separate so ids never shift decisions. */
    private final Random ids;

    private final String[] merchantIds;
    private final long[] typicalAmounts;
    private final List<ArrayDeque<Payment>> refundPools;
    private final PriorityQueue<Pending> pending = new PriorityQueue<>(
            Comparator.comparingLong(Pending::dueSlot).thenComparingLong(Pending::order));

    private long slot;
    private long order;
    private long paymentsCreated;
    private boolean finished;

    public LifecycleGenerator(GeneratorSettings settings, Clock clock, PaymentSink sink, SimulatorStats stats) {
        this.settings = settings;
        this.clock = clock;
        this.sink = sink;
        this.stats = stats;
        this.decisions = new Random(settings.seed());
        this.ids = new Random(settings.seed() ^ 0x5DEECE66DL);

        int n = settings.merchants();
        this.merchantIds = new String[n];
        this.typicalAmounts = new long[n];
        this.refundPools = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            merchantIds[i] = merchantId(i + 1);
            typicalAmounts[i] = between(TYPICAL_AMOUNT_MIN, TYPICAL_AMOUNT_MAX);
            refundPools.add(new ArrayDeque<>());
        }
    }

    public static String merchantId(int number) {
        return String.format("merchant-%03d", number);
    }

    /**
     * Runs one slot.
     *
     * @return {@code false} once the payment limit is reached and every payment has finished its lifecycle
     */
    public boolean tick() {
        if (finished) {
            return false;
        }
        slot++;
        stats.recordStep();

        if (chance(settings.burstRate())) {
            burst();
        }
        if (chance(settings.refundSpreeRate())) {
            refundSpree();
        }
        lifecycleStep();

        if (!canCreate() && pending.isEmpty()) {
            finished = true;
        }
        return !finished;
    }

    public boolean isFinished() {
        return finished;
    }

    public long paymentsCreated() {
        return paymentsCreated;
    }

    /** The typical amount of a merchant; outliers are at least ten times this. */
    public long typicalAmount(String merchantId) {
        for (int i = 0; i < merchantIds.length; i++) {
            if (merchantIds[i].equals(merchantId)) {
                return typicalAmounts[i];
            }
        }
        throw new IllegalArgumentException("unknown merchant " + merchantId);
    }

    private void lifecycleStep() {
        while (true) {
            Pending next = pending.peek();
            if (next != null && next.dueSlot() <= slot) {
                pending.poll();
                if (execute(next)) {
                    return;
                }
            } else if (canCreate()) {
                create(decisions.nextInt(merchantIds.length));
                return;
            } else if (next != null) {
                // Payment limit reached: run the next step ahead of time.
                pending.poll();
                if (execute(next)) {
                    return;
                }
            } else {
                return;
            }
        }
    }

    /** @return whether a step was emitted */
    private boolean execute(Pending p) {
        Payment payment = p.payment();
        switch (p.kind()) {
            case DECIDE -> decide(payment);
            case REFUND -> {
                if (payment.state != EventType.CONFIRMED) {
                    return false;
                }
                refund(payment);
            }
        }
        return true;
    }

    private boolean canCreate() {
        return settings.maxPayments() == 0 || paymentsCreated < settings.maxPayments();
    }

    private void create(int merchant) {
        long typical = typicalAmounts[merchant];
        long amount = Math.max(1, Math.round(typical * (NORMAL_FACTOR_MIN + decisions.nextDouble() * NORMAL_FACTOR_SPAN)));
        if (chance(settings.outlierRate())) {
            amount = Math.round(typical * (OUTLIER_FACTOR_MIN + decisions.nextDouble() * OUTLIER_FACTOR_SPAN));
            stats.recordOutlier();
        }
        Payment payment = new Payment(randomUuid(), merchant, amount);
        paymentsCreated++;
        emit(payment, EventType.CREATED);
        schedule(payment, Kind.DECIDE, between(DECIDE_DELAY_MIN, DECIDE_DELAY_MAX));
    }

    private void decide(Payment payment) {
        if (chance(settings.expireRate())) {
            emit(payment, EventType.EXPIRED);
            return;
        }
        emit(payment, EventType.CONFIRMED);
        if (chance(settings.refundRate())) {
            schedule(payment, Kind.REFUND, between(REFUND_DELAY_MIN, REFUND_DELAY_MAX));
        } else {
            ArrayDeque<Payment> pool = refundPools.get(payment.merchant);
            pool.addLast(payment);
            if (pool.size() > SPREE_POOL_CAP) {
                pool.removeFirst();
            }
        }
    }

    private void refund(Payment payment) {
        emit(payment, EventType.REFUNDED);
    }

    /** Many payments for one merchant in the same slot. */
    private void burst() {
        if (!canCreate()) {
            return;
        }
        int merchant = decisions.nextInt(merchantIds.length);
        long size = between(BURST_SIZE_MIN, BURST_SIZE_MAX);
        if (settings.maxPayments() > 0) {
            size = Math.min(size, settings.maxPayments() - paymentsCreated);
        }
        if (size < 2) {
            return;
        }
        stats.recordBurst((int) size);
        for (int i = 0; i < size; i++) {
            create(merchant);
        }
    }

    /** Several confirmed payments of one merchant refunded in the same slot. */
    private void refundSpree() {
        List<Integer> candidates = new ArrayList<>();
        for (int m = 0; m < refundPools.size(); m++) {
            if (refundPools.get(m).size() >= SPREE_SIZE_MIN) {
                candidates.add(m);
            }
        }
        if (candidates.isEmpty()) {
            return;
        }
        ArrayDeque<Payment> pool = refundPools.get(candidates.get(decisions.nextInt(candidates.size())));
        int size = (int) Math.min(pool.size(), between(SPREE_SIZE_MIN, SPREE_SIZE_MAX));
        stats.recordRefundSpree(size);
        for (int i = 0; i < size; i++) {
            refund(pool.removeLast());
        }
    }

    private void emit(Payment payment, EventType type) {
        payment.state = type;
        sink.send(new PaymentEvent(
                randomUuid(),
                type,
                clock.instant().truncatedTo(ChronoUnit.MILLIS),
                payment.id,
                merchantIds[payment.merchant],
                payment.amountCents));
    }

    private void schedule(Payment payment, Kind kind, long delay) {
        pending.add(new Pending(slot + delay, order++, kind, payment));
    }

    private boolean chance(double rate) {
        if (rate <= 0) {
            return false;
        }
        return rate >= 1 || decisions.nextDouble() < rate;
    }

    private long between(long min, long max) {
        return min + decisions.nextLong(max - min + 1);
    }

    private UUID randomUuid() {
        long msb = (ids.nextLong() & ~0xF000L) | 0x4000L; // version 4
        long lsb = (ids.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L; // IETF variant
        return new UUID(msb, lsb);
    }

    private enum Kind { DECIDE, REFUND }

    private record Pending(long dueSlot, long order, Kind kind, Payment payment) {
    }

    private static final class Payment {
        final UUID id;
        final int merchant;
        final long amountCents;
        EventType state;

        Payment(UUID id, int merchant, long amountCents) {
            this.id = id;
            this.merchant = merchant;
            this.amountCents = amountCents;
        }
    }
}
