package com.paymentskafka.paymentapi.payment;

import com.paymentskafka.paymentapi.outbox.OutboxRepository;
import com.paymentskafka.paymentapi.outbox.PaymentEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Every state change writes the payment row and its outbox row in one database
 * transaction: either both are committed or neither is. Nothing here talks to
 * Kafka; Debezium publishes the outbox.
 */
@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final OutboxRepository outbox;
    private final Clock clock;

    public PaymentService(PaymentRepository payments, OutboxRepository outbox, Clock clock) {
        this.payments = payments;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public Payment create(String merchantId, long amountCents) {
        Payment payment = Payment.create(UUID.randomUUID(), merchantId, amountCents, now());
        payments.insert(payment);
        outbox.insert(PaymentEvent.of(UUID.randomUUID(), PaymentTransition.CREATE, payment));
        return payment;
    }

    /**
     * Locks the payment row, applies the transition and records its event. The
     * row lock serializes concurrent transitions on one payment, so a second
     * concurrent request sees the new status and is rejected.
     */
    @Transactional
    public Payment transition(UUID paymentId, PaymentTransition transition) {
        Payment current = payments.findByIdForUpdate(paymentId)
                .orElseThrow(() -> new PaymentNotFoundException(paymentId.toString()));
        Payment next = current.apply(transition, now());
        payments.update(next);
        outbox.insert(PaymentEvent.of(UUID.randomUUID(), transition, next));
        return next;
    }

    @Transactional(readOnly = true)
    public Payment get(UUID paymentId) {
        return payments.findById(paymentId)
                .orElseThrow(() -> new PaymentNotFoundException(paymentId.toString()));
    }

    /** Millisecond precision, so stored and published timestamps are identical. */
    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MILLIS);
    }
}
