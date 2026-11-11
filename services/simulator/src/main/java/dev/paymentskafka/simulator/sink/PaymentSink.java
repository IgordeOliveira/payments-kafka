package dev.paymentskafka.simulator.sink;

import dev.paymentskafka.simulator.event.PaymentEvent;

/**
 * Receives the lifecycle steps decided by the generator. The mode only selects
 * the sink: {@code KafkaSink} in chaos mode, {@code ApiSink} in normal mode.
 * Sinks own the "sent" counters, because only they know what really went out.
 */
public interface PaymentSink {

    /** Performs one lifecycle step. Called from a single thread. */
    void send(PaymentEvent event);

    /** Pushes out anything still buffered (for example held-back events) and waits for it. */
    default void flush() {
    }
}
