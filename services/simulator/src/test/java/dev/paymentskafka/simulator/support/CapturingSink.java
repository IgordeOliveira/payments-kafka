package dev.paymentskafka.simulator.support;

import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.sink.PaymentSink;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import java.util.ArrayList;
import java.util.List;

/** Records every step in memory and counts it as sent. */
public class CapturingSink implements PaymentSink {

    private final List<PaymentEvent> events = new ArrayList<>();
    private final SimulatorStats stats;

    public CapturingSink(SimulatorStats stats) {
        this.stats = stats;
    }

    @Override
    public void send(PaymentEvent event) {
        events.add(event);
        stats.recordSent(event.type());
    }

    public List<PaymentEvent> events() {
        return events;
    }

    public long count(EventType type) {
        return events.stream().filter(e -> e.type() == type).count();
    }
}
