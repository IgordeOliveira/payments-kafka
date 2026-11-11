package dev.paymentskafka.simulator.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.config.SimulatorProperties;
import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Chaos mode: publishes payment events straight to {@code pix.payments.v1},
 * keyed by {@code merchant_id}, and injects faults on purpose.
 *
 * <ul>
 *   <li><b>Duplicate</b>: after a step, resends one of the last
 *       {@value #DUPLICATE_WINDOW} published events unchanged (same key, same bytes).</li>
 *   <li><b>Out of order</b>: holds a {@code payment.created} back and publishes it
 *       right after the same payment's {@code sequence} 2 event. Same key, so same
 *       partition: the later event really precedes the earlier one.</li>
 *   <li><b>Invalid</b>: after a step, publishes an extra record that is either not
 *       JSON or violates the schema. Invalid records are extra; they never replace
 *       a lifecycle event.</li>
 * </ul>
 *
 * <p>Every counter is incremented only when Kafka acknowledged the record, so
 * the topic holds exactly: sum of {@code events_sent} + {@code duplicates} +
 * {@code invalid} records.
 */
public class KafkaSink implements PaymentSink {

    static final int DUPLICATE_WINDOW = 100;

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    private final KafkaTemplate<String, byte[]> template;
    private final String topic;
    private final SimulatorProperties.Chaos chaos;
    private final SimulatorStats stats;
    private final ObjectMapper mapper;
    private final Random random;

    /** Created events held back for out-of-order injection, by payment id (insertion ordered). */
    private final Map<UUID, Record> held = new LinkedHashMap<>();
    /** Recently published valid events, candidates for duplication. */
    private final List<Record> recent = new ArrayList<>();
    private int recentNext;

    public KafkaSink(KafkaTemplate<String, byte[]> template, String topic, SimulatorProperties.Chaos chaos,
                     long seed, SimulatorStats stats, ObjectMapper mapper) {
        this.template = template;
        this.topic = topic;
        this.chaos = chaos;
        this.stats = stats;
        this.mapper = mapper;
        this.random = new Random(seed ^ 0x9E3779B97F4A7C15L);
    }

    @Override
    public void send(PaymentEvent event) {
        Record record = new Record(event.merchantId(), event.type(), json(event.toContractMap()));

        if (event.type() == EventType.CREATED && chance(chaos.outOfOrderRate())) {
            held.put(event.paymentId(), record);
            return;
        }
        publishValid(record, false);

        Record earlier = held.remove(event.paymentId());
        if (earlier != null) {
            publishValid(earlier, true);
        }
        if (!recent.isEmpty() && chance(chaos.duplicateRate())) {
            Record original = recent.get(random.nextInt(recent.size()));
            publish(original.key(), original.value(), stats::recordDuplicate);
        }
        if (chance(chaos.invalidRate())) {
            publishInvalid(event);
        }
    }

    /** Releases held events in their natural order and waits for every in-flight record. */
    @Override
    public void flush() {
        for (Record record : held.values()) {
            publishValid(record, false);
        }
        held.clear();
        template.flush();
    }

    private void publishValid(Record record, boolean outOfOrder) {
        publish(record.key(), record.value(), () -> {
            stats.recordSent(record.type());
            if (outOfOrder) {
                stats.recordOutOfOrder();
            }
        });
        if (recent.size() < DUPLICATE_WINDOW) {
            recent.add(record);
        } else {
            recent.set(recentNext, record);
            recentNext = (recentNext + 1) % DUPLICATE_WINDOW;
        }
    }

    private void publishInvalid(PaymentEvent event) {
        int kind = random.nextInt(7);
        if (kind < 2) {
            byte[] valid = json(event.toContractMap());
            byte[] value = kind == 0
                    ? Arrays.copyOf(valid, valid.length / 2) // truncated JSON
                    : ("not json: payment " + event.paymentId()).getBytes(StandardCharsets.UTF_8);
            publish(event.merchantId(), value, stats::recordInvalidNonJson);
            return;
        }
        // Schema violations, each with a fresh event_id so it never collides with a valid event.
        Map<String, Object> json = event.toContractMap();
        json.put("event_id", new UUID(random.nextLong(), random.nextLong()).toString());
        switch (kind) {
            case 2 -> json.remove("event_id");
            case 3 -> json.put("amount_cents", String.format("%d.%02d", event.amountCents() / 100, event.amountCents() % 100));
            case 4 -> json.put("sequence", event.sequence() == 1 ? 2 : 1);
            case 5 -> json.put("event_type", "payment.teleported");
            default -> json.put("currency", "USD");
        }
        publish(event.merchantId(), json(json), stats::recordInvalidSchema);
    }

    private void publish(String key, byte[] value, Runnable onAck) {
        template.send(topic, key, value).whenComplete((result, error) -> {
            if (error == null) {
                onAck.run();
            } else {
                stats.recordSendError();
                log.warn("failed to publish record with key {}", key, error);
            }
        });
    }

    private boolean chance(double rate) {
        if (rate <= 0) {
            return false;
        }
        return rate >= 1 || random.nextDouble() < rate;
    }

    private byte[] json(Map<String, Object> value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private record Record(String key, EventType type, byte[] value) {
    }
}
