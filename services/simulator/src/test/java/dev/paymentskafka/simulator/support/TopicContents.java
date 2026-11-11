package dev.paymentskafka.simulator.support;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import dev.paymentskafka.simulator.event.EventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * What a consumer would observe on the topic, computed independently from the
 * simulator's counters so tests can compare the two.
 */
public final class TopicContents {

    public final List<ConsumerRecord<String, byte[]>> records;
    /** Records that are not JSON or violate the payment event schema. */
    public final List<ConsumerRecord<String, byte[]>> invalid = new ArrayList<>();
    /** First occurrence of each valid event_id. */
    public final Map<String, ConsumerRecord<String, byte[]>> firstByEventId = new HashMap<>();
    /** Valid records whose event_id was already seen earlier in its partition. */
    public final List<ConsumerRecord<String, byte[]>> duplicates = new ArrayList<>();
    public final Map<EventType, Long> distinctPerType = new EnumMap<>(EventType.class);
    /** Payments whose sequence 2 record precedes their sequence 1 record (both first occurrences). */
    public final long outOfOrderPayments;
    /** Valid records whose key differs from their merchant_id. */
    public final long wronglyKeyed;
    /** Duplicates whose bytes or key differ from the original. */
    public final long alteredDuplicates;

    public TopicContents(List<ConsumerRecord<String, byte[]>> records) {
        this.records = records;
        for (EventType type : EventType.values()) {
            distinctPerType.put(type, 0L);
        }
        // Process each partition in offset order.
        List<ConsumerRecord<String, byte[]>> ordered = new ArrayList<>(records);
        ordered.sort((a, b) -> a.partition() != b.partition()
                ? Integer.compare(a.partition(), b.partition()) : Long.compare(a.offset(), b.offset()));

        Map<String, long[]> sequenceOffsets = new HashMap<>(); // payment_id -> [seq1 offset, seq2 offset]
        Map<String, Integer> paymentPartition = new HashMap<>();
        long keyed = 0;
        long altered = 0;
        for (ConsumerRecord<String, byte[]> record : ordered) {
            JsonNode event = ContractValidator.parseValidPaymentEvent(record.value());
            if (event == null) {
                invalid.add(record);
                continue;
            }
            if (!event.get("merchant_id").asText().equals(record.key())) {
                keyed++;
            }
            String eventId = event.get("event_id").asText();
            ConsumerRecord<String, byte[]> original = firstByEventId.putIfAbsent(eventId, record);
            if (original != null) {
                duplicates.add(record);
                if (!Arrays.equals(original.value(), record.value()) || !original.key().equals(record.key())) {
                    altered++;
                }
                continue;
            }
            EventType type = Arrays.stream(EventType.values())
                    .filter(t -> t.wireName().equals(event.get("event_type").asText())).findFirst().orElseThrow();
            distinctPerType.merge(type, 1L, Long::sum);

            String paymentId = event.get("payment_id").asText();
            Integer previous = paymentPartition.putIfAbsent(paymentId, record.partition());
            if (previous != null && previous != record.partition()) {
                throw new AssertionError("payment " + paymentId + " spans partitions");
            }
            int sequence = event.get("sequence").asInt();
            if (sequence <= 2) {
                sequenceOffsets.computeIfAbsent(paymentId, k -> new long[] {-1, -1})[sequence - 1] = record.offset();
            }
        }
        this.wronglyKeyed = keyed;
        this.alteredDuplicates = altered;
        this.outOfOrderPayments = sequenceOffsets.values().stream()
                .filter(o -> o[0] >= 0 && o[1] >= 0 && o[1] < o[0]).count();
    }

    public long validRecords() {
        return records.size() - invalid.size();
    }

    public long invalidNonJson() {
        return invalid.stream().filter(r -> {
            try {
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(r.value());
                return false;
            } catch (Exception e) {
                return true;
            }
        }).count();
    }
}
