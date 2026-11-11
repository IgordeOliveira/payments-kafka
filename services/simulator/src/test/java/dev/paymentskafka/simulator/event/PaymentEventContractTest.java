package dev.paymentskafka.simulator.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.support.ContractValidator;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class PaymentEventContractTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void validatorAcceptsTheValidExamplesAndRejectsTheInvalidOnes() {
        // Guards the test harness itself: the validator must agree with the shared examples.
        for (JsonNode valid : ContractValidator.readExample("payment-events.valid.json")) {
            assertThat(ContractValidator.paymentEventErrors(valid)).isEmpty();
        }
        for (JsonNode invalid : ContractValidator.readExample("payment-events.invalid.json")) {
            assertThat(ContractValidator.paymentEventErrors(invalid.get("event")))
                    .as(invalid.get("reason").asText()).isNotEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(EventType.class)
    void serializedEventsValidateAgainstTheContract(EventType type) throws Exception {
        PaymentEvent event = new PaymentEvent(UUID.randomUUID(), type, Instant.parse("2026-11-11T14:03:27Z"),
                UUID.randomUUID(), "merchant-007", 4590);
        JsonNode json = mapper.readTree(mapper.writeValueAsBytes(event.toContractMap()));

        assertThat(ContractValidator.paymentEventErrors(json)).isEmpty();
        assertThat(json.get("event_type").asText()).isEqualTo(type.wireName());
        assertThat(json.get("sequence").asInt()).isEqualTo(type.sequence());
        assertThat(json.get("occurred_at").asText()).isEqualTo("2026-11-11T14:03:27.000Z");
        assertThat(json.get("currency").asText()).isEqualTo("BRL");
        assertThat(json.get("schema_version").asInt()).isEqualTo(1);
    }
}
