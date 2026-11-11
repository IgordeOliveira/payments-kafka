package com.paymentskafka.paymentapi.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentskafka.paymentapi.TestcontainersConfiguration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class PaymentControllerTest {

    private static final String ISO_INSTANT = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z";

    @Autowired
    MockMvc mvc;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    JdbcClient jdbc;

    // --- success paths ---------------------------------------------------------------

    @Test
    void createReturns201WithTheRepresentation() throws Exception {
        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"merchant_id\": \"merchant-007\", \"amount_cents\": 4590}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", matchesPattern("/payments/[0-9a-f-]{36}")))
                .andExpect(jsonPath("$.payment_id").value(matchesPattern("[0-9a-f-]{36}")))
                .andExpect(jsonPath("$.merchant_id").value("merchant-007"))
                .andExpect(jsonPath("$.amount_cents").value(4590))
                .andExpect(jsonPath("$.currency").value("BRL"))
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.sequence").value(1))
                .andExpect(jsonPath("$.created_at").value(matchesPattern(ISO_INSTANT)))
                .andExpect(jsonPath("$.updated_at").value(matchesPattern(ISO_INSTANT)));
    }

    @Test
    void representationHasExactlyTheDocumentedFields() throws Exception {
        JsonNode payment = create("merchant-007", 4590);

        assertThat(payment.properties()).extracting(java.util.Map.Entry::getKey).containsExactlyInAnyOrder(
                "payment_id", "merchant_id", "amount_cents", "currency", "status", "sequence",
                "created_at", "updated_at");
    }

    @Test
    void confirmThenRefund() throws Exception {
        String id = create("merchant-007", 4590).get("payment_id").asText();

        transition(id, "confirm").andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CONFIRMED"))
                .andExpect(jsonPath("$.sequence").value(2));
        transition(id, "refund").andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REFUNDED"))
                .andExpect(jsonPath("$.sequence").value(3));

        mvc.perform(get("/payments/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.payment_id").value(id))
                .andExpect(jsonPath("$.status").value("REFUNDED"))
                .andExpect(jsonPath("$.sequence").value(3));
    }

    @Test
    void expire() throws Exception {
        String id = create("merchant-007", 4590).get("payment_id").asText();

        transition(id, "expire").andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("EXPIRED"))
                .andExpect(jsonPath("$.sequence").value(2));
    }

    @Test
    void getReturnsTheCurrentState() throws Exception {
        JsonNode created = create("merchant-007", 4590);

        mvc.perform(get("/payments/{id}", created.get("payment_id").asText()))
                .andExpect(status().isOk())
                .andExpect(content().json(created.toString(), true));
    }

    // --- errors ----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"amount_cents\": 4590}",
            "{\"merchant_id\": \"\", \"amount_cents\": 4590}",
            "{\"merchant_id\": \"merchant-007\"}",
            "{\"merchant_id\": \"merchant-007\", \"amount_cents\": 0}",
            "{\"merchant_id\": \"merchant-007\", \"amount_cents\": -5}",
            "{\"merchant_id\": \"merchant-007\", \"amount_cents\": 45.5}",
            "{\"merchant_id\": \"merchant-007\", \"amount_cents\": \"4590\"}",
            "{\"merchant_id\": \"merchant-007\", \"amount_cents\": 99999999999999999999}",
            "{\"merchant_id\": \"a-merchant-id-that-is-longer-than-sixty-four-characters-and-so-is-invalid\", \"amount_cents\": 1}",
            "not json",
            ""
    })
    void invalidBodyReturns400ProblemAndWritesNothing(String body) throws Exception {
        long paymentsBefore = count("payments");
        long outboxBefore = count("outbox");

        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.title").exists());

        assertThat(count("payments")).isEqualTo(paymentsBefore);
        assertThat(count("outbox")).isEqualTo(outboxBefore);
    }

    @Test
    void validationProblemNamesTheJsonFields() throws Exception {
        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"merchant_id\": \"\", \"amount_cents\": 0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value(matchesPattern("amount_cents: .*")))
                .andExpect(jsonPath("$.errors[1]").value(matchesPattern("merchant_id: .*")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "expire", "refund"})
    void transitionOnUnknownIdReturns404(String action) throws Exception {
        transition(UUID.randomUUID().toString(), action)
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0b9d6c0e-7f1e-4c7a-8a55-2a8f4f1f6c42", "not-a-uuid"})
    void getOnUnknownIdReturns404(String id) throws Exception {
        mvc.perform(get("/payments/{id}", id))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void refundOnCreatedReturns409AndChangesNothing() throws Exception {
        JsonNode created = create("merchant-007", 4590);
        String id = created.get("payment_id").asText();

        transition(id, "refund")
                .andExpect(status().isConflict())
                .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.current_status").value("CREATED"))
                .andExpect(jsonPath("$.transition").value("refund"));

        mvc.perform(get("/payments/{id}", id)).andExpect(content().json(created.toString(), true));
        assertThat(outboxRowsFor(id)).isEqualTo(1);
    }

    @Test
    void confirmOnExpiredReturns409AndChangesNothing() throws Exception {
        String id = create("merchant-007", 4590).get("payment_id").asText();
        transition(id, "expire").andExpect(status().isOk());

        transition(id, "confirm")
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.current_status").value("EXPIRED"));

        mvc.perform(get("/payments/{id}", id))
                .andExpect(jsonPath("$.status").value("EXPIRED"))
                .andExpect(jsonPath("$.sequence").value(2));
        assertThat(outboxRowsFor(id)).isEqualTo(2);
    }

    // --- helpers ---------------------------------------------------------------------

    private JsonNode create(String merchantId, long amountCents) throws Exception {
        String body = mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"merchant_id\": \"%s\", \"amount_cents\": %d}".formatted(merchantId, amountCents)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(body);
    }

    private ResultActions transition(String id, String action) throws Exception {
        return mvc.perform(post("/payments/{id}/{action}", id, action));
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }

    private long outboxRowsFor(String paymentId) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE aggregate_id = :id").param("id", paymentId)
                .query(Long.class).single();
    }
}
