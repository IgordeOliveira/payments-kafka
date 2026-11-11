package dev.paymentskafka.simulator.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/**
 * Validates payloads against the shared contract in the repository's
 * {@code contracts/} directory (format assertions enabled, as the contract README requires).
 */
public final class ContractValidator {

    /** Maven runs tests with the module directory as working directory. */
    public static final Path CONTRACTS = Path.of("..", "..", "contracts");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonSchema PAYMENT_EVENT = load("payment-event.v1.schema.json");

    private ContractValidator() {
    }

    private static JsonSchema load(String file) {
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build();
        try (InputStream in = Files.newInputStream(CONTRACTS.resolve(file))) {
            return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, config);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read contract " + CONTRACTS.resolve(file).toAbsolutePath(), e);
        }
    }

    /** @return the schema violations; empty when the node is a valid payment event */
    public static Set<ValidationMessage> paymentEventErrors(JsonNode node) {
        return PAYMENT_EVENT.validate(node);
    }

    /** @return the parsed event, or {@code null} if the bytes are not JSON or violate the schema */
    public static JsonNode parseValidPaymentEvent(byte[] value) {
        JsonNode node;
        try {
            node = MAPPER.readTree(value);
        } catch (IOException e) {
            return null;
        }
        if (node == null || !paymentEventErrors(node).isEmpty()) {
            return null;
        }
        return node;
    }

    public static JsonNode readExample(String file) {
        try {
            return MAPPER.readTree(CONTRACTS.resolve("examples").resolve(file).toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
