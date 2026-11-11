package com.paymentskafka.paymentapi.outbox;

import com.fasterxml.jackson.databind.JsonNode;
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
 * Loads the shared contract straight from the repository's contracts/ directory,
 * so the schema stays the single source of truth. Format assertions (uuid,
 * date-time) are enabled as contracts/README.md requires.
 */
public final class PaymentEventContract {

    /** Maven runs tests from the module directory, services/payment-api. */
    public static final Path CONTRACTS_DIR = Path.of("..", "..", "contracts");

    private static final JsonSchema SCHEMA = load(CONTRACTS_DIR.resolve("payment-event.v1.schema.json"));

    private PaymentEventContract() {
    }

    public static Set<ValidationMessage> validate(JsonNode event) {
        return SCHEMA.validate(event);
    }

    private static JsonSchema load(Path schemaFile) {
        SchemaValidatorsConfig config = SchemaValidatorsConfig.builder()
                .formatAssertionsEnabled(true)
                .build();
        try (InputStream in = Files.newInputStream(schemaFile)) {
            return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, config);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read the contract at " + schemaFile.toAbsolutePath(), e);
        }
    }
}
