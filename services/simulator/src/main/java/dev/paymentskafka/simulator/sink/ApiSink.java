package dev.paymentskafka.simulator.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.paymentskafka.simulator.config.SimulatorProperties;
import dev.paymentskafka.simulator.event.EventType;
import dev.paymentskafka.simulator.event.PaymentEvent;
import dev.paymentskafka.simulator.stats.SimulatorStats;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normal mode: performs every lifecycle step by calling the payment API (see
 * "Payment API (HTTP contract)" in {@code docs/architecture.md}). The API
 * assigns its own {@code payment_id}, so the simulator keeps a map from its
 * own payment id to the API's.
 *
 * <ul>
 *   <li>Connection errors and {@code 5xx} are retried with exponential backoff
 *       until the API answers (each retry counted in {@code api.retries}).</li>
 *   <li>{@code 409} is a lost race, not a failure: counted in
 *       {@code api.conflicts} and dropped.</li>
 *   <li>Any other status ({@code 400}, {@code 404}...) is counted in
 *       {@code api.errors} and dropped.</li>
 *   <li>A transition for a payment whose creation was dropped is skipped and
 *       counted in {@code api.skipped}.</li>
 * </ul>
 */
public class ApiSink implements PaymentSink {

    /** Upper bound for the simulator-to-API id map; the oldest entries are evicted first. */
    static final int MAX_TRACKED_PAYMENTS = 200_000;

    private static final Logger log = LoggerFactory.getLogger(ApiSink.class);

    private final HttpClient client;
    private final String baseUrl;
    private final SimulatorProperties.PaymentApi config;
    private final SimulatorStats stats;
    private final ObjectMapper mapper;
    private final Sleeper sleeper;

    private final Map<UUID, String> apiIds = new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, String> eldest) {
            return size() > MAX_TRACKED_PAYMENTS;
        }
    };

    /** Pauses between retries; replaceable so tests do not wait. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    public ApiSink(SimulatorProperties.PaymentApi config, SimulatorStats stats, ObjectMapper mapper) {
        this(config, stats, mapper, d -> Thread.sleep(d.toMillis()));
    }

    public ApiSink(SimulatorProperties.PaymentApi config, SimulatorStats stats, ObjectMapper mapper, Sleeper sleeper) {
        this.config = config;
        this.baseUrl = config.baseUrl().replaceAll("/+$", "");
        this.stats = stats;
        this.mapper = mapper;
        this.sleeper = sleeper;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(config.requestTimeout())
                .build();
    }

    @Override
    public void send(PaymentEvent event) {
        if (event.type() == EventType.CREATED) {
            create(event);
            return;
        }
        String apiId = apiIds.get(event.paymentId());
        if (apiId == null) {
            stats.recordApiSkipped();
            return;
        }
        String action = switch (event.type()) {
            case CONFIRMED -> "confirm";
            case EXPIRED -> "expire";
            case REFUNDED -> "refund";
            case CREATED -> throw new IllegalStateException();
        };
        HttpResponse<String> response = call(HttpRequest.newBuilder(
                        URI.create(baseUrl + "/payments/" + apiId + "/" + action))
                .POST(HttpRequest.BodyPublishers.noBody()));
        if (handled(response, event)) {
            stats.recordSent(event.type());
        }
        if (event.type() != EventType.CONFIRMED) {
            apiIds.remove(event.paymentId()); // expired and refunded are terminal
        }
    }

    private void create(PaymentEvent event) {
        String body;
        try {
            body = mapper.writeValueAsString(Map.of(
                    "merchant_id", event.merchantId(),
                    "amount_cents", event.amountCents()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        HttpResponse<String> response = call(HttpRequest.newBuilder(URI.create(baseUrl + "/payments"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)));
        if (!handled(response, event)) {
            return;
        }
        try {
            JsonNode payment = mapper.readTree(response.body());
            String apiId = payment.path("payment_id").asText(null);
            if (apiId == null || apiId.isBlank()) {
                throw new IOException("response has no payment_id");
            }
            apiIds.put(event.paymentId(), apiId);
            stats.recordSent(EventType.CREATED);
        } catch (IOException e) {
            stats.recordApiError();
            log.warn("unreadable response to POST /payments: {}", e.getMessage());
        }
    }

    /** @return whether the call succeeded; failures are counted here */
    private boolean handled(HttpResponse<String> response, PaymentEvent event) {
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return true;
        }
        if (status == 409) {
            stats.recordApiConflict();
        } else {
            stats.recordApiError();
            log.warn("payment API answered {} to {} for payment {}: {}",
                    status, event.type().wireName(), event.paymentId(), response.body());
        }
        return false;
    }

    /** Sends the request, retrying connection errors and 5xx until a non-5xx answer arrives. */
    private HttpResponse<String> call(HttpRequest.Builder builder) {
        HttpRequest request = builder.timeout(config.requestTimeout()).build();
        Duration backoff = config.initialBackoff();
        for (int attempt = 1; ; attempt++) {
            String failure;
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() < 500) {
                    return response;
                }
                failure = "status " + response.statusCode();
            } catch (IOException e) {
                failure = e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while calling the payment API", e);
            }
            stats.recordApiRetry();
            if (attempt == 1 || attempt % 10 == 0) {
                log.warn("payment API unavailable ({}) on {} {}, attempt {}, retrying in {} ms",
                        failure, request.method(), request.uri().getPath(), attempt, backoff.toMillis());
            }
            try {
                sleeper.sleep(backoff);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting to retry", e);
            }
            backoff = backoff.multipliedBy(2);
            if (backoff.compareTo(config.maxBackoff()) > 0) {
                backoff = config.maxBackoff();
            }
        }
    }
}
