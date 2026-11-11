package dev.paymentskafka.simulator.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A tiny in-process stand-in for the payment API (JDK HTTP server on a random
 * port) implementing the HTTP contract of docs/architecture.md, with hooks to
 * inject failures.
 */
public final class StubPaymentApi implements AutoCloseable {

    /** A request as received: method, path and body. */
    public record Call(String method, String path, String body) {
    }

    private final HttpServer server;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<Call> calls = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, String> status = new ConcurrentHashMap<>();
    /** Number of upcoming requests answered with 503 before behaving normally. */
    private final AtomicInteger failNext = new AtomicInteger();
    /** Optional override: return a status for a call, or null to behave normally. */
    private volatile Function<Call, Integer> override = c -> null;

    public StubPaymentApi() throws IOException {
        this(0);
    }

    /** @param port fixed port, or 0 for a random one */
    public StubPaymentApi(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public List<Call> calls() {
        synchronized (calls) {
            return List.copyOf(calls);
        }
    }

    public long count(String method, String pathRegex) {
        return calls().stream().filter(c -> c.method().equals(method) && c.path().matches(pathRegex)).count();
    }

    public void failNext(int requests) {
        failNext.set(requests);
    }

    public void override(Function<Call, Integer> override) {
        this.override = override;
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Call call = new Call(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), body);
        calls.add(call);

        if (failNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            respond(exchange, 503, "{\"error\":\"unavailable\"}");
            return;
        }
        Integer forced = override.apply(call);
        if (forced != null) {
            respond(exchange, forced, "{\"error\":\"forced\"}");
            return;
        }

        String[] parts = call.path().split("/");
        if (call.method().equals("POST") && call.path().equals("/payments")) {
            JsonNode request = mapper.readTree(body);
            if (!request.hasNonNull("merchant_id") || request.path("amount_cents").asLong() < 1) {
                respond(exchange, 400, "{\"error\":\"invalid body\"}");
                return;
            }
            String id = UUID.randomUUID().toString();
            status.put(id, "CREATED");
            respond(exchange, 201, payment(id, request.get("merchant_id").asText(),
                    request.get("amount_cents").asLong(), "CREATED", 1));
        } else if (call.method().equals("POST") && parts.length == 4 && parts[1].equals("payments")) {
            String id = parts[2];
            String current = status.get(id);
            if (current == null) {
                respond(exchange, 404, "{\"error\":\"not found\"}");
                return;
            }
            String next;
            int sequence;
            switch (parts[3]) {
                case "confirm" -> { next = current.equals("CREATED") ? "CONFIRMED" : null; sequence = 2; }
                case "expire" -> { next = current.equals("CREATED") ? "EXPIRED" : null; sequence = 2; }
                case "refund" -> { next = current.equals("CONFIRMED") ? "REFUNDED" : null; sequence = 3; }
                default -> { respond(exchange, 404, "{}"); return; }
            }
            if (next == null) {
                respond(exchange, 409, "{\"error\":\"invalid transition\"}");
                return;
            }
            status.put(id, next);
            respond(exchange, 200, payment(id, "merchant", 1, next, sequence));
        } else {
            respond(exchange, 404, "{}");
        }
    }

    private String payment(String id, String merchant, long amount, String state, int sequence) throws IOException {
        return mapper.writeValueAsString(Map.of(
                "payment_id", id, "merchant_id", merchant, "amount_cents", amount, "currency", "BRL",
                "status", state, "sequence", sequence,
                "created_at", "2026-11-11T14:03:20.100Z", "updated_at", "2026-11-11T14:03:27.512Z"));
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
