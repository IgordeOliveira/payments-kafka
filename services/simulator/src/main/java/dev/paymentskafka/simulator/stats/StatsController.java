package dev.paymentskafka.simulator.stats;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /stats}: current value of every counter. Experiments use it as the expected values. */
@RestController
public class StatsController {

    private final SimulatorStats stats;

    public StatsController(SimulatorStats stats) {
        this.stats = stats;
    }

    @GetMapping(path = "/stats", produces = "application/json")
    public Map<String, Object> stats() {
        return stats.snapshot();
    }
}
