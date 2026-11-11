package dev.paymentskafka.simulator.generator;

import dev.paymentskafka.simulator.config.SimulatorProperties;

/** The subset of the configuration that drives lifecycle decisions. */
public record GeneratorSettings(
        int merchants,
        long maxPayments,
        long seed,
        double expireRate,
        double refundRate,
        double outlierRate,
        double burstRate,
        double refundSpreeRate) {

    public static GeneratorSettings from(SimulatorProperties p) {
        return new GeneratorSettings(
                p.merchants(),
                p.maxPayments(),
                p.seed(),
                p.expireRate(),
                p.refundRate(),
                p.fraud().outlierRate(),
                p.fraud().burstRate(),
                p.fraud().refundSpreeRate());
    }
}
