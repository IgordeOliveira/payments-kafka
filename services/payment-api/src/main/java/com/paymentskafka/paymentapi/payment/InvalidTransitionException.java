package com.paymentskafka.paymentapi.payment;

import java.util.Locale;

public class InvalidTransitionException extends RuntimeException {

    private final PaymentTransition transition;
    private final PaymentStatus current;

    public InvalidTransitionException(PaymentTransition transition, PaymentStatus current) {
        super("Cannot " + transition.name().toLowerCase(Locale.ROOT) + " a payment in status " + current);
        this.transition = transition;
        this.current = current;
    }

    public PaymentTransition transition() {
        return transition;
    }

    public PaymentStatus current() {
        return current;
    }
}
