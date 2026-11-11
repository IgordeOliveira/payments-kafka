package com.paymentskafka.paymentapi.payment;

public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(String paymentId) {
        super("Payment " + paymentId + " not found");
    }
}
