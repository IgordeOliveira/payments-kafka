package com.paymentskafka.paymentapi.web;

import com.paymentskafka.paymentapi.payment.Payment;
import com.paymentskafka.paymentapi.payment.PaymentNotFoundException;
import com.paymentskafka.paymentapi.payment.PaymentService;
import com.paymentskafka.paymentapi.payment.PaymentTransition;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> create(@Valid @RequestBody CreatePaymentRequest request) {
        Payment payment = service.create(request.merchantId(), request.amountCents());
        return ResponseEntity.created(URI.create("/payments/" + payment.id())).body(PaymentResponse.from(payment));
    }

    @PostMapping("/{id}/confirm")
    public PaymentResponse confirm(@PathVariable String id) {
        return transition(id, PaymentTransition.CONFIRM);
    }

    @PostMapping("/{id}/expire")
    public PaymentResponse expire(@PathVariable String id) {
        return transition(id, PaymentTransition.EXPIRE);
    }

    @PostMapping("/{id}/refund")
    public PaymentResponse refund(@PathVariable String id) {
        return transition(id, PaymentTransition.REFUND);
    }

    @GetMapping("/{id}")
    public PaymentResponse get(@PathVariable String id) {
        return PaymentResponse.from(service.get(parseId(id)));
    }

    private PaymentResponse transition(String id, PaymentTransition transition) {
        return PaymentResponse.from(service.transition(parseId(id), transition));
    }

    /** An id that is not a UUID cannot name an existing payment, so it is a 404 rather than a 400. */
    private static UUID parseId(String id) {
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw new PaymentNotFoundException(id);
        }
    }
}
