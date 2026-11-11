package com.paymentskafka.paymentapi.web;

import com.paymentskafka.paymentapi.payment.InvalidTransitionException;
import com.paymentskafka.paymentapi.payment.PaymentNotFoundException;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Maps errors to RFC 9457 (formerly 7807) problem details: 400 for invalid
 * bodies (handled by the base class), 404 for unknown payments, 409 for
 * invalid transitions and 500 for anything else.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(PaymentNotFoundException.class)
    ProblemDetail handleNotFound(PaymentNotFoundException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
        problem.setTitle("Payment not found");
        return problem;
    }

    @ExceptionHandler(InvalidTransitionException.class)
    ProblemDetail handleInvalidTransition(InvalidTransitionException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        problem.setTitle("Invalid transition");
        problem.setProperty("current_status", e.current().name());
        problem.setProperty("transition", e.transition().name().toLowerCase(Locale.ROOT));
        return problem;
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail handleUnexpected(Exception e) {
        log.error("Unexpected error while handling a request", e);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR,
                "The request could not be completed; no change was committed.");
        problem.setTitle("Internal server error");
        return problem;
    }

    /** Adds the failing fields, named as in the JSON body, to the standard 400 problem. */
    @Override
    @Nullable
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(error -> toSnakeCase(error.getField()) + ": " + error.getDefaultMessage())
                .sorted()
                .toList();
        ProblemDetail problem = ex.getBody();
        problem.setDetail("Invalid request body");
        problem.setProperty("errors", errors);
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    private static String toSnakeCase(String field) {
        return field.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT);
    }
}
