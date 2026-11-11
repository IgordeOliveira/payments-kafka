package com.paymentskafka.paymentapi.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.EnumSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PaymentTransitionTest {

    static Stream<Arguments> validTransitions() {
        return Stream.of(
                Arguments.of(PaymentTransition.CONFIRM, PaymentStatus.CREATED, PaymentStatus.CONFIRMED, "payment.confirmed", 2),
                Arguments.of(PaymentTransition.EXPIRE, PaymentStatus.CREATED, PaymentStatus.EXPIRED, "payment.expired", 2),
                Arguments.of(PaymentTransition.REFUND, PaymentStatus.CONFIRMED, PaymentStatus.REFUNDED, "payment.refunded", 3));
    }

    @ParameterizedTest(name = "{0} from {1}")
    @MethodSource("validTransitions")
    void validTransitionProducesStatusEventAndSequence(PaymentTransition transition, PaymentStatus from,
            PaymentStatus expectedStatus, String expectedEventType, int expectedSequence) {
        assertThat(transition.applyTo(from)).isEqualTo(expectedStatus);
        assertThat(transition.eventType()).isEqualTo(expectedEventType);
        assertThat(transition.resultingSequence()).isEqualTo(expectedSequence);
        assertThat(transition.resultingSequence()).isEqualTo(from.sequence() + 1);
    }

    @Test
    void creationStartsTheLifecycle() {
        assertThat(PaymentTransition.CREATE.applyTo(null)).isEqualTo(PaymentStatus.CREATED);
        assertThat(PaymentTransition.CREATE.eventType()).isEqualTo("payment.created");
        assertThat(PaymentTransition.CREATE.resultingSequence()).isEqualTo(1);
    }

    static Stream<Arguments> invalidTransitions() {
        Stream.Builder<Arguments> invalid = Stream.builder();
        for (PaymentTransition transition : PaymentTransition.values()) {
            for (PaymentStatus status : EnumSet.allOf(PaymentStatus.class)) {
                if (transition.from() != status) {
                    invalid.add(Arguments.of(transition, status));
                }
            }
        }
        return invalid.build();
    }

    @ParameterizedTest(name = "{0} from {1} is rejected")
    @MethodSource("invalidTransitions")
    void everyOtherCombinationIsRejected(PaymentTransition transition, PaymentStatus from) {
        assertThat(transition.isAllowedFrom(from)).isFalse();
        assertThatThrownBy(() -> transition.applyTo(from))
                .isInstanceOf(InvalidTransitionException.class)
                .hasMessageContaining(from.name());
    }

    @Test
    void lifecycleTransitionsRequireAnExistingPayment() {
        for (PaymentTransition transition : EnumSet.of(PaymentTransition.CONFIRM, PaymentTransition.EXPIRE, PaymentTransition.REFUND)) {
            assertThat(transition.isAllowedFrom(null)).isFalse();
        }
    }

    @Test
    void terminalStatusesAllowNothing() {
        for (PaymentStatus terminal : EnumSet.of(PaymentStatus.EXPIRED, PaymentStatus.REFUNDED)) {
            assertThat(PaymentTransition.values()).noneMatch(t -> t.isAllowedFrom(terminal));
        }
    }
}
