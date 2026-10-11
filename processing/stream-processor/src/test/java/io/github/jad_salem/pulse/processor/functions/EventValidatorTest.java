package io.github.jad_salem.pulse.processor.functions;

import static io.github.jad_salem.pulse.processor.TestEvents.T0;
import static io.github.jad_salem.pulse.processor.TestEvents.event;
import static io.github.jad_salem.pulse.processor.TestEvents.purchase;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.jad_salem.pulse.schemas.Event;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventValidatorTest {
    private static final Duration SKEW = Duration.ofMinutes(5);

    @Test
    void acceptsValidEvents() {
        assertThat(EventValidator.validate(event("page_view", "u", "s", T0).build(), SKEW)).isEmpty();
        assertThat(EventValidator.validate(purchase("u", "s", "o-1", T0, 2, 999), SKEW)).isEmpty();
    }

    @Test
    void rejectsUnknownTypesAndDevices() {
        Event event = event("signup", "u", "s", T0).setDevice("console").build();
        assertThat(EventValidator.validate(event, SKEW))
                .hasValueSatisfying(reason -> assertThat(reason)
                        .contains("unknown event_type 'signup'")
                        .contains("unknown device 'console'"));
    }

    @Test
    void requiresCommerceFieldsByType() {
        assertThat(EventValidator.validate(event("product_view", "u", "s", T0).build(), SKEW))
                .hasValueSatisfying(r -> assertThat(r).contains("product_view requires product_id"));

        Event noOrder = event("purchase", "u", "s", T0).setProductId("P1").setQuantity(1).setUnitPriceCents(5L).build();
        assertThat(EventValidator.validate(noOrder, SKEW))
                .hasValueSatisfying(r -> assertThat(r).isEqualTo("purchase requires order_id"));

        Event badQuantity = event("add_to_cart", "u", "s", T0).setProductId("P1").setQuantity(0).setUnitPriceCents(5L).build();
        assertThat(EventValidator.validate(badQuantity, SKEW))
                .hasValueSatisfying(r -> assertThat(r).contains("quantity in 1..1000"));
    }

    @Test
    void rejectsMalformedIdentifiers() {
        Event event = event("page_view", " ", "s", T0).setEventId("1-1-1-1-1").setCountry("usa").build();
        assertThat(EventValidator.validate(event, SKEW))
                .hasValueSatisfying(r -> assertThat(r)
                        .contains("event_id is not a UUID")
                        .contains("user_id is blank")
                        .contains("country is not ISO 3166-1 alpha-2"));
    }

    @Test
    void rejectsClientClocksFarAhead() {
        Event event = event("page_view", "u", "s", T0)
                .setReceivedAt(Instant.ofEpochMilli(T0 - Duration.ofMinutes(6).toMillis()))
                .build();
        assertThat(EventValidator.validate(event, SKEW))
                .hasValueSatisfying(r -> assertThat(r).contains("after received_at"));
    }
}
