package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.schemas.Event;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The events.raw validation contract (schemas/README.md). The collector enforces the same rules at
 * the edge; this copy protects the pipeline from producers that write to Kafka directly.
 */
public final class EventValidator {
    public static final Set<String> EVENT_TYPES = Set.of(
            "page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_started", "purchase");
    public static final Set<String> PRODUCT_EVENTS = Set.of("product_view", "add_to_cart", "remove_from_cart", "purchase");
    public static final Set<String> LINE_ITEM_EVENTS = Set.of("add_to_cart", "remove_from_cart", "purchase");
    public static final Set<String> DEVICES = Set.of("desktop", "mobile", "tablet", "unknown");

    private static final Pattern UUID = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    private static final Pattern COUNTRY = Pattern.compile("^[A-Z]{2}$");

    private EventValidator() {}

    /** Returns the reason the event violates the contract, or empty if it is valid. */
    public static Optional<String> validate(Event event, Duration maxClockSkew) {
        List<String> problems = new ArrayList<>();
        if (!UUID.matcher(event.getEventId()).matches()) {
            problems.add("event_id is not a UUID");
        }
        String type = event.getEventType();
        if (!EVENT_TYPES.contains(type)) {
            problems.add("unknown event_type '" + type + "'");
        }
        if (event.getUserId().isBlank()) {
            problems.add("user_id is blank");
        }
        if (event.getSessionId().isBlank()) {
            problems.add("session_id is blank");
        }
        if (event.getPageUrl().isBlank()) {
            problems.add("page_url is blank");
        }
        if (!COUNTRY.matcher(event.getCountry()).matches()) {
            problems.add("country is not ISO 3166-1 alpha-2");
        }
        if (!DEVICES.contains(event.getDevice())) {
            problems.add("unknown device '" + event.getDevice() + "'");
        }
        if (PRODUCT_EVENTS.contains(type) && isBlank(event.getProductId())) {
            problems.add(type + " requires product_id");
        }
        if (LINE_ITEM_EVENTS.contains(type)) {
            Integer quantity = event.getQuantity();
            Long price = event.getUnitPriceCents();
            if (quantity == null || quantity < 1 || quantity > 1000) {
                problems.add(type + " requires quantity in 1..1000");
            }
            if (price == null || price < 0) {
                problems.add(type + " requires unit_price_cents >= 0");
            }
        }
        if ("purchase".equals(type) && isBlank(event.getOrderId())) {
            problems.add("purchase requires order_id");
        }
        if (event.getEventTime().isAfter(event.getReceivedAt().plus(maxClockSkew))) {
            problems.add("event_time is more than " + maxClockSkew.toSeconds() + "s after received_at");
        }
        return problems.isEmpty() ? Optional.empty() : Optional.of(String.join("; ", problems));
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
