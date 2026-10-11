package io.github.jad_salem.pulse.processor;

import io.github.jad_salem.pulse.processor.functions.ProductEnricher;
import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import io.github.jad_salem.pulse.schemas.Event;
import io.github.jad_salem.pulse.schemas.Product;
import java.time.Instant;
import java.util.UUID;

/** Builders for test records. Times are epoch millis relative to a fixed base. */
public final class TestEvents {
    public static final long T0 = Instant.parse("2026-01-01T12:00:00Z").toEpochMilli();

    private TestEvents() {}

    public static Event.Builder event(String type, String userId, String sessionId, long time) {
        return Event.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(type)
                .setEventTime(Instant.ofEpochMilli(time))
                .setReceivedAt(Instant.ofEpochMilli(time + 50))
                .setUserId(userId)
                .setSessionId(sessionId)
                .setPageUrl("https://shop.example.com/")
                .setCountry("US")
                .setDevice("desktop");
    }

    public static Event pageView(String userId, String sessionId, long time) {
        return event("page_view", userId, sessionId, time).build();
    }

    public static Event purchase(String userId, String sessionId, String orderId, long time, int qty, long price) {
        return event("purchase", userId, sessionId, time)
                .setProductId("P0001")
                .setQuantity(qty)
                .setUnitPriceCents(price)
                .setOrderId(orderId)
                .build();
    }

    public static EnrichedEvent enrichedPurchase(String userId, String orderId, long time) {
        return ProductEnricher.enrich(purchase(userId, "s-" + userId, orderId, time, 1, 1000), null, time);
    }

    public static DecodeResult decoded(Event event) {
        return DecodeResult.decoded(event, "events.raw", 0, 0L, event.getReceivedAt().toEpochMilli(), null, new byte[] {0});
    }

    public static Product product(String id, String category) {
        return Product.newBuilder()
                .setProductId(id)
                .setName("Product " + id)
                .setCategory(category)
                .setBrand("Acme")
                .setListPriceCents(1999)
                .setUpdatedAt(Instant.ofEpochMilli(T0))
                .build();
    }

}
