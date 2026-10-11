package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import io.github.jad_salem.pulse.schemas.Event;
import io.github.jad_salem.pulse.schemas.Product;
import java.time.Instant;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Joins events with the product catalog. The catalog (a compacted topic) is broadcast to every
 * parallel instance and kept in broadcast state, which is checkpointed with the job.
 */
public class ProductEnricher extends BroadcastProcessFunction<Event, Product, EnrichedEvent> {
    public static final String UNKNOWN_CATEGORY = "unknown";
    public static final MapStateDescriptor<String, Product> CATALOG =
            new MapStateDescriptor<>("product-catalog", Types.STRING, TypeInformation.of(Product.class));

    private transient Counter catalogMisses;

    @Override
    public void open(OpenContext openContext) {
        catalogMisses = getRuntimeContext().getMetricGroup().counter("catalogMisses");
    }

    @Override
    public void processElement(Event event, ReadOnlyContext ctx, Collector<EnrichedEvent> out) throws Exception {
        Product product = null;
        if (event.getProductId() != null) {
            product = ctx.getBroadcastState(CATALOG).get(event.getProductId());
            if (product == null) {
                catalogMisses.inc();
            }
        }
        out.collect(enrich(event, product, ctx.currentProcessingTime()));
    }

    @Override
    public void processBroadcastElement(Product product, Context ctx, Collector<EnrichedEvent> out) throws Exception {
        ctx.getBroadcastState(CATALOG).put(product.getProductId(), product);
    }

    /** Pure mapping; {@code product} is null when the event has no product or it is not in the catalog. */
    public static EnrichedEvent enrich(Event event, Product product, long processedAtMillis) {
        boolean hasProduct = event.getProductId() != null;
        long revenue = 0L;
        if ("purchase".equals(event.getEventType())
                && event.getQuantity() != null
                && event.getUnitPriceCents() != null) {
            revenue = Math.multiplyExact(event.getQuantity().longValue(), event.getUnitPriceCents());
        }
        return EnrichedEvent.newBuilder()
                .setEventId(event.getEventId())
                .setEventType(event.getEventType())
                .setEventTime(event.getEventTime())
                .setReceivedAt(event.getReceivedAt())
                .setProcessedAt(Instant.ofEpochMilli(processedAtMillis))
                .setUserId(event.getUserId())
                .setSessionId(event.getSessionId())
                .setPageUrl(event.getPageUrl())
                .setReferrer(event.getReferrer())
                .setProductId(event.getProductId())
                .setProductName(product == null ? null : product.getName())
                .setProductCategory(hasProduct ? (product == null ? UNKNOWN_CATEGORY : product.getCategory()) : null)
                .setProductBrand(product == null ? null : product.getBrand())
                .setQuantity(event.getQuantity())
                .setUnitPriceCents(event.getUnitPriceCents())
                .setRevenueCents(revenue)
                .setOrderId(event.getOrderId())
                .setCountry(event.getCountry())
                .setDevice(event.getDevice())
                .setUserAgent(event.getUserAgent())
                .build();
    }
}
