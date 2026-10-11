package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.processor.model.SessionAccumulator;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incrementally aggregates a session window, so state is one accumulator per open session rather
 * than every event. {@link #merge} is required because session windows merge when a late event
 * bridges two of them.
 */
public class SessionAggregator implements AggregateFunction<EnrichedEvent, SessionAccumulator, SessionAccumulator> {

    @Override
    public SessionAccumulator createAccumulator() {
        return new SessionAccumulator();
    }

    @Override
    public SessionAccumulator add(EnrichedEvent event, SessionAccumulator acc) {
        long time = event.getEventTime().toEpochMilli();
        acc.sessionId = event.getSessionId();
        acc.userId = event.getUserId();
        if (time < acc.firstEventTime) {
            acc.firstEventTime = time;
            acc.landingPage = event.getPageUrl();
            acc.country = event.getCountry();
            acc.device = event.getDevice();
        }
        acc.lastEventTime = Math.max(acc.lastEventTime, time);
        acc.eventCount++;
        switch (event.getEventType()) {
            case "page_view" -> acc.pageViews++;
            case "product_view" -> acc.productViews++;
            case "add_to_cart" -> acc.addToCarts++;
            case "checkout_started" -> acc.checkouts++;
            case "purchase" -> acc.purchases++;
            default -> {}
        }
        acc.revenueCents += event.getRevenueCents();
        return acc;
    }

    @Override
    public SessionAccumulator getResult(SessionAccumulator acc) {
        return acc;
    }

    @Override
    public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
        SessionAccumulator earlier = a.firstEventTime <= b.firstEventTime ? a : b;
        SessionAccumulator merged = new SessionAccumulator();
        merged.sessionId = a.sessionId != null ? a.sessionId : b.sessionId;
        merged.userId = a.userId != null ? a.userId : b.userId;
        merged.firstEventTime = earlier.firstEventTime;
        merged.landingPage = earlier.landingPage;
        merged.country = earlier.country;
        merged.device = earlier.device;
        merged.lastEventTime = Math.max(a.lastEventTime, b.lastEventTime);
        merged.eventCount = a.eventCount + b.eventCount;
        merged.pageViews = a.pageViews + b.pageViews;
        merged.productViews = a.productViews + b.productViews;
        merged.addToCarts = a.addToCarts + b.addToCarts;
        merged.checkouts = a.checkouts + b.checkouts;
        merged.purchases = a.purchases + b.purchases;
        merged.revenueCents = a.revenueCents + b.revenueCents;
        return merged;
    }
}
