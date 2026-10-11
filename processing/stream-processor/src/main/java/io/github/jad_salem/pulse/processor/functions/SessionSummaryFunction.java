package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.processor.model.SessionAccumulator;
import io.github.jad_salem.pulse.schemas.SessionSummary;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Turns the final accumulator of a closed session window into a SessionSummary record. */
public class SessionSummaryFunction
        extends ProcessWindowFunction<SessionAccumulator, SessionSummary, String, TimeWindow> {

    @Override
    public void process(
            String sessionId, Context ctx, Iterable<SessionAccumulator> accumulators, Collector<SessionSummary> out) {
        for (SessionAccumulator acc : accumulators) {
            out.collect(toSummary(acc));
        }
    }

    static SessionSummary toSummary(SessionAccumulator acc) {
        return SessionSummary.newBuilder()
                .setSessionId(acc.sessionId)
                .setUserId(acc.userId)
                .setSessionStart(Instant.ofEpochMilli(acc.firstEventTime))
                .setSessionEnd(Instant.ofEpochMilli(acc.lastEventTime))
                .setDurationMs(acc.lastEventTime - acc.firstEventTime)
                .setEventCount(acc.eventCount)
                .setPageViews(acc.pageViews)
                .setProductViews(acc.productViews)
                .setAddToCarts(acc.addToCarts)
                .setCheckouts(acc.checkouts)
                .setPurchases(acc.purchases)
                .setRevenueCents(acc.revenueCents)
                .setConverted(acc.purchases > 0)
                .setCountry(acc.country)
                .setDevice(acc.device)
                .setLandingPage(acc.landingPage)
                .build();
    }
}
