package io.github.jad_salem.pulse.processor.functions;

import static io.github.jad_salem.pulse.processor.TestEvents.T0;
import static io.github.jad_salem.pulse.processor.TestEvents.event;
import static io.github.jad_salem.pulse.processor.TestEvents.purchase;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.jad_salem.pulse.processor.model.SessionAccumulator;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import io.github.jad_salem.pulse.schemas.Event;
import io.github.jad_salem.pulse.schemas.SessionSummary;
import org.junit.jupiter.api.Test;

class SessionAggregatorTest {
    private final SessionAggregator aggregator = new SessionAggregator();

    private static EnrichedEvent enriched(Event event) {
        return ProductEnricher.enrich(event, null, event.getEventTime().toEpochMilli());
    }

    @Test
    void summarizesASession() {
        SessionAccumulator acc = aggregator.createAccumulator();
        acc = aggregator.add(enriched(event("page_view", "u", "s", T0 + 1000).setPageUrl("/second").build()), acc);
        acc = aggregator.add(enriched(event("page_view", "u", "s", T0).setPageUrl("/landing").build()), acc);
        acc = aggregator.add(enriched(purchase("u", "s", "o1", T0 + 5000, 2, 500)), acc);

        SessionSummary summary = SessionSummaryFunction.toSummary(aggregator.getResult(acc));

        assertThat(summary.getLandingPage()).isEqualTo("/landing");
        assertThat(summary.getDurationMs()).isEqualTo(5000L);
        assertThat(summary.getEventCount()).isEqualTo(3);
        assertThat(summary.getPageViews()).isEqualTo(2);
        assertThat(summary.getPurchases()).isEqualTo(1);
        assertThat(summary.getRevenueCents()).isEqualTo(1000L);
        assertThat(summary.getConverted()).isTrue();
    }

    @Test
    void mergeKeepsTheEarliestLandingPageAndSumsCounters() {
        SessionAccumulator late = aggregator.add(
                enriched(event("page_view", "u", "s", T0 + 60_000).setPageUrl("/later").build()),
                aggregator.createAccumulator());
        SessionAccumulator early = aggregator.add(
                enriched(event("page_view", "u", "s", T0).setPageUrl("/first").build()),
                aggregator.createAccumulator());

        SessionAccumulator merged = aggregator.merge(late, early);

        assertThat(merged.landingPage).isEqualTo("/first");
        assertThat(merged.firstEventTime).isEqualTo(T0);
        assertThat(merged.lastEventTime).isEqualTo(T0 + 60_000);
        assertThat(merged.eventCount).isEqualTo(2);
        assertThat(SessionSummaryFunction.toSummary(merged).getConverted()).isFalse();
    }
}
