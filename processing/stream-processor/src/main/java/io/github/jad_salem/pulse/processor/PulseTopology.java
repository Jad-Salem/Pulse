package io.github.jad_salem.pulse.processor;

import io.github.jad_salem.pulse.processor.functions.Deduplicator;
import io.github.jad_salem.pulse.processor.functions.ProductEnricher;
import io.github.jad_salem.pulse.processor.functions.PurchaseVelocityDetector;
import io.github.jad_salem.pulse.processor.functions.SessionAggregator;
import io.github.jad_salem.pulse.processor.functions.SessionSummaryFunction;
import io.github.jad_salem.pulse.processor.functions.TriageFunction;
import io.github.jad_salem.pulse.processor.model.DeadLetter;
import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.github.jad_salem.pulse.schemas.Alert;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import io.github.jad_salem.pulse.schemas.Event;
import io.github.jad_salem.pulse.schemas.Product;
import io.github.jad_salem.pulse.schemas.SessionSummary;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;

/**
 * The processing graph, independent of sources and sinks, so tests can drive it with in-memory
 * streams and production wires it to Kafka.
 *
 * <pre>
 *   raw ──► triage ──► dedupe(event_id) ──► enrich(+catalog) ──┬──► enriched
 *            │                                                 ├──► sessionize(session_id) ──► sessions
 *            └──► dead letters                                 └──► velocity(user_id) ──► alerts
 * </pre>
 *
 * Every stateful operator has a stable uid, so savepoints survive code changes.
 */
public final class PulseTopology {
    private PulseTopology() {}

    public record Outputs(
            DataStream<EnrichedEvent> enriched,
            DataStream<SessionSummary> sessions,
            DataStream<Alert> alerts,
            DataStream<DeadLetter> deadLetters) {}

    public static Outputs build(
            DataStream<DecodeResult> raw, DataStream<Product> catalog, JobConfig.Processing config) {
        SingleOutputStreamOperator<Event> valid = raw
                .process(new TriageFunction(config.maxClockSkew()))
                .uid("triage")
                .name("Decode + validate");
        DataStream<DeadLetter> deadLetters = valid.getSideOutput(TriageFunction.DEAD_LETTERS);

        DataStream<Event> unique = valid
                .keyBy(Event::getEventId)
                .process(new Deduplicator(config.dedupTtl()))
                .uid("dedupe")
                .name("De-duplicate by event_id");

        DataStream<EnrichedEvent> enriched = unique
                .connect(catalog.broadcast(ProductEnricher.CATALOG))
                .process(new ProductEnricher())
                .uid("enrich")
                .name("Enrich with product catalog");

        DataStream<SessionSummary> sessions = enriched
                .keyBy(EnrichedEvent::getSessionId)
                .window(EventTimeSessionWindows.withGap(config.sessionGap()))
                .aggregate(new SessionAggregator(), new SessionSummaryFunction())
                .uid("sessionize")
                .name("Sessionize");

        DataStream<Alert> alerts = enriched
                .filter(event -> "purchase".equals(event.getEventType()))
                .name("Purchases only")
                .keyBy(EnrichedEvent::getUserId)
                .process(new PurchaseVelocityDetector(
                        config.velocityThreshold(), config.velocityWindow(), config.alertCooldown()))
                .uid("purchase-velocity")
                .name("Detect purchase velocity");

        return new Outputs(enriched, sessions, alerts, deadLetters);
    }

    /**
     * Event-time watermarks for decoded records. The timestamp is the client's event_time, but
     * clamped to received_at + max skew: one event with a wildly future clock must not drag the
     * watermark forward and make every other event "late". Undecodable records use the Kafka
     * record timestamp.
     */
    public static WatermarkStrategy<DecodeResult> eventWatermarks(JobConfig.Processing config) {
        long maxSkew = config.maxClockSkew().toMillis();
        return WatermarkStrategy.<DecodeResult>forBoundedOutOfOrderness(config.maxOutOfOrderness())
                .withTimestampAssigner((record, kafkaTimestamp) -> {
                    if (!record.succeeded()) {
                        return record.kafkaTimestamp;
                    }
                    long eventTime = record.event.getEventTime().toEpochMilli();
                    long ceiling = record.event.getReceivedAt().toEpochMilli() + maxSkew;
                    return Math.min(eventTime, ceiling);
                })
                .withIdleness(config.sourceIdleness());
    }

    /**
     * The catalog is reference data with no meaningful event time. It advertises a final watermark
     * so it never holds back the event-time clock of the stream it is connected to.
     */
    public static WatermarkStrategy<Product> catalogWatermarks() {
        return WatermarkStrategy.forGenerator(ctx -> new WatermarkGenerator<Product>() {
            @Override
            public void onEvent(Product product, long timestamp, WatermarkOutput output) {}

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(Long.MAX_VALUE));
            }
        });
    }
}
