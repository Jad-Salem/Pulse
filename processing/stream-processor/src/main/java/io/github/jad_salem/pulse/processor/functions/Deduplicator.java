package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.schemas.Event;
import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Drops repeated event_ids (client retries, at-least-once producers). Keyed by event_id; each key
 * holds one flag that expires after the TTL, so state stays proportional to the TTL's traffic.
 */
public class Deduplicator extends KeyedProcessFunction<String, Event, Event> {
    private final Duration ttl;
    private transient ValueState<Boolean> seen;
    private transient Counter duplicates;

    public Deduplicator(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        seen = getRuntimeContext().getState(descriptor);
        duplicates = getRuntimeContext().getMetricGroup().counter("duplicatesDropped");
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        if (seen.value() != null) {
            duplicates.inc();
            return;
        }
        seen.update(Boolean.TRUE);
        out.collect(event);
    }
}
