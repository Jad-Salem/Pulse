package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.schemas.Alert;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Card-testing / bot heuristic: raise an alert when one user places {@code threshold} or more
 * distinct orders within {@code window} of event time. Keyed by user_id.
 *
 * <p>State per user: order_id -> earliest event time, plus the end of the last alerted burst for
 * cooldown. Event-time timers evict orders once they can no longer be part of a burst, so state is
 * bounded and correct under out-of-order arrival.
 */
public class PurchaseVelocityDetector extends KeyedProcessFunction<String, EnrichedEvent, Alert> {
    public static final String ALERT_TYPE = "purchase_velocity";

    private final int threshold;
    private final long windowMillis;
    private final long cooldownMillis;

    private transient MapState<String, Long> orders;
    private transient ValueState<Long> lastAlertEnd;

    public PurchaseVelocityDetector(int threshold, Duration window, Duration cooldown) {
        if (threshold < 2) {
            throw new IllegalArgumentException("threshold must be at least 2");
        }
        this.threshold = threshold;
        this.windowMillis = window.toMillis();
        this.cooldownMillis = cooldown.toMillis();
    }

    @Override
    public void open(OpenContext openContext) {
        orders = getRuntimeContext().getMapState(new MapStateDescriptor<>("orders", Types.STRING, Types.LONG));
        lastAlertEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("last-alert-end", Types.LONG));
    }

    @Override
    public void processElement(EnrichedEvent event, Context ctx, Collector<Alert> out) throws Exception {
        if (!"purchase".equals(event.getEventType()) || event.getOrderId() == null) {
            return;
        }
        long time = event.getEventTime().toEpochMilli();
        Long known = orders.get(event.getOrderId());
        if (known != null && known <= time) {
            return; // another line of an order we already counted
        }
        orders.put(event.getOrderId(), time);
        ctx.timerService().registerEventTimeTimer(time + windowMillis + cooldownMillis);

        List<Long> times = new ArrayList<>();
        for (Map.Entry<String, Long> entry : orders.entries()) {
            times.add(entry.getValue());
        }
        long[] burst = densestBurstContaining(times, time);
        int count = (int) burst[2];
        if (count < threshold) {
            return;
        }
        Long previous = lastAlertEnd.value();
        if (previous != null && Math.abs(burst[1] - previous) < cooldownMillis) {
            return;
        }
        lastAlertEnd.update(burst[1]);
        out.collect(alert(ctx.getCurrentKey(), count, burst[0], burst[1], ctx.timerService().currentProcessingTime()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        long horizon = timestamp - windowMillis - cooldownMillis;
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, Long> entry : orders.entries()) {
            if (entry.getValue() <= horizon) {
                expired.add(entry.getKey());
            }
        }
        for (String orderId : expired) {
            orders.remove(orderId);
        }
        if (orders.isEmpty()) {
            Long previous = lastAlertEnd.value();
            if (previous == null || previous <= horizon) {
                lastAlertEnd.clear();
            }
        }
    }

    /**
     * The window of length {@code windowMillis} that contains {@code anchor} and the most order
     * times. Returns {start, end, count}.
     */
    long[] densestBurstContaining(List<Long> times, long anchor) {
        Collections.sort(times);
        long[] best = {anchor, anchor, 0};
        int left = 0;
        for (int right = 0; right < times.size(); right++) {
            while (times.get(right) - times.get(left) > windowMillis) {
                left++;
            }
            long start = times.get(left);
            long end = times.get(right);
            int count = right - left + 1;
            if (start <= anchor && anchor <= end && count > best[2]) {
                best = new long[] {start, end, count};
            }
        }
        return best;
    }

    private Alert alert(String userId, int count, long start, long end, long detectedAt) {
        String identity = ALERT_TYPE + "|" + userId + "|" + start + "|" + end;
        return Alert.newBuilder()
                .setAlertId(UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString())
                .setAlertType(ALERT_TYPE)
                .setSeverity(count >= 2 * threshold ? "critical" : "warning")
                .setEntityId(userId)
                .setDescription(String.format(
                        "%d orders in %.0f seconds (threshold %d per %d seconds)",
                        count, (end - start) / 1000.0, threshold, windowMillis / 1000))
                .setObservedValue(count)
                .setThreshold(threshold)
                .setWindowStart(Instant.ofEpochMilli(start))
                .setWindowEnd(Instant.ofEpochMilli(end))
                .setDetectedAt(Instant.ofEpochMilli(detectedAt))
                .build();
    }
}
