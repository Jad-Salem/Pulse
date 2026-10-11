package io.github.jad_salem.pulse.processor;

import static io.github.jad_salem.pulse.processor.TestEvents.T0;
import static io.github.jad_salem.pulse.processor.TestEvents.decoded;
import static io.github.jad_salem.pulse.processor.TestEvents.event;
import static io.github.jad_salem.pulse.processor.TestEvents.pageView;
import static io.github.jad_salem.pulse.processor.TestEvents.product;
import static io.github.jad_salem.pulse.processor.TestEvents.purchase;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.jad_salem.pulse.processor.model.DeadLetter;
import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.github.jad_salem.pulse.schemas.Alert;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import io.github.jad_salem.pulse.schemas.Event;
import io.github.jad_salem.pulse.schemas.Product;
import io.github.jad_salem.pulse.schemas.SessionSummary;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The full graph on an embedded Flink cluster with bounded in-memory input. When the input ends,
 * Flink emits a final watermark, so every session window closes and every timer fires.
 */
class PulseTopologyTest {
    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder()
            .setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(2)
            .build());

    private static final JobConfig.Processing CONFIG = new JobConfig.Processing(
            Duration.ofSeconds(5),
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofHours(1),
            Duration.ofMinutes(30),
            3,
            Duration.ofMinutes(1),
            Duration.ofMinutes(5));

    private static List<DecodeResult> scenario() {
        List<Event> events = new ArrayList<>();
        // Session A: browses, buys, and the client retries the purchase (duplicate event_id).
        events.add(pageView("alice", "A", T0));
        events.add(event("product_view", "alice", "A", T0 + 10_000).setProductId("P0001").build());
        Event buy = purchase("alice", "A", "order-a", T0 + 20_000, 2, 1500);
        events.add(buy);
        events.add(Event.newBuilder(buy).build());
        // Session B: a bot placing three orders in 20 seconds.
        for (int i = 0; i < 3; i++) {
            events.add(purchase("bot", "B", "order-b" + i, T0 + i * 10_000L, 1, 100));
        }
        // Session C: one page view, then silence (a bounce).
        events.add(pageView("carol", "C", T0 + 5_000));

        List<DecodeResult> input = new ArrayList<>();
        events.forEach(e -> input.add(decoded(e)));
        // Contract violation: purchase without an order_id.
        input.add(decoded(event("purchase", "mallory", "M", T0).setProductId("P1").setQuantity(1).setUnitPriceCents(1L).build()));
        // Undecodable bytes.
        input.add(DecodeResult.failed("not in Confluent Avro wire format", "events.raw", 0, 99L, T0, null, new byte[] {9}));
        return input;
    }

    private static <T> List<T> run(Function<PulseTopology.Outputs, DataStream<T>> output) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<DecodeResult> raw = env.fromData(scenario(), TypeInformation.of(DecodeResult.class))
                .assignTimestampsAndWatermarks(PulseTopology.eventWatermarks(CONFIG));
        DataStream<Product> catalog = env.fromData(List.of(product("P0001", "books")), TypeInformation.of(Product.class))
                .assignTimestampsAndWatermarks(PulseTopology.catalogWatermarks());

        List<T> results = new ArrayList<>();
        try (CloseableIterator<T> it = output.apply(PulseTopology.build(raw, catalog, CONFIG)).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }
        return results;
    }

    @Test
    void duplicatesAreRemovedAndRevenueIsComputed() throws Exception {
        List<EnrichedEvent> enriched = run(PulseTopology.Outputs::enriched);

        assertThat(enriched).hasSize(7); // 9 events - 1 duplicate - 1 invalid
        assertThat(enriched).filteredOn(e -> e.getOrderId() != null && e.getOrderId().equals("order-a"))
                .singleElement()
                .satisfies(e -> assertThat(e.getRevenueCents()).isEqualTo(3000L));
        assertThat(enriched).allSatisfy(e -> assertThat(e.getProcessedAt()).isAfter(Instant.ofEpochMilli(T0)));
    }

    @Test
    void sessionsCloseWithCorrectCounters() throws Exception {
        List<SessionSummary> sessions = run(PulseTopology.Outputs::sessions);

        assertThat(sessions).extracting(SessionSummary::getSessionId).containsExactlyInAnyOrder("A", "B", "C");
        SessionSummary alice = sessions.stream().filter(s -> s.getSessionId().equals("A")).findFirst().orElseThrow();
        assertThat(alice.getEventCount()).isEqualTo(3);
        assertThat(alice.getPurchases()).isEqualTo(1);
        assertThat(alice.getRevenueCents()).isEqualTo(3000L);
        assertThat(alice.getConverted()).isTrue();
        assertThat(alice.getDurationMs()).isEqualTo(20_000L);
        SessionSummary carol = sessions.stream().filter(s -> s.getSessionId().equals("C")).findFirst().orElseThrow();
        assertThat(carol.getEventCount()).isEqualTo(1);
        assertThat(carol.getConverted()).isFalse();
    }

    @Test
    void purchaseBurstRaisesExactlyOneAlert() throws Exception {
        List<Alert> alerts = run(PulseTopology.Outputs::alerts);

        assertThat(alerts).singleElement().satisfies(alert -> {
            assertThat(alert.getEntityId()).isEqualTo("bot");
            assertThat(alert.getObservedValue()).isEqualTo(3.0);
        });
    }

    @Test
    void badRecordsReachTheDeadLetterOutput() throws Exception {
        List<DeadLetter> letters = run(PulseTopology.Outputs::deadLetters);

        assertThat(letters).extracting(l -> l.stage).containsExactlyInAnyOrder("validate", "decode");
        assertThat(letters).filteredOn(l -> l.stage.equals("decode"))
                .singleElement()
                .satisfies(l -> assertThat(l.sourceOffset).isEqualTo(99L));
    }
}
