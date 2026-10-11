package io.github.jad_salem.pulse.processor.functions;

import static io.github.jad_salem.pulse.processor.TestEvents.T0;
import static io.github.jad_salem.pulse.processor.TestEvents.decoded;
import static io.github.jad_salem.pulse.processor.TestEvents.enrichedPurchase;
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
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

/** Each operator in isolation, driven by Flink's operator test harnesses (real state, real timers). */
class OperatorHarnessTest {

    @Test
    void triageRoutesDecodeAndContractFailuresToDeadLetters() throws Exception {
        try (OneInputStreamOperatorTestHarness<DecodeResult, Event> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new TriageFunction(Duration.ofMinutes(5)))) {
            Event valid = pageView("u1", "s1", T0);
            Event invalid = event("product_view", "u1", "s1", T0).build(); // no product_id
            DecodeResult garbage = DecodeResult.failed("not in Confluent Avro wire format", "events.raw", 2, 42L, T0, null, new byte[] {7});

            harness.processElement(decoded(valid), T0);
            harness.processElement(decoded(invalid), T0);
            harness.processElement(garbage, T0);

            assertThat(harness.extractOutputValues()).containsExactly(valid);
            List<DeadLetter> letters = harness.getSideOutput(TriageFunction.DEAD_LETTERS).stream()
                    .map(StreamRecord::getValue)
                    .toList();
            assertThat(letters).extracting(l -> l.stage).containsExactly("validate", "decode");
            assertThat(letters.get(0).reason).contains("requires product_id");
            assertThat(letters.get(1).sourcePartition).isEqualTo(2);
            assertThat(letters.get(1).sourceOffset).isEqualTo(42L);
            assertThat(letters.get(1).value).containsExactly(7);
        }
    }

    @Test
    void deduplicatorDropsRepeatedEventIds() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, Event> harness =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new Deduplicator(Duration.ofHours(1)), Event::getEventId, Types.STRING)) {
            Event first = pageView("u1", "s1", T0);
            Event retry = Event.newBuilder(first).build();
            Event other = pageView("u1", "s1", T0 + 1);

            harness.processElement(first, T0);
            harness.processElement(retry, T0 + 5);
            harness.processElement(other, T0 + 10);

            assertThat(harness.extractOutputValues()).containsExactly(first, other);
        }
    }

    @Test
    void enricherJoinsCatalogAndComputesRevenue() throws Exception {
        try (BroadcastOperatorTestHarness<Event, Product, EnrichedEvent> harness =
                ProcessFunctionTestHarnesses.forBroadcastProcessFunction(new ProductEnricher(), ProductEnricher.CATALOG)) {
            harness.processBroadcastElement(product("P0001", "books"), T0);

            harness.processElement(purchase("u1", "s1", "o1", T0, 3, 1250), T0);
            harness.processElement(event("product_view", "u1", "s1", T0).setProductId("P9999").build(), T0);
            harness.processElement(pageView("u1", "s1", T0), T0);

            List<EnrichedEvent> out = harness.extractOutputValues();
            assertThat(out.get(0).getProductCategory()).isEqualTo("books");
            assertThat(out.get(0).getProductName()).isEqualTo("Product P0001");
            assertThat(out.get(0).getRevenueCents()).isEqualTo(3750L);
            assertThat(out.get(1).getProductCategory()).isEqualTo(ProductEnricher.UNKNOWN_CATEGORY);
            assertThat(out.get(1).getRevenueCents()).isZero();
            assertThat(out.get(2).getProductCategory()).isNull();
        }
    }

    @Test
    void velocityDetectorAlertsOncePerBurstAndCountsOrdersNotLines() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, Alert> harness = velocityHarness()) {
            harness.processElement(enrichedPurchase("u1", "o1", T0), T0);
            harness.processElement(enrichedPurchase("u1", "o1", T0 + 100), T0 + 100); // 2nd line, same order
            harness.processElement(enrichedPurchase("u1", "o2", T0 + 10_000), T0 + 10_000);
            assertThat(harness.extractOutputValues()).isEmpty();

            harness.processElement(enrichedPurchase("u1", "o3", T0 + 20_000), T0 + 20_000);
            harness.processElement(enrichedPurchase("u1", "o4", T0 + 30_000), T0 + 30_000); // same burst: cooldown

            List<Alert> alerts = harness.extractOutputValues();
            assertThat(alerts).hasSize(1);
            Alert alert = alerts.get(0);
            assertThat(alert.getEntityId()).isEqualTo("u1");
            assertThat(alert.getObservedValue()).isEqualTo(3.0);
            assertThat(alert.getWindowStart().toEpochMilli()).isEqualTo(T0);
            assertThat(alert.getWindowEnd().toEpochMilli()).isEqualTo(T0 + 20_000);
        }
    }

    @Test
    void velocityDetectorIgnoresOrdersSpreadBeyondTheWindowAndEvictsState() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, Alert> harness = velocityHarness()) {
            harness.processElement(enrichedPurchase("u2", "o1", T0), T0);
            harness.processElement(enrichedPurchase("u2", "o2", T0 + 45_000), T0 + 45_000);
            harness.processElement(enrichedPurchase("u2", "o3", T0 + 90_000), T0 + 90_000);
            assertThat(harness.extractOutputValues()).isEmpty();

            harness.processWatermark(T0 + Duration.ofHours(1).toMillis());
            assertThat(harness.numKeyedStateEntries()).isZero();
        }
    }

    @Test
    void alertIdsAreDeterministic() throws Exception {
        String first;
        try (KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, Alert> harness = velocityHarness()) {
            for (int i = 0; i < 3; i++) {
                harness.processElement(enrichedPurchase("u3", "o" + i, T0 + i * 1000L), T0);
            }
            first = harness.extractOutputValues().get(0).getAlertId();
        }
        try (KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, Alert> replay = velocityHarness()) {
            for (int i = 0; i < 3; i++) {
                replay.processElement(enrichedPurchase("u3", "o" + i, T0 + i * 1000L), T0);
            }
            assertThat(replay.extractOutputValues().get(0).getAlertId()).isEqualTo(first);
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, EnrichedEvent, Alert> velocityHarness()
            throws Exception {
        return ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new PurchaseVelocityDetector(3, Duration.ofMinutes(1), Duration.ofMinutes(5)),
                EnrichedEvent::getUserId,
                Types.STRING);
    }
}
