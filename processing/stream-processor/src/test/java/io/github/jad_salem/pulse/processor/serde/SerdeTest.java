package io.github.jad_salem.pulse.processor.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.jad_salem.pulse.processor.model.DeadLetter;
import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class SerdeTest {
    /** Nothing listens here, so any registry lookup fails with a connection error. */
    private static final String UNREACHABLE_REGISTRY = "http://127.0.0.1:1";

    @Test
    void garbageBytesBecomeADecodeFailureWithoutTouchingTheRegistry() throws Exception {
        List<DecodeResult> out = decode("{\"not\":\"avro\"}".getBytes(StandardCharsets.UTF_8));

        assertThat(out).singleElement().satisfies(result -> {
            assertThat(result.succeeded()).isFalse();
            assertThat(result.error).isEqualTo("not in Confluent Avro wire format");
            assertThat(result.offset).isEqualTo(7L);
        });
    }

    @Test
    void tombstonesBecomeADecodeFailure() throws Exception {
        assertThat(decode(null)).singleElement().satisfies(r -> assertThat(r.error).contains("tombstone"));
    }

    @Test
    void registryOutageIsTransientAndFailsTheTaskInsteadOfDeadLettering() {
        byte[] wellFormedHeader = {0, 0, 0, 0, 42, 2, 4};
        assertThatThrownBy(() -> decode(wellFormedHeader))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("transient failure");
    }

    @Test
    void classifier() {
        assertThat(FailureClassifier.isTransient(new IOException("wrapped", new ConnectException("refused")))).isTrue();
        assertThat(FailureClassifier.isTransient(new IOException("x", new RestClientException("not found", 404, 40403))))
                .isFalse();
        assertThat(FailureClassifier.isTransient(new IOException("x", new RestClientException("boom", 503, 50301))))
                .isTrue();
        assertThat(FailureClassifier.isTransient(new ArrayIndexOutOfBoundsException(9))).isFalse();
    }

    @Test
    void deadLettersKeepThePayloadAndCarryContextInHeaders() {
        DecodeResult source = DecodeResult.failed("bad", "events.raw", 3, 99L, 1L, "k".getBytes(), new byte[] {1, 2});
        DeadLetter letter = DeadLetter.of(source, "decode", "bad", 1234L);

        ProducerRecord<byte[], byte[]> record = new DeadLetterSerializationSchema("events.dlq").serialize(letter, null, null);

        assertThat(record.topic()).isEqualTo("events.dlq");
        assertThat(record.value()).containsExactly(1, 2);
        assertThat(record.key()).isEqualTo("k".getBytes());
        assertThat(new String(record.headers().lastHeader("dlq.source.offset").value(), StandardCharsets.UTF_8))
                .isEqualTo("99");
        assertThat(new String(record.headers().lastHeader("dlq.stage").value(), StandardCharsets.UTF_8))
                .isEqualTo("decode");
    }

    private static List<DecodeResult> decode(byte[] value) throws Exception {
        RawEventDeserializationSchema schema = new RawEventDeserializationSchema(UNREACHABLE_REGISTRY);
        List<DecodeResult> out = new ArrayList<>();
        schema.deserialize(new ConsumerRecord<>("events.raw", 0, 7L, null, value), new ListCollector<>(out));
        return out;
    }

    private record ListCollector<T>(List<T> sink) implements Collector<T> {
        @Override
        public void collect(T record) {
            sink.add(record);
        }

        @Override
        public void close() {}
    }
}
