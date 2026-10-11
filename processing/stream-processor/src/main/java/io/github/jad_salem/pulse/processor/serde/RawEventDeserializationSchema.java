package io.github.jad_salem.pulse.processor.serde;

import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.github.jad_salem.pulse.schemas.Event;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Decodes events.raw (Confluent wire format: magic byte, schema id, Avro body) without ever failing
 * on a bad record. Poison records become {@link DecodeResult#failed} and continue to the DLQ;
 * transient failures (registry unreachable) are rethrown so the job retries instead of
 * mis-classifying good data.
 */
public class RawEventDeserializationSchema implements KafkaRecordDeserializationSchema<DecodeResult> {
    private static final byte MAGIC_BYTE = 0;
    private static final int HEADER_LENGTH = 5;

    private final ConfluentRegistryAvroDeserializationSchema<Event> avro;

    public RawEventDeserializationSchema(String schemaRegistryUrl) {
        this.avro = ConfluentRegistryAvroDeserializationSchema.forSpecific(Event.class, schemaRegistryUrl);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        avro.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<DecodeResult> out) throws IOException {
        byte[] value = record.value();
        if (value == null) {
            out.collect(failed(record, "null value (tombstone) on an event topic"));
            return;
        }
        if (value.length < HEADER_LENGTH || value[0] != MAGIC_BYTE) {
            out.collect(failed(record, "not in Confluent Avro wire format"));
            return;
        }
        try {
            Event event = avro.deserialize(value);
            out.collect(DecodeResult.decoded(
                    event, record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), value));
        } catch (Exception e) {
            if (FailureClassifier.isTransient(e)) {
                throw new IOException("transient failure decoding " + coordinates(record), e);
            }
            out.collect(failed(record, FailureClassifier.describe(e)));
        }
    }

    @Override
    public TypeInformation<DecodeResult> getProducedType() {
        return TypeInformation.of(DecodeResult.class);
    }

    private static DecodeResult failed(ConsumerRecord<byte[], byte[]> record, String reason) {
        return DecodeResult.failed(
                reason, record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value());
    }

    private static String coordinates(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
