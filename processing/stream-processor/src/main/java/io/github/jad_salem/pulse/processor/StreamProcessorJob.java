package io.github.jad_salem.pulse.processor;

import io.github.jad_salem.pulse.processor.model.DeadLetter;
import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.github.jad_salem.pulse.processor.serde.DeadLetterSerializationSchema;
import io.github.jad_salem.pulse.processor.serde.ProductDeserializationSchema;
import io.github.jad_salem.pulse.processor.serde.RawEventDeserializationSchema;
import io.github.jad_salem.pulse.schemas.Alert;
import io.github.jad_salem.pulse.schemas.EnrichedEvent;
import io.github.jad_salem.pulse.schemas.Product;
import io.github.jad_salem.pulse.schemas.SessionSummary;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Entry point. Wires {@link PulseTopology} to Kafka with exactly-once sinks: every output is
 * written in a Kafka transaction that commits with the Flink checkpoint, so downstream
 * read_committed consumers (ClickHouse) never see a record twice or a record from a failed attempt.
 *
 * <p>Checkpoint interval, state backend (RocksDB) and restart strategy come from the cluster
 * configuration, not from code.
 */
public final class StreamProcessorJob {
    /** Must stay below the broker's transaction.max.timeout.ms (15 minutes by default). */
    private static final String TRANSACTION_TIMEOUT_MS = "900000";

    private StreamProcessorJob() {}

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment(System.getenv());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DecodeResult> raw = env.fromSource(
                        eventSource(config), PulseTopology.eventWatermarks(config.processing()), "events.raw")
                .uid("source-events-raw");
        DataStream<Product> catalog = env.fromSource(
                        catalogSource(config), PulseTopology.catalogWatermarks(), "products")
                .uid("source-products");

        PulseTopology.Outputs outputs = PulseTopology.build(raw, catalog, config.processing());

        outputs.enriched()
                .sinkTo(avroSink(config, config.topics().enriched(), EnrichedEvent.class, EnrichedEvent::getUserId))
                .uid("sink-enriched")
                .name("events.enriched");
        outputs.sessions()
                .sinkTo(avroSink(config, config.topics().sessions(), SessionSummary.class, SessionSummary::getSessionId))
                .uid("sink-sessions")
                .name("sessions");
        outputs.alerts()
                .sinkTo(avroSink(config, config.topics().alerts(), Alert.class, Alert::getEntityId))
                .uid("sink-alerts")
                .name("alerts");
        outputs.deadLetters()
                .sinkTo(deadLetterSink(config))
                .uid("sink-dead-letters")
                .name("events.dlq");

        env.execute("pulse-stream-processor");
    }

    @SuppressWarnings("deprecation") // OffsetResetStrategy is still the connector's API in 5.0
    private static KafkaSource<DecodeResult> eventSource(JobConfig config) {
        return KafkaSource.<DecodeResult>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.topics().raw())
                .setGroupId(config.consumerGroup())
                // Resume from committed offsets (committed on checkpoint); a brand-new group starts at the beginning.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new RawEventDeserializationSchema(config.schemaRegistryUrl()))
                .setProperty("partition.discovery.interval.ms", "60000")
                .setClientIdPrefix("pulse-events")
                .build();
    }

    private static KafkaSource<Product> catalogSource(JobConfig config) {
        return KafkaSource.<Product>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.topics().products())
                .setGroupId(config.consumerGroup() + "-catalog")
                // Always load the full compacted catalog on a fresh start.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new ProductDeserializationSchema(config.schemaRegistryUrl()))
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();
    }

    private static <T extends SpecificRecord> KafkaSink<T> avroSink(
            JobConfig config, String topic, Class<T> type, SerializableKey<T> key) {
        KafkaRecordSerializationSchema<T> serializer = KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setKeySerializationSchema(key)
                .setValueSerializationSchema(
                        ConfluentRegistryAvroSerializationSchema.forSpecific(type, topic + "-value", config.schemaRegistryUrl()))
                .build();
        return KafkaSink.<T>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("pulse-" + topic)
                .setKafkaProducerConfig(producerConfig())
                .build();
    }

    private static KafkaSink<DeadLetter> deadLetterSink(JobConfig config) {
        return KafkaSink.<DeadLetter>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new DeadLetterSerializationSchema(config.topics().deadLetters()))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("pulse-" + config.topics().deadLetters())
                .setKafkaProducerConfig(producerConfig())
                .build();
    }

    private static Properties producerConfig() {
        Properties props = new Properties();
        props.setProperty("transaction.timeout.ms", TRANSACTION_TIMEOUT_MS);
        props.setProperty("compression.type", "zstd");
        props.setProperty("linger.ms", "20");
        return props;
    }

    /** Record key as UTF-8 bytes; a serializable function so Flink can ship it to the cluster. */
    @FunctionalInterface
    interface SerializableKey<T> extends SerializationSchema<T>, Function<T, String> {
        @Override
        default byte[] serialize(T element) {
            return apply(element).getBytes(StandardCharsets.UTF_8);
        }
    }
}
