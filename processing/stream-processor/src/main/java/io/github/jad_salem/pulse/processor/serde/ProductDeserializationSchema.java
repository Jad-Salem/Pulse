package io.github.jad_salem.pulse.processor.serde;

import io.github.jad_salem.pulse.schemas.Product;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decodes the compacted products topic. Tombstones and undecodable records are skipped. */
public class ProductDeserializationSchema implements KafkaRecordDeserializationSchema<Product> {
    private static final Logger LOG = LoggerFactory.getLogger(ProductDeserializationSchema.class);

    private final ConfluentRegistryAvroDeserializationSchema<Product> avro;

    public ProductDeserializationSchema(String schemaRegistryUrl) {
        this.avro = ConfluentRegistryAvroDeserializationSchema.forSpecific(Product.class, schemaRegistryUrl);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        avro.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Product> out) throws IOException {
        if (record.value() == null) {
            return;
        }
        try {
            out.collect(avro.deserialize(record.value()));
        } catch (Exception e) {
            if (FailureClassifier.isTransient(e)) {
                throw new IOException("transient failure decoding product record", e);
            }
            LOG.warn("Skipping undecodable product record at offset {}: {}", record.offset(), FailureClassifier.describe(e));
        }
    }

    @Override
    public TypeInformation<Product> getProducedType() {
        return TypeInformation.of(Product.class);
    }
}
