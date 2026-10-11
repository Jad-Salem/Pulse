package io.github.jad_salem.pulse.processor.serde;

import io.github.jad_salem.pulse.processor.model.DeadLetter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Writes a dead letter as the original key and value bytes, unchanged, with the failure context in
 * dlq.* headers. Keeping the payload byte-identical means a fixed consumer can replay it as-is.
 */
public class DeadLetterSerializationSchema implements KafkaRecordSerializationSchema<DeadLetter> {
    private final String topic;

    public DeadLetterSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DeadLetter letter, KafkaSinkContext context, Long timestamp) {
        List<Header> headers = List.of(
                header("dlq.stage", letter.stage),
                header("dlq.reason", letter.reason),
                header("dlq.source.topic", letter.sourceTopic),
                header("dlq.source.partition", Integer.toString(letter.sourcePartition)),
                header("dlq.source.offset", Long.toString(letter.sourceOffset)),
                header("dlq.failed.at", Long.toString(letter.failedAt)));
        byte[] value = letter.value == null ? new byte[0] : letter.value;
        return new ProducerRecord<>(topic, null, letter.failedAt, letter.key, value, headers);
    }

    private static Header header(String name, String value) {
        return new RecordHeader(name, (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
    }
}
