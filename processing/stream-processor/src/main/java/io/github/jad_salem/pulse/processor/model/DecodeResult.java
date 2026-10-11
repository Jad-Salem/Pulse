package io.github.jad_salem.pulse.processor.model;

import io.github.jad_salem.pulse.schemas.Event;

/**
 * Outcome of decoding one Kafka record: either a decoded {@link Event} or the reason it could not
 * be decoded. The original record coordinates and bytes travel along, so a failure can be written
 * to the dead-letter topic verbatim and replayed later.
 *
 * <p>Public fields and a no-arg constructor make this a Flink POJO (efficient serialization).
 */
public class DecodeResult {
    public Event event;
    public String error;
    public String topic;
    public int partition;
    public long offset;
    public long kafkaTimestamp;
    public byte[] key;
    public byte[] value;

    public DecodeResult() {}

    public static DecodeResult decoded(
            Event event, String topic, int partition, long offset, long kafkaTimestamp, byte[] key, byte[] value) {
        DecodeResult result = new DecodeResult();
        result.event = event;
        result.topic = topic;
        result.partition = partition;
        result.offset = offset;
        result.kafkaTimestamp = kafkaTimestamp;
        result.key = key;
        result.value = value;
        return result;
    }

    public static DecodeResult failed(
            String error, String topic, int partition, long offset, long kafkaTimestamp, byte[] key, byte[] value) {
        DecodeResult result = decoded(null, topic, partition, offset, kafkaTimestamp, key, value);
        result.error = error;
        return result;
    }

    public boolean succeeded() {
        return event != null;
    }
}
