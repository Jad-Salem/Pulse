package io.github.jad_salem.pulse.processor.model;

/** A record that failed processing, with enough context to inspect and replay it. Flink POJO. */
public class DeadLetter {
    public String stage;
    public String reason;
    public String sourceTopic;
    public int sourcePartition;
    public long sourceOffset;
    public long failedAt;
    public byte[] key;
    public byte[] value;

    public DeadLetter() {}

    public static DeadLetter of(DecodeResult source, String stage, String reason, long failedAt) {
        DeadLetter letter = new DeadLetter();
        letter.stage = stage;
        letter.reason = reason;
        letter.sourceTopic = source.topic;
        letter.sourcePartition = source.partition;
        letter.sourceOffset = source.offset;
        letter.failedAt = failedAt;
        letter.key = source.key;
        letter.value = source.value;
        return letter;
    }
}
