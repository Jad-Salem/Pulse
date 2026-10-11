package io.github.jad_salem.pulse.processor.functions;

import io.github.jad_salem.pulse.processor.model.DeadLetter;
import io.github.jad_salem.pulse.processor.model.DecodeResult;
import io.github.jad_salem.pulse.schemas.Event;
import java.time.Duration;
import java.util.Optional;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Routes undecodable and contract-violating records to the dead-letter side output. */
public class TriageFunction extends ProcessFunction<DecodeResult, Event> {
    public static final OutputTag<DeadLetter> DEAD_LETTERS =
            new OutputTag<>("dead-letters", TypeInformation.of(DeadLetter.class));

    private final Duration maxClockSkew;
    private transient Counter decodeFailures;
    private transient Counter validationFailures;

    public TriageFunction(Duration maxClockSkew) {
        this.maxClockSkew = maxClockSkew;
    }

    @Override
    public void open(OpenContext openContext) {
        decodeFailures = getRuntimeContext().getMetricGroup().counter("deadLettersDecode");
        validationFailures = getRuntimeContext().getMetricGroup().counter("deadLettersValidation");
    }

    @Override
    public void processElement(DecodeResult record, Context ctx, Collector<Event> out) {
        long now = ctx.timerService().currentProcessingTime();
        if (!record.succeeded()) {
            decodeFailures.inc();
            ctx.output(DEAD_LETTERS, DeadLetter.of(record, "decode", record.error, now));
            return;
        }
        Optional<String> violation = EventValidator.validate(record.event, maxClockSkew);
        if (violation.isPresent()) {
            validationFailures.inc();
            ctx.output(DEAD_LETTERS, DeadLetter.of(record, "validate", violation.get(), now));
            return;
        }
        out.collect(record.event);
    }
}
