package io.github.jad_salem.pulse.processor;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/**
 * Job configuration, read from PULSE_* environment variables (twelve-factor style). Cluster
 * concerns (checkpointing, state backend, restart strategy) are deliberately not here: they belong
 * to the Flink cluster configuration so the same jar runs unchanged in every environment.
 */
public record JobConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String consumerGroup,
        Topics topics,
        Processing processing)
        implements Serializable {

    /** Topic names. */
    public record Topics(
            String raw, String products, String enriched, String sessions, String alerts, String deadLetters)
            implements Serializable {}

    /** Tunables of the processing topology. */
    public record Processing(
            Duration maxOutOfOrderness,
            Duration sourceIdleness,
            Duration maxClockSkew,
            Duration dedupTtl,
            Duration sessionGap,
            int velocityThreshold,
            Duration velocityWindow,
            Duration alertCooldown)
            implements Serializable {

        public static Processing defaults() {
            return new Processing(
                    Duration.ofSeconds(10),
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(5),
                    Duration.ofHours(1),
                    Duration.ofMinutes(30),
                    3,
                    Duration.ofMinutes(1),
                    Duration.ofMinutes(5));
        }
    }

    public static JobConfig fromEnvironment(Map<String, String> env) {
        Processing defaults = Processing.defaults();
        return new JobConfig(
                env.getOrDefault("PULSE_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
                env.getOrDefault("PULSE_SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                env.getOrDefault("PULSE_CONSUMER_GROUP", "stream-processor"),
                new Topics(
                        env.getOrDefault("PULSE_TOPIC_RAW", "events.raw"),
                        env.getOrDefault("PULSE_TOPIC_PRODUCTS", "products"),
                        env.getOrDefault("PULSE_TOPIC_ENRICHED", "events.enriched"),
                        env.getOrDefault("PULSE_TOPIC_SESSIONS", "sessions"),
                        env.getOrDefault("PULSE_TOPIC_ALERTS", "alerts"),
                        env.getOrDefault("PULSE_TOPIC_DLQ", "events.dlq")),
                new Processing(
                        seconds(env, "PULSE_MAX_OUT_OF_ORDERNESS_SECONDS", defaults.maxOutOfOrderness()),
                        seconds(env, "PULSE_SOURCE_IDLENESS_SECONDS", defaults.sourceIdleness()),
                        seconds(env, "PULSE_MAX_CLOCK_SKEW_SECONDS", defaults.maxClockSkew()),
                        seconds(env, "PULSE_DEDUP_TTL_SECONDS", defaults.dedupTtl()),
                        seconds(env, "PULSE_SESSION_GAP_SECONDS", defaults.sessionGap()),
                        Integer.parseInt(
                                env.getOrDefault(
                                        "PULSE_VELOCITY_THRESHOLD",
                                        Integer.toString(defaults.velocityThreshold()))),
                        seconds(env, "PULSE_VELOCITY_WINDOW_SECONDS", defaults.velocityWindow()),
                        seconds(env, "PULSE_ALERT_COOLDOWN_SECONDS", defaults.alertCooldown())));
    }

    private static Duration seconds(Map<String, String> env, String name, Duration fallback) {
        String value = env.get(name);
        return value == null || value.isBlank() ? fallback : Duration.ofSeconds(Long.parseLong(value.trim()));
    }
}
