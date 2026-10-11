package io.github.jad_salem.pulse.processor.serde;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;

/**
 * Decides whether a decoding failure is a property of the record (poison: route it to the
 * dead-letter topic) or of the environment (transient: fail the task so Flink restarts from the
 * last checkpoint and retries).
 *
 * <p>Getting this wrong in the "poison" direction is the dangerous case: a Schema Registry outage
 * would silently divert every valid event to the DLQ. So anything network-shaped is transient.
 */
public final class FailureClassifier {
    private FailureClassifier() {}

    public static boolean isTransient(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof ConnectException
                    || t instanceof SocketTimeoutException
                    || t instanceof UnknownHostException
                    || t instanceof HttpTimeoutException) {
                return true;
            }
            if (t instanceof RestClientException rest) {
                // 4xx (e.g. 40403 schema not found) is a property of the record; 5xx is an outage.
                return rest.getStatus() >= 500;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /** A short, single-line description suitable for a Kafka header. */
    public static String describe(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        if (root != failure) {
            message += " (caused by " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
        }
        message = message.replaceAll("\\s+", " ");
        return message.length() > 500 ? message.substring(0, 500) : message;
    }
}
