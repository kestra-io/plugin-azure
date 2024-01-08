package io.kestra.plugin.azure.eventhubs.model;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Map;

/**
 * A serializable entity class representing an Event Data
 * to be published or consumed from Azure Event Hubs.
 *
 * @param partitionKey  the event data partitionKey.
 * @param body          the event data body.
 * @param contentType   the event data content-type.
 * @param correlationId The event correlation ID.
 * @param messageId     the event message ID.
 * @param properties    the event properties.
 * @see com.azure.messaging.eventhubs.EventData
 */
public record EventDataObject(
    String partitionKey,
    Object body,
    String contentType,
    String correlationId,
    String messageId,
    Long enqueuedTimestamp,
    Long offset,
    Long sequenceNumber,
    Map<String, Object> properties) {

    public EventDataObject(@NotNull final String body) {
        this(null, body);
    }

    public EventDataObject(final String partitionKey, @NotNull final String body) {
        this(
            partitionKey,
            body,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.emptyMap()
        );
    }
}
