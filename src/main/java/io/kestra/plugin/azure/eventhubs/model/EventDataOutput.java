package io.kestra.plugin.azure.eventhubs.model;

import java.util.Map;

import io.kestra.core.models.tasks.Output;
import io.swagger.v3.oas.annotations.media.Schema;

import lombok.Builder;
import lombok.Getter;

/**
 * A serializable entity class representing an Event Data
 * to be consumed from Azure Event Hubs.
 *
 * @see EventDataObject
 */
@Getter
@Builder
public final class EventDataOutput implements Output {

    /**
     * the event data partitionKey.
     */
    @Schema(title = "Partition Key")
    private final String partitionKey;

    /**
     * the event data body.
     */
    @Schema(title = "Body")
    private final Object body;

    /**
     * the event data content-type.
     */
    @Schema(title = "Content Type")
    private final String contentType;

    /**
     * The event correlation ID.
     */
    @Schema(title = "Correlation Id")
    private final String correlationId;

    /**
     * the event message ID.
     */
    @Schema(title = "Message Id")
    private final String messageId;

    @Schema(title = "Enqueued Timestamp")
    private final Long enqueuedTimestamp;

    @Schema(title = "Offset")
    private final Long offset;

    @Schema(title = "Sequence Number")
    private final Long sequenceNumber;

    /**
     * the event properties.
     */
    @Schema(title = "Properties")
    private final Map<String, Object> properties;

    public static EventDataOutput of(final EventDataObject event) {
        return EventDataOutput.builder()
            .partitionKey(event.partitionKey())
            .body(event.body())
            .contentType(event.contentType())
            .correlationId(event.correlationId())
            .messageId(event.messageId())
            .enqueuedTimestamp(event.enqueuedTimestamp())
            .offset(event.offset())
            .sequenceNumber(event.sequenceNumber())
            .properties(event.properties())
            .build();
    }

}
