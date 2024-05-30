package io.kestra.plugin.azure.eventhubs.model;

import io.kestra.core.models.tasks.Output;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

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
	private final String partitionKey;

	/**
	 * the event data body.
	 */
	private final Object body;

	/**
	 * the event data content-type.
	 */
	private final String contentType;

	/**
	 * The event correlation ID.
	 */
	private final String correlationId;

	/**
	 * the event message ID.
	 */
	private final String messageId;

	private final Long enqueuedTimestamp;

	private final Long offset;

	private final Long sequenceNumber;

	/**
	 * the event properties.
	 */
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
