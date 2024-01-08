package io.kestra.plugin.azure.eventhubs.service;

import com.azure.messaging.eventhubs.EventData;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.serdes.Serde;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Converts {@link EventData} into {@link EventDataObject} and vice versa.
 */
public final class EventDataObjectConverter {

    private final Serde serde;

    public EventDataObjectConverter(final Serde serde) {
        this.serde = Objects.requireNonNull(serde, "serde cannot be null");
    }

    /**
     * Converts the given list of {@link EventData} into a new list of {@link EventDataObject}.
     *
     * @param data The list of {@link Collectors}
     * @return the new list of {@link EventDataObject}, or {@code null} if the given data is null.
     */
    public List<EventDataObject> convertFromEventData(final List<EventData> data) {
        return data.stream().map(this::convertFromEventData).collect(Collectors.toList());
    }

    /**
     * Converts the given {@link EventDataObject} into a new {@link EventData}.
     *
     * @param data The {@link EventDataObject}.
     * @return the new {@link EventData}, or {@code null} if the given data is null.
     */
    public EventData convertToEventData(final EventDataObject data) {
        if (data == null) return null;

        byte[] value = serde.serialize(data.body());

        final EventData event = new EventData(value);
        Optional.ofNullable(data.contentType()).ifPresent(event::setContentType);
        Optional.ofNullable(data.correlationId()).ifPresent(event::setCorrelationId);
        Optional.ofNullable(data.messageId()).ifPresent(event::setMessageId);
        Optional.ofNullable(data.properties()).ifPresent(props -> event.getProperties().putAll(data.properties()));
        return event;
    }

    /**
     * Converts the given {@link EventDataObject} into a new {@link EventData}.
     *
     * @param data The {@link EventDataObject}
     * @return the new {@link EventData}, or {@code null} if the given data is null.
     */
    public EventDataObject convertFromEventData(final EventData data) {
        if (data == null) return null;

        return new EventDataObject(
            data.getPartitionKey(),
            serde.deserialize(data.getBody()),
            data.getContentType(),
            data.getCorrelationId(),
            data.getMessageId(),
            Optional.ofNullable(data.getEnqueuedTime()).map(Instant::toEpochMilli).orElse(null),
            data.getOffset(),
            data.getSequenceNumber(),
            data.getProperties()
        );
    }
}
