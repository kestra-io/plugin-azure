package io.kestra.plugin.azure.eventhubs.service.consumer;

import com.azure.messaging.eventhubs.models.EventPosition;

import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Strategies to initialize an Event Hub consumer if no offsets are stored.
 */
public interface EventPositionStrategy {

    EventPosition get();

    /**
     * Earliest.
     *
     * @see EventPosition#earliest()
     */
    record Earliest() implements EventPositionStrategy {

        /**
         * @return an {@link EventPosition} that corresponds to the location of
         * the first event present in the partition.
         */
        @Override
        public EventPosition get() {
            return EventPosition.earliest();
        }
    }

    /**
     * Latest.
     *
     * @see EventPosition#latest() ()
     */
    record Latest() implements EventPositionStrategy {

        /**
         * @return an {@link EventPosition} that corresponds to the end of the partition,
         * where no more events are currently enqueued.
         */
        @Override
        public EventPosition get() {
            return EventPosition.latest();
        }
    }

    /**
     * EnqueuedTime.
     *
     * @param dateTime the ISO Date Time
     */
    record EnqueuedTime(@NotNull String dateTime) implements EventPositionStrategy {

        static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC);

        /**
         * @return an {@link EventPosition} that corresponds to a specific Instant.
         */
        @Override
        public EventPosition get() {
            OffsetDateTime odt = OffsetDateTime.parse(dateTime, DATE_TIME_FORMATTER);
            Instant instant = Instant.from(odt);
            return EventPosition.fromEnqueuedTime(instant);
        }
    }

}
