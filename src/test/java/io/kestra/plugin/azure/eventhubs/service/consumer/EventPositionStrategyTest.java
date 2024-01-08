package io.kestra.plugin.azure.eventhubs.service.consumer;

import com.azure.messaging.eventhubs.models.EventPosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

class EventPositionStrategyTest {

    @Test
    void shouldGetLatest() {
        Assertions.assertEquals(new EventPositionStrategy.Latest().get(), EventPosition.latest());
    }

    @Test
    void shouldGetEarliest() {
        Assertions.assertEquals(new EventPositionStrategy.Earliest().get(), EventPosition.earliest());
    }

    @Test
    void shouldGetEnqueueTime() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        String ISO = EventPositionStrategy.EnqueuedTime.DATE_TIME_FORMATTER.format(now);
        Assertions.assertEquals(
            new EventPositionStrategy.EnqueuedTime(ISO).get(),
            EventPosition.fromEnqueuedTime(now)
        );
    }
}