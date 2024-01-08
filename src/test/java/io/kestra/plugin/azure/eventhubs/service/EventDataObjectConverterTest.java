package io.kestra.plugin.azure.eventhubs.service;

import com.azure.messaging.eventhubs.EventData;
import io.kestra.plugin.azure.eventhubs.model.EventDataObject;
import io.kestra.plugin.azure.eventhubs.serdes.StringSerde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

class EventDataObjectConverterTest {

    @Test
    void shouldConvertToEventData() {
        EventDataObjectConverter converter = new EventDataObjectConverter(new StringSerde());
        long enqueuedTimestamp = Instant.now().toEpochMilli();
        Map<String, Object> prop = Map.of("prop", "value");
        EventData result = converter.convertToEventData(new EventDataObject(
            "key",
            "value",
            "contentType",
            "correlationId",
            "messageId",
            enqueuedTimestamp,
            1L,
            1L,
            prop
        ));
        Assertions.assertEquals("contentType", result.getContentType());
        Assertions.assertEquals("correlationId", result.getCorrelationId());
        Assertions.assertEquals("messageId", result.getMessageId());
        Assertions.assertEquals("value", result.getBodyAsString());
        Assertions.assertEquals(prop, result.getProperties());
    }
}