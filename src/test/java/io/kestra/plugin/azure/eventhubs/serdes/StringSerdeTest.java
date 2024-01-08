package io.kestra.plugin.azure.eventhubs.serdes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class StringSerdeTest {

    @Test
    void shouldSerializeGivenString() {
        // Given
        String input = "test";
        StringSerde serde = (StringSerde) Serdes.STRING.create(Map.of(StringSerde.SERIALIZER_ENCODING_CONFIG_NAME, "utf-8"));

        // When
        byte[] serialized = serde.serialize(input);

        // Then
        Assertions.assertEquals(input, serde.deserialize(serialized));
    }
}