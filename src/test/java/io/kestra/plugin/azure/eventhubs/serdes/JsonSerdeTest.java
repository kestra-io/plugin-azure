package io.kestra.plugin.azure.eventhubs.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Collections;
import java.util.Map;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
class JsonSerdeTest {

    static ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

    @Test
    void shouldSerializeGivenAnyObject() {
        // Given
        Map<String, String> input = Map.of("foo", "bar");
        JsonSerde serde = (JsonSerde) Serdes.JSON.create(Collections.emptyMap());

        // When
        byte[] serialized = serde.serialize(input);

        // Then
        Assertions.assertNotNull(serialized);
        JsonNode node = serde.deserialize(serialized);
        Assertions.assertEquals(OBJECT_MAPPER.convertValue(node, Map.class), input);
    }
}