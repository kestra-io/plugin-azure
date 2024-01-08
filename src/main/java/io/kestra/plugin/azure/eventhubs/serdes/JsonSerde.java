package io.kestra.plugin.azure.eventhubs.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;

/**
 * A {@link Serde} for JSON.
 */
public class JsonSerde implements Serde {

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().build();

    /**
     * Creates a new {@link JsonSerde} instance.
     */
    public JsonSerde() {
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public byte[] serialize(Object data) {
        if (data == null) return null;
        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing JSON message", e);
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public JsonNode deserialize(byte[] data) {
        if (data == null) return null;
        try {
            return OBJECT_MAPPER.readTree(data);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing JSON message", e);
        }
    }
}
