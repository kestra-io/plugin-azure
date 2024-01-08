package io.kestra.plugin.azure.eventhubs.serdes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;

import java.io.IOException;

/**
 * A {@link Serde} for serializing/deserializing objects from and to Amazon Ion format.
 */
public class IonSerde implements Serde {

    private final static ObjectMapper OBJECT_MAPPER = JacksonMapper.ofIon()
        .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    /**
     * Creates a new {@link IonSerde} instance.
     */
    public IonSerde() {
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
            throw new RuntimeException("Error serializing data object into Ion.", e);
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public Object deserialize(byte[] data) {
        if (data == null) return null;
        try {
            return OBJECT_MAPPER.readTree(data);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing Ion into object.", e);
        }
    }
}
