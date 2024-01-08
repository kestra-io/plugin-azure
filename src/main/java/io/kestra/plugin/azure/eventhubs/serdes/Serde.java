package io.kestra.plugin.azure.eventhubs.serdes;

import java.util.Map;

/**
 * Service interface for serializing/deserializing data objects.
 */
public interface Serde {

    /**
     * Configures this class.
     *
     * @param configs configs in key/value pairs.
     */
    default void configure(Map<String, Object> configs) {
    }

    /**
     * Method that can be used to serialize a data object into a specific byte array data format.
     *
     * @param data The data to be serialized. Can be {@code null}.
     * @return the serialized object.
     */
    byte[] serialize(Object data);

    /**
     * Method that can be used to deserialize a specific byte array data format into object.
     *
     * @param data The data to be deserialized. Can be {@code null}.
     * @return the deserialized object.
     */
    Object deserialize(byte[] data);
}
