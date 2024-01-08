package io.kestra.plugin.azure.eventhubs.serdes;

import java.nio.ByteBuffer;

/**
 * A {@link Serde} for serializing/deserializing bytes array.
 */
public class ByteArraySerde implements Serde {

    /**
     * {@inheritDoc}
     **/
    @Override
    public byte[] serialize(Object data) {
        if (data == null) return null;

        if (data instanceof ByteBuffer buffer)
            return buffer.array();
        if (data instanceof byte[] array) {
            return array;
        }
        throw new RuntimeException(
            "Cannot serialize object of type '" + data + "' into bytes array."
        );
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public ByteBuffer deserialize(byte[] data) {
        if (data == null) return null;
        return ByteBuffer.wrap(data);
    }
}
