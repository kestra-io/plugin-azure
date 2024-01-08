package io.kestra.plugin.azure.eventhubs.serdes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link Serde} for serializing/deserializing String objects.
 */
public class StringSerde implements Serde {

    public static final String SERIALIZER_ENCODING_CONFIG_NAME = "serializer.encoding";
    private Charset encoding;

    public StringSerde() {
        this(StandardCharsets.UTF_8);
    }

    public StringSerde(final Charset encoding) {
        this.encoding = Objects.requireNonNull(encoding, "encoding cannot be null");
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void configure(Map<String, Object> configs) {
        encoding = Optional
            .ofNullable(configs.get(SERIALIZER_ENCODING_CONFIG_NAME))
            .map(Object::toString)
            .map(Charset::forName)
            .orElse(StandardCharsets.UTF_8);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public byte[] serialize(Object data) {
        if (data == null) return null;
        return data.toString().getBytes(encoding);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public String deserialize(byte[] data) {
        if (data == null) return null;
        return new String(data, encoding);
    }
}
