package io.kestra.plugin.azure.eventhubs.serdes;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Factory class for constructing built-in {@link Serde}.
 */
public enum Serdes {

    STRING(StringSerde::new),
    BINARY(ByteArraySerde::new),
    ION(IonSerde::new),
    JSON(JsonSerde::new);

    private final Supplier<Serde> supplier;

    /**
     * Creates a new {@link Serdes} instance.
     *
     * @param supplier the serde supplier.
     */
    Serdes(Supplier<Serde> supplier) {
        this.supplier = supplier;
    }

    /**
     * Factory method for constructing a new {@link Serde} instance configured with the properties configs.
     *
     * @param properties configs in key/value pairs.
     * @return a new {@link Serde}
     */
    public Serde create(final Map<String, Object> properties) {
        Objects.requireNonNull(properties, "Cannot create 'Serde' with null properties.");
        Serde dataFormat = supplier.get();
        dataFormat.configure(properties);
        return dataFormat;
    }
}
